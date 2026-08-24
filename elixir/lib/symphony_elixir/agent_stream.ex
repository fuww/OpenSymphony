defmodule SymphonyElixir.AgentStream do
  @moduledoc false

  # Transport-neutral handle over an agent's bidirectional byte stream.
  #
  # In `:port` mode it wraps a real OTP `port()` (local and SSH transports), and the
  # owning process receives the usual `{port, {:data, {:eol | :noeol, chunk}}}` /
  # `{port, {:exit_status, status}}` messages directly.
  #
  # In `:broker` mode the underlying `kubectl run -i` connection is owned by a
  # `SymphonyElixir.K8s.Broker`, which forwards the agent's output to the subscriber as
  # `{:agent_stream, ref, {:data, {:eol | :noeol, chunk}}}` / `{:agent_stream, ref,
  # {:exit_status, status}}`. The sub-shape (`:eol`/`:noeol`) matches port mode so an
  # app-server's line-reassembly loop stays identical; it only needs one extra receive
  # clause matching `stream.ref`.
  #
  # App-servers hold an `%AgentStream{}` in their session struct and drive it through
  # `send_input/2`, `close/1`, `os_pid/1`, and `open?/1` instead of touching the port.

  require Logger

  alias SymphonyElixir.K8s.Broker

  @enforce_keys [:mode]
  defstruct [:mode, :port, :broker, :ref]

  @type t :: %__MODULE__{
          mode: :port | :broker,
          port: port() | nil,
          broker: pid() | nil,
          ref: reference() | nil
        }

  @shutdown_grace_ms 500
  @shutdown_kill_wait_ms 500
  @shutdown_poll_ms 25

  @spec from_port(port()) :: t()
  def from_port(port) when is_port(port), do: %__MODULE__{mode: :port, port: port}

  @spec from_broker(pid(), reference()) :: t()
  def from_broker(broker, ref) when is_pid(broker) and is_reference(ref) do
    %__MODULE__{mode: :broker, broker: broker, ref: ref}
  end

  @doc """
  Sends input to the agent. In broker mode the bytes are framed and relayed into the
  pod's agent input FIFO; the pod shell (and connection) stay alive.
  """
  @spec send_input(t(), iodata()) :: :ok | {:error, term()}
  def send_input(%__MODULE__{mode: :port, port: port}, data) when is_port(port) do
    case :erlang.port_info(port) do
      :undefined ->
        {:error, :port_closed}

      _ ->
        true = Port.command(port, data)
        :ok
    end
  rescue
    ArgumentError -> {:error, :port_closed}
  end

  def send_input(%__MODULE__{mode: :broker, broker: broker, ref: ref}, data) do
    Broker.send_input(broker, ref, data)
  end

  @doc """
  Ends the agent. In port mode the OS process group is signalled and the port closed. In
  broker mode the agent's input FIFO is closed (agent gets EOF and exits) WITHOUT closing
  the shared connection, so the pod shell survives for the trailing `after_run` hook.
  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{mode: :port, port: port}) when is_port(port), do: stop_port(port)
  def close(%__MODULE__{mode: :port}), do: :ok

  def close(%__MODULE__{mode: :broker, broker: broker, ref: ref}) do
    Broker.end_agent(broker, ref)
  end

  @doc """
  Returns the agent's OS pid (local pid in port mode, pod-side pid in broker mode) or
  `nil` when unavailable. Informational only — used for metadata.
  """
  @spec os_pid(t()) :: non_neg_integer() | nil
  def os_pid(%__MODULE__{mode: :port, port: port}) when is_port(port) do
    case :erlang.port_info(port, :os_pid) do
      {:os_pid, os_pid} -> os_pid
      _ -> nil
    end
  end

  def os_pid(%__MODULE__{mode: :port}), do: nil

  def os_pid(%__MODULE__{mode: :broker, broker: broker, ref: ref}) do
    Broker.os_pid(broker, ref)
  end

  @doc "Whether the stream is still usable."
  @spec open?(t()) :: boolean()
  def open?(%__MODULE__{mode: :port, port: port}) when is_port(port) do
    :erlang.port_info(port) != :undefined
  end

  def open?(%__MODULE__{mode: :port}), do: false
  def open?(%__MODULE__{mode: :broker, broker: broker}), do: Process.alive?(broker)

  # --- Port-mode shutdown (moved out of the app-servers) ---------------------------

  defp stop_port(port) when is_port(port) do
    case :erlang.port_info(port) do
      :undefined ->
        :ok

      _ ->
        terminate_port_os_process(port)

        try do
          Port.close(port)
          :ok
        rescue
          ArgumentError -> :ok
        end
    end
  end

  defp terminate_port_os_process(port) when is_port(port) do
    case :erlang.port_info(port, :os_pid) do
      {:os_pid, os_pid} when is_integer(os_pid) and os_pid > 0 ->
        terminate_os_process_group(os_pid)

      _ ->
        :ok
    end
  end

  defp terminate_os_process_group(os_pid) do
    send_process_signal(os_pid, "TERM")

    unless wait_for_process_exit(os_pid, @shutdown_grace_ms) do
      send_process_signal(os_pid, "KILL")
      wait_for_process_exit(os_pid, @shutdown_kill_wait_ms)
    end

    :ok
  end

  defp send_process_signal(os_pid, signal) do
    group_target = "-#{os_pid}"
    pid_target = Integer.to_string(os_pid)

    case System.cmd("kill", ["-#{signal}", "--", group_target], stderr_to_stdout: true) do
      {_output, 0} ->
        :ok

      _ ->
        case System.cmd("kill", ["-#{signal}", "--", pid_target], stderr_to_stdout: true) do
          {_output, 0} -> :ok
          _ -> :ok
        end
    end
  rescue
    _ -> :ok
  end

  defp wait_for_process_exit(os_pid, remaining_ms) when remaining_ms <= 0 do
    not os_process_alive?(os_pid)
  end

  defp wait_for_process_exit(os_pid, remaining_ms) do
    if os_process_alive?(os_pid) do
      Process.sleep(@shutdown_poll_ms)
      wait_for_process_exit(os_pid, remaining_ms - @shutdown_poll_ms)
    else
      true
    end
  end

  defp os_process_alive?(os_pid) do
    case System.cmd("kill", ["-0", "--", Integer.to_string(os_pid)], stderr_to_stdout: true) do
      {_output, 0} -> true
      _ -> false
    end
  rescue
    _ -> false
  end
end
