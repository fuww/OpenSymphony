defmodule SymphonyElixir.K8s.PortForward do
  @moduledoc false

  # A deliberate exception to `K8s.Broker`'s single-connection rule.
  #
  # `opencode` is the one app-server that speaks HTTP + SSE rather than stdio, so its
  # traffic cannot be multiplexed over the broker's line-framed stdout protocol. To reach
  # the server it starts on `127.0.0.1:<port>` *inside* the runner pod, we open a second,
  # short-lived `kubectl port-forward` connection alongside the broker's `kubectl run -i`
  # one and bind it tightly to the opencode session: it is started when the session starts
  # and torn down (`stop/1`) when the session stops, so it can never outlive the pod.
  #
  # If the pod disappears (e.g. a spot-node preemption kills both the broker connection and
  # this one), `kubectl port-forward` exits and the owning session receives the port's
  # `{:exit_status, _}` — surfacing as a failed run instead of a hung session.

  require Logger

  alias SymphonyElixir.{Config, K8s}

  @forwarding_line_regex ~r/Forwarding from 127\.0\.0\.1:(?<port>\d+) ->/
  @line_bytes 1_048_576
  @default_timeout_ms 30_000
  @log_preview_bytes 1_000

  @doc """
  Opens a `kubectl port-forward` to `pod`'s in-pod `remote_port` and returns the still-open
  port together with the local port kubectl bound. The caller owns the returned port: its
  output and `{:exit_status, _}` messages flow to the caller, and it must be handed to
  `stop/1` at teardown.

  `context` is the app-server's startup context; its `:read_timeout_ms` bounds how long we
  wait for kubectl to announce its local port.
  """
  @spec start(String.t(), non_neg_integer(), map()) ::
          {:ok, port(), non_neg_integer()} | {:error, term()}
  def start(pod, remote_port, context \\ %{})
      when is_binary(pod) and is_integer(remote_port) and remote_port > 0 do
    with {:ok, executable} <- K8s.kubectl_executable(),
         {:ok, k8s} <- kubernetes_settings() do
      args =
        K8s.context_args(k8s) ++
          ["-n", k8s.namespace, "port-forward", "pod/#{pod}", ":#{remote_port}"]

      port =
        Port.open(
          {:spawn_executable, String.to_charlist(executable)},
          [
            :binary,
            :exit_status,
            :stderr_to_stdout,
            {:line, @line_bytes},
            args: Enum.map(args, &String.to_charlist/1)
          ]
        )

      deadline_ms = System.monotonic_time(:millisecond) + timeout_ms(context)
      await_forwarding(port, "", deadline_ms)
    end
  end

  @doc """
  Tears the port-forward down. Closing the port terminates the `kubectl port-forward`
  child. Never raises and accepts `nil` (local mode, where no port-forward exists).
  """
  @spec stop(port() | nil) :: :ok
  def stop(nil), do: :ok

  def stop(port) when is_port(port) do
    if :erlang.port_info(port) != :undefined do
      try do
        Port.close(port)
      rescue
        ArgumentError -> :ok
      end
    end

    :ok
  end

  def stop(_other), do: :ok

  defp await_forwarding(port, pending_line, deadline_ms) do
    timeout = max(0, deadline_ms - System.monotonic_time(:millisecond))

    receive do
      {^port, {:data, {:eol, chunk}}} ->
        line = pending_line <> IO.chardata_to_string(chunk)

        case Regex.named_captures(@forwarding_line_regex, line) do
          %{"port" => local_port} ->
            {:ok, port, String.to_integer(local_port)}

          _ ->
            log_output(line)
            await_forwarding(port, "", deadline_ms)
        end

      {^port, {:data, {:noeol, chunk}}} ->
        await_forwarding(port, pending_line <> IO.chardata_to_string(chunk), deadline_ms)

      {^port, {:exit_status, status}} ->
        {:error, {:port_forward_exit, status}}
    after
      timeout ->
        stop(port)
        {:error, :port_forward_timeout}
    end
  end

  defp timeout_ms(context) do
    case Map.get(context, :read_timeout_ms) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_timeout_ms
    end
  end

  defp kubernetes_settings do
    case Config.kubernetes_settings() do
      %{namespace: namespace} = k8s when is_binary(namespace) and namespace != "" ->
        {:ok, k8s}

      _ ->
        {:error, :kubernetes_not_configured}
    end
  end

  defp log_output(line) when is_binary(line) do
    text = line |> String.trim_trailing() |> truncate()

    if text != "" do
      Logger.debug("kubectl port-forward output: #{text}")
    end
  end

  defp truncate(text) when byte_size(text) > @log_preview_bytes do
    binary_part(text, 0, @log_preview_bytes) <> "..."
  end

  defp truncate(text), do: text
end
