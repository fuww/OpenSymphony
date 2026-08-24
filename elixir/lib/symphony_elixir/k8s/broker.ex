defmodule SymphonyElixir.K8s.Broker do
  @moduledoc false

  # Owns the single `kubectl run -i --rm` connection to one runner pod for the whole run
  # and multiplexes everything over it, so there is exactly one `kubectl run` and no
  # `kubectl exec` — every command's output flows to the pod's stdout and is visible via
  # `kubectl logs`. The pod's PID 1 is the protocol reader from `K8s.Protocol`.
  #
  # Two request kinds share the connection, serialized by run ordering (the orchestrator
  # never runs a sync command while the agent is streaming — see `AgentRunner`):
  #
  #   * sync command (`run/3`): write a RUN frame, collect the pod's stdout lines until
  #     the `DONE` marker, return `{:ok, {output, exit_status}}` (drop-in for the old
  #     `System.cmd`-style return).
  #   * agent stream (`start_agent/3` + `send_input/3` + `end_agent/2`): launch the agent
  #     reading an input FIFO, relay the app-server's prompt bytes into it, and forward the
  #     agent's stdout lines to the subscriber as `{:agent_stream, ref, ...}`. Ending the
  #     agent closes the FIFO (agent EOF) WITHOUT closing the connection, so the shell
  #     survives for the trailing `after_run` hook.

  use GenServer, restart: :temporary

  require Logger

  alias SymphonyElixir.K8s.Protocol

  @registry SymphonyElixir.K8s.BrokerRegistry
  @line_bytes 1_048_576
  @call_timeout :infinity

  # --- Client ----------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: via(name))
  end

  @spec via(String.t()) :: {:via, module(), {module(), String.t()}}
  def via(name), do: {:via, Registry, {@registry, name}}

  @spec whereis(String.t()) :: pid() | nil
  def whereis(name) do
    case Registry.lookup(@registry, name) do
      [{pid, _value}] -> pid
      [] -> nil
    end
  end

  @spec run(pid(), String.t(), keyword()) ::
          {:ok, {String.t(), non_neg_integer()}} | {:error, term()}
  def run(broker, command, _opts \\ []) when is_pid(broker) and is_binary(command) do
    GenServer.call(broker, {:run, command}, @call_timeout)
  end

  @spec start_agent(pid(), String.t(), keyword()) :: {:ok, reference()} | {:error, term()}
  def start_agent(broker, command, _opts \\ []) when is_pid(broker) and is_binary(command) do
    GenServer.call(broker, {:start_agent, command, self()}, @call_timeout)
  end

  @spec send_input(pid(), reference(), iodata()) :: :ok | {:error, term()}
  def send_input(broker, ref, data) when is_pid(broker) and is_reference(ref) do
    GenServer.call(broker, {:send_input, ref, data}, @call_timeout)
  end

  @spec end_agent(pid(), reference()) :: :ok
  def end_agent(broker, ref) when is_pid(broker) and is_reference(ref) do
    GenServer.call(broker, {:end_agent, ref}, @call_timeout)
  end

  @spec os_pid(pid(), reference()) :: non_neg_integer() | nil
  def os_pid(broker, ref) when is_pid(broker) and is_reference(ref) do
    GenServer.call(broker, {:os_pid, ref}, @call_timeout)
  catch
    :exit, _reason -> nil
  end

  # --- Server ----------------------------------------------------------------------

  @impl true
  def init(opts) do
    executable = Keyword.fetch!(opts, :executable)
    args = Keyword.fetch!(opts, :args)
    name = Keyword.fetch!(opts, :name)
    line_bytes = Keyword.get(opts, :line, @line_bytes)

    port =
      Port.open(
        {:spawn_executable, String.to_charlist(executable)},
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          {:line, line_bytes},
          args: Enum.map(args, &String.to_charlist/1)
        ]
      )

    {:ok,
     %{
       port: port,
       name: name,
       buf: "",
       sync: nil,
       sync_queue: :queue.new(),
       agent: nil
     }}
  rescue
    error -> {:stop, {:port_open_failed, Exception.message(error)}}
  end

  @impl true
  def handle_call({:run, command}, from, %{sync: nil} = state) do
    Port.command(state.port, Protocol.frame(:run, command))
    {:noreply, %{state | sync: {from, []}}}
  end

  def handle_call({:run, command}, from, state) do
    {:noreply, %{state | sync_queue: :queue.in({from, command}, state.sync_queue)}}
  end

  def handle_call({:start_agent, command, subscriber}, _from, state) do
    ref = make_ref()
    Port.command(state.port, Protocol.frame(:agent_start, command))
    {:reply, {:ok, ref}, %{state | agent: %{ref: ref, subscriber: subscriber, os_pid: nil}}}
  end

  def handle_call({:send_input, ref, data}, _from, %{agent: %{ref: ref}} = state) do
    Port.command(state.port, Protocol.frame(:agent_in, data))
    {:reply, :ok, state}
  end

  def handle_call({:send_input, _ref, _data}, _from, state) do
    {:reply, {:error, :no_active_agent}, state}
  end

  def handle_call({:end_agent, ref}, _from, %{agent: %{ref: ref}} = state) do
    # Reply immediately; the agent is cleared when its AGENT_EXIT line arrives. The
    # connection stays open so a following `run/3` (the after_run hook) still works.
    Port.command(state.port, Protocol.frame(:agent_eof))
    {:reply, :ok, state}
  end

  def handle_call({:end_agent, _ref}, _from, state), do: {:reply, :ok, state}

  def handle_call({:os_pid, ref}, _from, %{agent: %{ref: ref, os_pid: os_pid}} = state) do
    {:reply, os_pid, state}
  end

  def handle_call({:os_pid, _ref}, _from, state), do: {:reply, nil, state}

  @impl true
  def handle_info({port, {:data, {:eol, chunk}}}, %{port: port, buf: buf} = state) do
    {:noreply, process_line(buf <> chunk, %{state | buf: ""})}
  end

  def handle_info({port, {:data, {:noeol, chunk}}}, %{port: port, buf: buf} = state) do
    {:noreply, %{state | buf: buf <> chunk}}
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    {:stop, :normal, fail_pending(state, {:connection_closed, status})}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{port: port}) do
    # Closing the port sends stdin EOF to `kubectl run -i`; the reader exits and `--rm`
    # deletes the pod. This is how `K8s.Pod.close/1` (which stops this broker) tears down.
    if is_port(port) and :erlang.port_info(port) != :undefined do
      try do
        Port.close(port)
      rescue
        ArgumentError -> :ok
      end
    end

    :ok
  end

  def terminate(_reason, _state), do: :ok

  # --- Line dispatch ---------------------------------------------------------------

  defp process_line(line, state) do
    case Protocol.parse_reply(line) do
      {:done, code} -> complete_sync(code, state)
      {:agent_pid, pid} -> put_agent_os_pid(pid, state)
      {:agent_exit, code} -> finish_agent(code, state)
      :not_control -> route_output(line, state)
    end
  end

  defp complete_sync(code, %{sync: {from, lines}} = state) do
    GenServer.reply(from, {:ok, {join_output(lines), code}})
    drain_queue(%{state | sync: nil})
  end

  defp complete_sync(_code, state), do: state

  defp put_agent_os_pid(pid, %{agent: %{} = agent} = state) do
    %{state | agent: %{agent | os_pid: pid}}
  end

  defp put_agent_os_pid(_pid, state), do: state

  defp finish_agent(code, %{agent: %{ref: ref, subscriber: subscriber}} = state) do
    send(subscriber, {:agent_stream, ref, {:exit_status, code}})
    %{state | agent: nil}
  end

  defp finish_agent(_code, state), do: state

  # Agent output takes priority when an agent is active; otherwise it is command output
  # for the in-flight sync. These are mutually exclusive per the run-ordering invariant.
  defp route_output(line, %{agent: %{ref: ref, subscriber: subscriber}} = state) do
    send(subscriber, {:agent_stream, ref, {:data, {:eol, line}}})
    state
  end

  defp route_output(line, %{sync: {from, lines}} = state) do
    %{state | sync: {from, [line | lines]}}
  end

  defp route_output(line, state) do
    trimmed = String.trim(line)

    cond do
      trimmed == "" -> :ok
      kubectl_attach_banner?(trimmed) -> :ok
      true -> Logger.debug("Runner pod #{state.name} stray output: #{trimmed}")
    end

    state
  end

  # `kubectl run -i` prints this banner to stderr when it attaches interactively. There
  # is no flag to suppress it, so drop it instead of logging it as stray pod output.
  defp kubectl_attach_banner?("If you don't see a command prompt, try pressing enter."), do: true
  defp kubectl_attach_banner?(_line), do: false

  defp drain_queue(%{sync: nil, sync_queue: queue} = state) do
    case :queue.out(queue) do
      {{:value, {from, command}}, rest} ->
        Port.command(state.port, Protocol.frame(:run, command))
        %{state | sync: {from, []}, sync_queue: rest}

      {:empty, _rest} ->
        state
    end
  end

  defp drain_queue(state), do: state

  defp fail_pending(state, reason) do
    if state.sync, do: GenServer.reply(elem(state.sync, 0), {:error, reason})

    state.sync_queue
    |> :queue.to_list()
    |> Enum.each(fn {from, _command} -> GenServer.reply(from, {:error, reason}) end)

    case state.agent do
      %{ref: ref, subscriber: subscriber} ->
        send(subscriber, {:agent_stream, ref, {:exit_status, exit_code(reason)}})

      _ ->
        :ok
    end

    %{state | sync: nil, sync_queue: :queue.new(), agent: nil}
  end

  defp exit_code({:connection_closed, status}) when is_integer(status), do: status
  defp exit_code(_reason), do: 1

  # `:eol` chunks arrive without their trailing newline; rejoin to mimic command output
  # (callers like `Workspace` split on "\n").
  defp join_output([]), do: ""

  defp join_output(lines) do
    lines
    |> Enum.reverse()
    |> Enum.map_join("", &(&1 <> "\n"))
  end
end
