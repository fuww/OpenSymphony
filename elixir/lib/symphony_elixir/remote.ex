defmodule SymphonyElixir.Remote do
  @moduledoc false

  # Single indirection over the remote-execution transports. `worker_host` is a
  # plain string in both modes (an SSH host, or an ephemeral pod name), so every
  # caller stays transport-agnostic — only the transport used to reach the host
  # changes, governed by the globally-configured `worker.mode`.

  alias SymphonyElixir.{AgentStream, Config, K8s, SSH}

  @spec run(String.t(), String.t(), keyword()) ::
          {:ok, {String.t(), non_neg_integer()}} | {:error, term()}
  def run(worker_host, command, opts \\ []) when is_binary(worker_host) and is_binary(command) do
    transport().run(worker_host, command, opts)
  end

  @spec start_port(String.t(), String.t(), keyword()) :: {:ok, port()} | {:error, term()}
  def start_port(worker_host, command, opts \\ [])
      when is_binary(worker_host) and is_binary(command) do
    SSH.start_port(worker_host, command, opts)
  end

  @doc """
  Opens the agent's bidirectional stream on the worker, returning an `AgentStream` handle.
  SSH mode wraps a real port; Kubernetes mode goes over the broker-owned connection.
  """
  @spec open_agent_stream(String.t(), String.t(), keyword()) ::
          {:ok, AgentStream.t()} | {:error, term()}
  def open_agent_stream(worker_host, command, opts \\ [])
      when is_binary(worker_host) and is_binary(command) do
    case Config.worker_mode() do
      :kubernetes ->
        K8s.open_agent_stream(worker_host, command, opts)

      _ ->
        with {:ok, port} <- SSH.start_port(worker_host, command, opts) do
          {:ok, AgentStream.from_port(port)}
        end
    end
  end

  defp transport do
    case Config.worker_mode() do
      :kubernetes -> K8s
      _ -> SSH
    end
  end
end
