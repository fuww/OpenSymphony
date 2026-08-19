defmodule SymphonyElixir.Remote do
  @moduledoc false

  # Single indirection over the remote-execution transports. `worker_host` is a
  # plain string in both modes (an SSH host, or an ephemeral pod name), so every
  # caller stays transport-agnostic — only the transport used to reach the host
  # changes, governed by the globally-configured `worker.mode`.

  alias SymphonyElixir.{Config, K8s, SSH}

  @spec run(String.t(), String.t(), keyword()) ::
          {:ok, {String.t(), non_neg_integer()}} | {:error, term()}
  def run(worker_host, command, opts \\ []) when is_binary(worker_host) and is_binary(command) do
    transport().run(worker_host, command, opts)
  end

  @spec start_port(String.t(), String.t(), keyword()) :: {:ok, port()} | {:error, term()}
  def start_port(worker_host, command, opts \\ [])
      when is_binary(worker_host) and is_binary(command) do
    transport().start_port(worker_host, command, opts)
  end

  defp transport do
    case Config.worker_mode() do
      :kubernetes -> K8s
      _ -> SSH
    end
  end
end
