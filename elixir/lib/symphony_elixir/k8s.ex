defmodule SymphonyElixir.K8s do
  @moduledoc false

  # Transport peer of `SymphonyElixir.SSH`. Instead of reaching a remote host over `ssh`,
  # every command for a Kubernetes run travels over the single `kubectl run -i --rm`
  # connection owned by that run's `SymphonyElixir.K8s.Broker` (resolved by pod name via
  # the broker registry). There is no `kubectl exec`: `run/3` sends a framed command and
  # collects its output/exit-status from the broker, and `open_agent_stream/3` starts the
  # agent over the same connection. All output flows to the pod's stdout, so `kubectl logs`
  # shows the whole run.

  alias SymphonyElixir.AgentStream
  alias SymphonyElixir.Config
  alias SymphonyElixir.K8s.Broker

  @spec run(String.t(), String.t(), keyword()) ::
          {:ok, {String.t(), non_neg_integer()}} | {:error, term()}
  def run(pod, command, opts \\ []) when is_binary(pod) and is_binary(command) do
    with {:ok, _k8s} <- kubernetes_settings(),
         {:ok, broker} <- broker_for(pod) do
      Broker.run(broker, command, opts)
    end
  end

  @spec open_agent_stream(String.t(), String.t(), keyword()) ::
          {:ok, AgentStream.t()} | {:error, term()}
  def open_agent_stream(pod, command, opts \\ []) when is_binary(pod) and is_binary(command) do
    with {:ok, _k8s} <- kubernetes_settings(),
         {:ok, broker} <- broker_for(pod),
         {:ok, ref} <- Broker.start_agent(broker, command, opts) do
      {:ok, AgentStream.from_broker(broker, ref)}
    end
  end

  defp broker_for(pod) do
    case Broker.whereis(pod) do
      pid when is_pid(pid) -> {:ok, pid}
      nil -> {:error, {:no_broker_for_pod, pod}}
    end
  end

  @doc false
  @spec kubectl_executable() :: {:ok, String.t()} | {:error, :kubectl_not_found}
  def kubectl_executable do
    case System.find_executable("kubectl") do
      nil -> {:error, :kubectl_not_found}
      executable -> {:ok, executable}
    end
  end

  @doc false
  @spec context_args(map()) :: [String.t()]
  def context_args(%{kubectl_context: context}) when is_binary(context) and context != "",
    do: ["--context", context]

  def context_args(_k8s), do: []

  defp kubernetes_settings do
    case Config.kubernetes_settings() do
      %{namespace: namespace} = k8s when is_binary(namespace) and namespace != "" ->
        {:ok, k8s}

      _ ->
        {:error, :kubernetes_not_configured}
    end
  end
end
