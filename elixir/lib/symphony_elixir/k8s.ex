defmodule SymphonyElixir.K8s do
  @moduledoc false

  # Transport peer of `SymphonyElixir.SSH`. Instead of executing a command on a
  # remote host over `ssh`, it executes the command inside a Kubernetes pod via
  # `kubectl exec`. Because `kubectl exec` shells out just like `ssh`, both
  # `run/3` and `start_port/3` are drop-in equivalents of the SSH functions and
  # `start_port/3` returns a real OTP `port()` for streaming.

  alias SymphonyElixir.Config

  @spec run(String.t(), String.t(), keyword()) ::
          {:ok, {String.t(), non_neg_integer()}} | {:error, term()}
  def run(pod, command, opts \\ []) when is_binary(pod) and is_binary(command) do
    with {:ok, executable} <- kubectl_executable(),
         {:ok, k8s} <- kubernetes_settings() do
      {:ok, System.cmd(executable, exec_args(k8s, pod, command), opts)}
    end
  end

  @spec start_port(String.t(), String.t(), keyword()) :: {:ok, port()} | {:error, term()}
  def start_port(pod, command, opts \\ []) when is_binary(pod) and is_binary(command) do
    with {:ok, executable} <- kubectl_executable(),
         {:ok, k8s} <- kubernetes_settings() do
      line_bytes = Keyword.get(opts, :line)

      port_opts =
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args: Enum.map(exec_args(k8s, pod, command), &String.to_charlist/1)
        ]
        |> maybe_put_line_option(line_bytes)

      {:ok, Port.open({:spawn_executable, String.to_charlist(executable)}, port_opts)}
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

  # kubectl exec passes argv directly to the container (no intermediate shell), so
  # the command travels to `bash -lc` intact without any shell escaping — unlike
  # SSH, which concatenates its args into one remote shell command line.
  defp exec_args(k8s, pod, command) do
    ["exec", "-i"]
    |> maybe_put_context(k8s)
    |> Kernel.++(["-n", k8s.namespace])
    |> maybe_put_container(k8s)
    |> Kernel.++([pod, "--", "bash", "-lc", command])
  end

  @doc false
  @spec context_args(map()) :: [String.t()]
  def context_args(%{kubectl_context: context}) when is_binary(context) and context != "",
    do: ["--context", context]

  def context_args(_k8s), do: []

  defp maybe_put_context(args, k8s), do: args ++ context_args(k8s)

  defp maybe_put_container(args, %{container: container})
       when is_binary(container) and container != "",
       do: args ++ ["-c", container]

  defp maybe_put_container(args, _k8s), do: args

  defp maybe_put_line_option(port_opts, nil), do: port_opts
  defp maybe_put_line_option(port_opts, line_bytes), do: Keyword.put(port_opts, :line, line_bytes)

  defp kubernetes_settings do
    case Config.kubernetes_settings() do
      %{namespace: namespace} = k8s when is_binary(namespace) and namespace != "" ->
        {:ok, k8s}

      _ ->
        {:error, :kubernetes_not_configured}
    end
  end
end
