defmodule SymphonyElixir.K8s.Pod do
  @moduledoc false

  # Ephemeral, per-run Kubernetes pods. A pod is created when an agent run starts
  # and deleted the moment it finishes, so a pod lives only while its agent uses
  # it. All operations go through the `kubectl` CLI (see `SymphonyElixir.K8s`),
  # reusing the operator's kubeconfig / in-cluster credentials.

  require Logger

  alias SymphonyElixir.Config.Schema
  alias SymphonyElixir.K8s

  @runner_label "symphony-runner"
  @max_name_length 63

  @spec runner_label() :: String.t()
  def runner_label, do: @runner_label

  @doc """
  Generates a fresh RFC-1123-compliant pod name unique to a single run.
  """
  @spec generate_name(term()) :: String.t()
  def generate_name(issue) do
    prefix = sanitize_segment(pod_name_prefix())
    issue_segment = issue |> issue_identifier() |> sanitize_segment()
    suffix = random_suffix()

    [prefix, issue_segment, suffix]
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("-")
    |> truncate_name()
    |> trim_trailing_dash()
  end

  @doc """
  Creates the pod from the configured template and blocks until it is Ready.

  On any failure the pod is deleted (best effort) and `{:error, {:pod_start_failed, ...}}`
  is returned so the caller never leaks a half-started pod.
  """
  @spec create(String.t(), Schema.t() | map(), term()) :: :ok | {:error, term()}
  def create(pod_name, settings, issue \\ nil) when is_binary(pod_name) do
    k8s = kubernetes(settings)
    issue_id = issue && issue_identifier(issue)

    with :ok <- ensure_configured(k8s),
         :ok <- apply_manifest(build_manifest(pod_name, k8s, issue_id), k8s),
         :ok <- wait_ready(pod_name, k8s) do
      Logger.info("Created symphony runner pod pod=#{pod_name} namespace=#{k8s.namespace}")
      :ok
    else
      {:error, reason} ->
        _ = delete(pod_name, settings)
        {:error, {:pod_start_failed, pod_name, reason}}
    end
  end

  @doc """
  Deletes the pod. Never raises — a failed delete is logged so leaks are findable.
  """
  @spec delete(String.t(), Schema.t() | map()) :: :ok
  def delete(pod_name, settings) when is_binary(pod_name) do
    k8s = kubernetes(settings)

    args =
      base_args(k8s) ++ ["delete", "pod", pod_name, "--wait=false", "--ignore-not-found"]

    case run_kubectl(args) do
      {_output, 0} ->
        :ok

      {output, status} ->
        Logger.warning("Failed to delete symphony runner pod pod=#{pod_name} namespace=#{k8s.namespace} status=#{status} output=#{inspect(String.slice(output, 0, 512))}")

        :ok
    end
  rescue
    error ->
      Logger.warning("Error deleting symphony runner pod pod=#{pod_name} error=#{Exception.message(error)}")
      :ok
  end

  @doc """
  Deletes all runner pods. Intended to run on orchestrator boot, when no runs are
  active, to reclaim pods leaked by a prior crash that skipped normal teardown.
  """
  @spec reap_orphans(Schema.t() | map()) :: :ok
  def reap_orphans(settings) do
    k8s = kubernetes(settings)

    case ensure_configured(k8s) do
      :ok ->
        args =
          base_args(k8s) ++
            ["delete", "pods", "-l", "app=#{@runner_label}", "--ignore-not-found", "--wait=false"]

        case run_kubectl(args) do
          {_output, 0} ->
            :ok

          {output, status} ->
            Logger.warning("Failed to reap orphan runner pods namespace=#{k8s.namespace} status=#{status} output=#{inspect(String.slice(output, 0, 512))}")

            :ok
        end

      {:error, _reason} ->
        :ok
    end
  rescue
    error ->
      Logger.warning("Error reaping orphan runner pods error=#{Exception.message(error)}")
      :ok
  end

  @doc false
  @spec build_manifest(String.t(), map(), term()) :: map()
  def build_manifest(pod_name, k8s, issue_id \\ nil) do
    template = normalize_template(k8s.pod_template)

    template
    |> Map.put_new("apiVersion", "v1")
    |> Map.put_new("kind", "Pod")
    |> put_metadata(pod_name, k8s, issue_id)
    |> put_active_deadline(k8s)
  end

  defp put_metadata(manifest, pod_name, k8s, issue_id) do
    metadata = ensure_map(Map.get(manifest, "metadata"))

    labels =
      metadata
      |> Map.get("labels")
      |> ensure_map()
      |> Map.put("app", @runner_label)
      |> maybe_put_issue_label(issue_id)

    metadata =
      metadata
      |> Map.put("name", pod_name)
      |> Map.put("namespace", k8s.namespace)
      |> Map.put("labels", labels)

    Map.put(manifest, "metadata", metadata)
  end

  defp maybe_put_issue_label(labels, issue_id) when is_binary(issue_id) and issue_id != "" do
    Map.put(labels, "symphony/issue", sanitize_segment(issue_id))
  end

  defp maybe_put_issue_label(labels, _issue_id), do: labels

  defp put_active_deadline(manifest, %{active_deadline_seconds: seconds})
       when is_integer(seconds) and seconds > 0 do
    spec = ensure_map(Map.get(manifest, "spec"))
    Map.put(manifest, "spec", Map.put_new(spec, "activeDeadlineSeconds", seconds))
  end

  defp put_active_deadline(manifest, _k8s), do: manifest

  defp apply_manifest(manifest, k8s) do
    json = Jason.encode!(manifest)
    tmp_path = Path.join(System.tmp_dir!(), "symphony-pod-#{random_suffix()}.json")
    File.write!(tmp_path, json)

    try do
      case run_kubectl(base_args(k8s) ++ ["apply", "-f", tmp_path]) do
        {_output, 0} -> :ok
        {output, status} -> {:error, {:apply_failed, status, output}}
      end
    after
      File.rm(tmp_path)
    end
  end

  defp wait_ready(pod_name, k8s) do
    timeout_seconds = max(1, div(k8s.ready_timeout_ms || 120_000, 1000))

    args =
      base_args(k8s) ++
        ["wait", "--for=condition=Ready", "pod/#{pod_name}", "--timeout=#{timeout_seconds}s"]

    case run_kubectl(args) do
      {_output, 0} -> :ok
      {output, status} -> {:error, {:wait_failed, status, output}}
    end
  end

  defp base_args(k8s) do
    K8s.context_args(k8s) ++ ["-n", k8s.namespace]
  end

  defp run_kubectl(args) do
    case K8s.kubectl_executable() do
      {:ok, executable} -> System.cmd(executable, args, stderr_to_stdout: true)
      {:error, reason} -> {"kubectl unavailable: #{inspect(reason)}", 127}
    end
  end

  defp ensure_configured(%{namespace: namespace, pod_template: template})
       when is_binary(namespace) and namespace != "" and is_map(template) and map_size(template) > 0,
       do: :ok

  defp ensure_configured(_k8s), do: {:error, :kubernetes_not_configured}

  defp kubernetes(%Schema{worker: %{kubernetes: %{} = k8s}}), do: k8s
  defp kubernetes(%{namespace: _} = k8s), do: k8s
  defp kubernetes(_settings), do: %{namespace: nil, pod_template: nil}

  defp pod_name_prefix do
    case SymphonyElixir.Config.kubernetes_settings() do
      %{pod_name_prefix: prefix} when is_binary(prefix) and prefix != "" -> prefix
      _ -> "symphony"
    end
  rescue
    _ -> "symphony"
  end

  defp issue_identifier(%{identifier: identifier}) when is_binary(identifier), do: identifier
  defp issue_identifier(%{"identifier" => identifier}) when is_binary(identifier), do: identifier
  defp issue_identifier(identifier) when is_binary(identifier), do: identifier
  defp issue_identifier(_issue), do: "issue"

  defp normalize_template(template) when is_map(template), do: template
  defp normalize_template(_template), do: %{}

  defp ensure_map(value) when is_map(value), do: value
  defp ensure_map(_value), do: %{}

  # RFC 1123: lowercase alphanumerics and '-', starting/ending alphanumeric.
  defp sanitize_segment(value) when is_binary(value) do
    value
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9-]+/, "-")
    |> String.replace(~r/-+/, "-")
    |> String.trim("-")
  end

  defp sanitize_segment(_value), do: ""

  defp random_suffix do
    :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
  end

  defp truncate_name(name) when byte_size(name) <= @max_name_length, do: name
  defp truncate_name(name), do: binary_part(name, 0, @max_name_length)

  defp trim_trailing_dash(name), do: String.trim_trailing(name, "-")
end
