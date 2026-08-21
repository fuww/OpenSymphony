defmodule SymphonyElixir.K8s.Pod do
  @moduledoc false

  # Ephemeral, per-run Kubernetes pods. Each pod is launched with a single
  # `kubectl run -i --rm` process whose stdin the orchestrator holds open for the
  # whole run (see `create/3`). The pod's PID 1 is a keepalive that reads stdin and
  # exits on EOF, so closing that connection — or the orchestrator dying — stops the
  # pod, and `--rm` removes it. Zombie reaping is handled by injecting
  # `spec.shareProcessNamespace: true` (the pod's `pause` container becomes PID 1 and
  # reaps). Commands still run via `kubectl exec` (see `SymphonyElixir.K8s`), reusing
  # the operator's kubeconfig / in-cluster credentials.

  require Logger

  alias SymphonyElixir.Config.Schema
  alias SymphonyElixir.K8s

  @runner_label "symphony-runner"
  @max_name_length 63

  # PID 1 keepalive: read stdin, discard it, and exit the moment stdin hits EOF.
  @keepalive_command ["/bin/sh", "-c", "cat >/dev/null"]

  # `kubectl wait` errors immediately if the pod object does not exist yet, so we
  # bridge the brief create race with a few short retries before the real Ready wait.
  @wait_retry_interval_ms 200
  @wait_max_retries 25

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
  Launches the pod with a single `kubectl run -i --rm` process and blocks until it
  is Ready. Returns `{:ok, port}` where `port` is the held connection: keep it open
  for the run's lifetime and hand it to `close/1` at teardown.

  On any failure the connection is closed and the pod deleted (best effort) so the
  caller never leaks a half-started pod.
  """
  @spec create(String.t(), Schema.t() | map(), term()) :: {:ok, port()} | {:error, term()}
  def create(pod_name, settings, issue \\ nil) when is_binary(pod_name) do
    k8s = kubernetes(settings)
    issue_id = issue && issue_identifier(issue)

    with :ok <- ensure_configured(k8s),
         {:ok, port} <- start_run_port(pod_name, k8s, issue_id) do
      case wait_ready(pod_name, k8s) do
        :ok ->
          Logger.info("Started symphony runner pod pod=#{pod_name} namespace=#{k8s.namespace}")
          {:ok, port}

        {:error, reason} ->
          _ = safe_close(port)
          _ = delete(pod_name, settings)
          {:error, {:pod_start_failed, pod_name, reason}}
      end
    else
      {:error, reason} ->
        {:error, {:pod_start_failed, pod_name, reason}}
    end
  end

  # Opens the `kubectl run -i --rm` connection as a port. No output is expected
  # (the keepalive discards stdin and writes nothing), so the port stays silent
  # until it is closed at teardown.
  defp start_run_port(pod_name, k8s, issue_id) do
    case K8s.kubectl_executable() do
      {:ok, executable} ->
        args = run_args(build_manifest(pod_name, k8s, issue_id), k8s)

        port =
          Port.open(
            {:spawn_executable, String.to_charlist(executable)},
            [:binary, args: Enum.map(args, &String.to_charlist/1)]
          )

        {:ok, port}

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    error -> {:error, {:run_failed, Exception.message(error)}}
  end

  @doc """
  Closes the held run connection. Its stdin EOF stops the pod's keepalive command,
  which — together with `--rm` — deletes the pod. Never raises.
  """
  @spec close(port() | term()) :: :ok
  def close(port), do: safe_close(port)

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
    |> put_share_process_namespace()
    |> put_keepalive_command(k8s)
  end

  @doc false
  @spec run_args(map(), map()) :: [String.t()]
  def run_args(manifest, k8s) do
    pod_name = get_in(manifest, ["metadata", "name"])

    base_args(k8s) ++
      [
        "run",
        pod_name,
        "--image=#{container_image(manifest, k8s)}",
        "--restart=Never",
        "--rm",
        "-i",
        "--pod-running-timeout=#{ready_timeout_seconds(k8s)}s",
        "--overrides=#{Jason.encode!(manifest)}",
        "--command",
        "--"
      ] ++ @keepalive_command
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

  # Make the pod's `pause` container PID 1 for the whole pod so it reaps the zombies
  # left behind by exec'd commands. `put_new` lets an explicit template value win.
  defp put_share_process_namespace(manifest) do
    spec = ensure_map(Map.get(manifest, "spec"))
    Map.put(manifest, "spec", Map.put_new(spec, "shareProcessNamespace", true))
  end

  # Force the target container to the stdin-reading keepalive so the pod dies on EOF.
  # Symphony owns the lifecycle contract, so this replaces any command in the template.
  defp put_keepalive_command(manifest, k8s) do
    spec = ensure_map(Map.get(manifest, "spec"))
    containers = spec |> Map.get("containers") |> ensure_list()

    containers =
      case containers do
        [] ->
          [%{"name" => "runner", "command" => @keepalive_command}]

        _ ->
          index = target_container_index(containers, k8s)

          List.update_at(containers, index, fn container ->
            Map.put(ensure_map(container), "command", @keepalive_command)
          end)
      end

    Map.put(manifest, "spec", Map.put(spec, "containers", containers))
  end

  defp container_image(manifest, k8s) do
    containers = get_in(manifest, ["spec", "containers"]) || []

    case Enum.at(containers, target_container_index(containers, k8s)) do
      %{"image" => image} when is_binary(image) and image != "" -> image
      _ -> @runner_label
    end
  end

  # The container Symphony execs into / injects the keepalive command on: the one
  # named by `container` if configured, else the first.
  defp target_container_index(containers, %{container: name}) when is_binary(name) and name != "" do
    case Enum.find_index(containers, &(Map.get(&1, "name") == name)) do
      nil -> 0
      index -> index
    end
  end

  defp target_container_index(_containers, _k8s), do: 0

  defp wait_ready(pod_name, k8s), do: wait_ready(pod_name, k8s, @wait_max_retries)

  defp wait_ready(pod_name, k8s, retries) do
    args =
      base_args(k8s) ++
        [
          "wait",
          "--for=condition=Ready",
          "pod/#{pod_name}",
          "--timeout=#{ready_timeout_seconds(k8s)}s"
        ]

    case run_kubectl(args) do
      {_output, 0} ->
        :ok

      {output, status} ->
        if retries > 0 and pod_not_found?(output) do
          Process.sleep(@wait_retry_interval_ms)
          wait_ready(pod_name, k8s, retries - 1)
        else
          {:error, {:wait_failed, status, output}}
        end
    end
  end

  defp pod_not_found?(output) when is_binary(output) do
    String.contains?(output, "not found") or String.contains?(output, "NotFound")
  end

  defp pod_not_found?(_output), do: false

  defp ready_timeout_seconds(k8s), do: max(1, div(k8s.ready_timeout_ms || 120_000, 1000))

  defp safe_close(port) when is_port(port) do
    Port.close(port)
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp safe_close(_port), do: :ok

  defp ensure_list(value) when is_list(value), do: value
  defp ensure_list(_value), do: []

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
