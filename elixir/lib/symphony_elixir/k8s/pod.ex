defmodule SymphonyElixir.K8s.Pod do
  @moduledoc false

  # Ephemeral, per-run Kubernetes pods. Each pod is launched with a single
  # `kubectl run -i --rm` process whose stdin the orchestrator holds open for the
  # whole run (see `create/3`). The pod's PID 1 is the protocol reader from
  # `SymphonyElixir.K8s.Protocol`: it reads framed commands from stdin, runs them as its
  # own children (so all output flows to the pod's stdout and is visible via
  # `kubectl logs`), and exits the moment stdin hits EOF — so closing the connection, or
  # the orchestrator dying, stops the pod and `--rm` removes it. There is exactly one
  # `kubectl run` and no `kubectl exec`. The held connection is owned by a
  # `SymphonyElixir.K8s.Broker` (returned by `create/3`); commands run through it.
  # Zombie reaping is handled by injecting `spec.shareProcessNamespace: true` (the pod's
  # `pause` container becomes PID 1 and reaps).

  require Logger

  alias SymphonyElixir.Config.Schema
  alias SymphonyElixir.K8s
  alias SymphonyElixir.K8s.{Broker, Protocol}

  @runner_label "symphony-runner"
  @default_container_name "runner"
  @max_name_length 63
  @broker_supervisor SymphonyElixir.K8s.BrokerSupervisor

  # PID 1 command: the framed protocol reader (bash). See `K8s.Protocol.reader_script/0`.
  defp reader_command, do: ["/bin/bash", "-c", Protocol.reader_script()]

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
  is Ready. Returns `{:ok, broker}` where `broker` is the `K8s.Broker` pid that owns the
  held connection: keep it for the run's lifetime and hand it to `close/1` at teardown.

  On any failure the connection is closed and the pod deleted (best effort) so the
  caller never leaks a half-started pod.
  """
  @spec create(String.t(), Schema.t() | map(), term()) :: {:ok, pid()} | {:error, term()}
  def create(pod_name, settings, issue \\ nil) when is_binary(pod_name) do
    k8s = kubernetes(settings)
    issue_id = issue && issue_identifier(issue)

    with :ok <- ensure_configured(k8s),
         {:ok, broker} <- start_broker(pod_name, k8s, issue_id) do
      case wait_ready(pod_name, k8s) do
        :ok ->
          confirm_broker_started(broker, pod_name, settings, k8s)

        {:error, reason} ->
          _ = stop_broker(broker)
          _ = delete(pod_name, settings, wait: true)
          {:error, {:pod_start_failed, pod_name, reason}}
      end
    else
      {:error, reason} ->
        {:error, {:pod_start_failed, pod_name, reason}}
    end
  end

  # `wait_ready` only proves that *a* pod with this name is Ready — not that this
  # broker's `kubectl run -i` connection actually attached. When the connection
  # dies early (classically because the name collided and kubectl exited with
  # `AlreadyExists`, matching a leftover Ready pod owned by nobody), the broker
  # process is already gone and every command would fail `{:no_broker_for_pod}`.
  # Treat a dead broker as a failed start and tear the pod down synchronously so a
  # retry does not inherit an orphan.
  defp confirm_broker_started(broker, pod_name, settings, k8s) do
    if Process.alive?(broker) do
      Logger.info("Started symphony runner pod pod=#{pod_name} namespace=#{k8s.namespace}")
      {:ok, broker}
    else
      _ = stop_broker(broker)
      _ = delete(pod_name, settings, wait: true)
      {:error, {:pod_start_failed, pod_name, :broker_connection_lost}}
    end
  end

  # Starts a `K8s.Broker` that opens the single `kubectl run -i --rm` connection. The
  # broker owns the port and multiplexes every command over it (no `kubectl exec`).
  defp start_broker(pod_name, k8s, issue_id) do
    case K8s.kubectl_executable() do
      {:ok, executable} ->
        args = run_args(build_manifest(pod_name, k8s, issue_id), k8s)
        spec = {Broker, name: pod_name, executable: executable, args: args}

        case DynamicSupervisor.start_child(@broker_supervisor, spec) do
          {:ok, broker} -> {:ok, broker}
          {:error, reason} -> {:error, {:broker_start_failed, reason}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    error -> {:error, {:run_failed, Exception.message(error)}}
  end

  @doc """
  Closes the held run connection by stopping its broker. The broker closing its port
  sends stdin EOF to the reader, which — together with `--rm` — deletes the pod. Never
  raises. Accepts a broker pid (a raw port is still accepted for safety).
  """
  @spec close(pid() | port() | term()) :: :ok
  def close(broker) when is_pid(broker), do: stop_broker(broker)
  def close(port) when is_port(port), do: safe_close(port)
  def close(_other), do: :ok

  defp stop_broker(broker) when is_pid(broker) do
    DynamicSupervisor.terminate_child(@broker_supervisor, broker)
    :ok
  catch
    :exit, _reason -> :ok
  end

  defp stop_broker(_broker), do: :ok

  @doc """
  Deletes the pod. Never raises — a failed delete is logged so leaks are findable.

  Pass `wait: true` to block until the pod is gone (bounded by a short timeout); teardown
  after a failed `create/3` uses this so a leftover pod cannot linger and collide with a
  retry. The default `wait: false` fires the delete and returns.
  """
  @spec delete(String.t(), Schema.t() | map(), keyword()) :: :ok
  def delete(pod_name, settings, opts \\ []) when is_binary(pod_name) do
    k8s = kubernetes(settings)

    args = base_args(k8s) ++ ["delete", "pod", pod_name, "--ignore-not-found"] ++ delete_wait_args(opts)

    case run_kubectl(args) do
      {_output, 0} ->
        :ok

      {output, status} ->
        Logger.warning(
          "Failed to delete symphony runner pod pod=#{pod_name} namespace=#{k8s.namespace} status=#{status} output=#{inspect(String.slice(output, 0, 512))}"
        )

        :ok
    end
  rescue
    error ->
      Logger.warning("Error deleting symphony runner pod pod=#{pod_name} error=#{Exception.message(error)}")
      :ok
  end

  @doc """
  Reaps leaked runner pods so they do not accumulate.

  With no options (the boot call) it bulk-deletes every runner pod: at boot no runs are
  active, so any pod present was leaked by a prior crash that skipped teardown.

  Called periodically mid-run it must not touch live pods, so pass:

    * `keep:` — a set/list of pod names belonging to active runs, never deleted.
    * `min_age_seconds:` — only reap pods at least this old, so a pod created between two
      poll cycles (not yet reflected in `keep`) is not mistaken for an orphan.
  """
  @spec reap_orphans(Schema.t() | map(), keyword()) :: :ok
  def reap_orphans(settings, opts \\ []) do
    k8s = kubernetes(settings)

    case ensure_configured(k8s) do
      :ok -> do_reap_orphans(k8s, opts)
      {:error, _reason} -> :ok
    end
  rescue
    error ->
      Logger.warning("Error reaping orphan runner pods error=#{Exception.message(error)}")
      :ok
  end

  # Boot path: no active runs, so bulk-delete all runner pods in one call.
  defp do_reap_orphans(k8s, []) do
    args =
      base_args(k8s) ++
        ["delete", "pods", "-l", "app=#{@runner_label}", "--ignore-not-found", "--wait=false"]

    case run_kubectl(args) do
      {_output, 0} ->
        :ok

      {output, status} ->
        Logger.warning(
          "Failed to reap orphan runner pods namespace=#{k8s.namespace} status=#{status} output=#{inspect(String.slice(output, 0, 512))}"
        )

        :ok
    end
  end

  # Mid-run path: delete only runner pods that no active run owns and that are old enough
  # to not be a just-created pod racing the `keep` set.
  defp do_reap_orphans(k8s, opts) do
    keep = opts |> Keyword.get(:keep, []) |> Enum.into(MapSet.new())
    min_age = Keyword.get(opts, :min_age_seconds, 0)

    k8s
    |> list_runner_pods()
    |> Enum.filter(fn {name, age_seconds} ->
      not MapSet.member?(keep, name) and age_seconds >= min_age
    end)
    |> Enum.each(fn {name, age_seconds} ->
      Logger.info("Reaping orphan runner pod pod=#{name} namespace=#{k8s.namespace} age_seconds=#{age_seconds}")
      delete(name, k8s)
    end)

    :ok
  end

  # Returns `[{pod_name, age_seconds}]` for every runner pod. On any failure returns `[]`
  # so a reap cycle that cannot list simply does nothing.
  defp list_runner_pods(k8s) do
    args =
      base_args(k8s) ++
        [
          "get",
          "pods",
          "-l",
          "app=#{@runner_label}",
          "-o",
          "jsonpath=" <> pod_age_jsonpath()
        ]

    case run_kubectl(args) do
      {output, 0} -> parse_pod_ages(output)
      _ -> []
    end
  end

  defp pod_age_jsonpath do
    ~S({range .items[*]}{.metadata.name}{" "}{.metadata.creationTimestamp}{"\n"}{end})
  end

  @doc false
  @spec parse_pod_ages(String.t(), integer()) :: [{String.t(), integer()}]
  def parse_pod_ages(output, now_seconds \\ nil) do
    now = now_seconds || System.os_time(:second)

    output
    |> String.split("\n", trim: true)
    |> Enum.flat_map(fn line ->
      case String.split(line, " ", trim: true) do
        [name, timestamp] -> [{name, pod_age_seconds(timestamp, now)}]
        _ -> []
      end
    end)
  end

  # Unparseable timestamps yield age 0 so an ambiguous pod is treated as "too young to
  # reap" rather than risk deleting a live one.
  defp pod_age_seconds(timestamp, now) do
    case DateTime.from_iso8601(timestamp) do
      {:ok, datetime, _offset} -> max(0, now - DateTime.to_unix(datetime))
      _ -> 0
    end
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
    |> put_protocol_reader_command(k8s)
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
        # Attach to the reader container explicitly. Without this, a multi-container pod
        # (e.g. a `dind` sidecar) makes kubectl pick the first container and print a
        # `Defaulted container "..." out of: ...` banner to stderr, which then shows up as
        # stray output on the broker's connection.
        "--container=#{target_container_name(manifest, k8s)}",
        "--pod-running-timeout=#{ready_timeout_seconds(k8s)}s",
        "--overrides=#{Jason.encode!(manifest)}",
        "--command",
        "--"
      ] ++ reader_command()
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

  # Force the target container to the stdin-reading protocol reader so the pod dies on
  # EOF. Symphony owns the lifecycle contract, so this replaces any command in the
  # template. `stdin`/`stdinOnce` must be set on the container itself: `kubectl run -i`
  # only adds them to a container it generates, but `--overrides` supplies the full
  # container spec and wins, so without these the reader gets a closed stdin and exits
  # at once.
  defp put_protocol_reader_command(manifest, k8s) do
    spec = ensure_map(Map.get(manifest, "spec"))
    containers = spec |> Map.get("containers") |> ensure_list()

    containers =
      case containers do
        [] ->
          [
            %{
              "name" => @default_container_name,
              "command" => reader_command(),
              "stdin" => true,
              "stdinOnce" => true
            }
          ]

        _ ->
          index = target_container_index(containers, k8s)

          List.update_at(containers, index, fn container ->
            Map.merge(ensure_map(container), %{
              "command" => reader_command(),
              "stdin" => true,
              "stdinOnce" => true
            })
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

  defp target_container_name(manifest, k8s) do
    containers = get_in(manifest, ["spec", "containers"]) || []

    case Enum.at(containers, target_container_index(containers, k8s)) do
      %{"name" => name} when is_binary(name) and name != "" -> name
      _ -> @default_container_name
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

  # `--wait=true` blocks until the object is gone; cap it so a stuck delete cannot hang a
  # failed `create/3` indefinitely. `--now` drops the grace period to make it prompt.
  defp delete_wait_args(opts) do
    if Keyword.get(opts, :wait, false) do
      ["--wait=true", "--now", "--timeout=30s"]
    else
      ["--wait=false"]
    end
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
