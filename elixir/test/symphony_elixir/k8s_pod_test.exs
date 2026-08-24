defmodule SymphonyElixir.K8s.PodTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.K8s.{Pod, Protocol}

  @reader_command ["/bin/bash", "-c", Protocol.reader_script()]

  describe "generate_name/1" do
    test "produces an RFC-1123-compliant, unique name" do
      name = Pod.generate_name(%{identifier: "ENG-123"})

      assert name =~ ~r/^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/
      assert String.length(name) <= 63
      assert String.starts_with?(name, "symphony-eng-123-")

      refute Pod.generate_name(%{identifier: "ENG-123"}) ==
               Pod.generate_name(%{identifier: "ENG-123"})
    end

    test "sanitizes and falls back for missing identifiers" do
      assert Pod.generate_name(nil) =~ ~r/^symphony-issue-[0-9a-f]+$/
    end
  end

  describe "parse_pod_ages/2" do
    test "computes each pod's age from its creation timestamp" do
      # 2026-01-01T00:00:00Z is 100 seconds before `now`.
      now = DateTime.to_unix(~U[2026-01-01T00:01:40Z])

      output = """
      symphony-eng-1-aaaa 2026-01-01T00:00:00Z
      symphony-eng-2-bbbb 2026-01-01T00:01:30Z
      """

      assert [{"symphony-eng-1-aaaa", 100}, {"symphony-eng-2-bbbb", 10}] =
               Pod.parse_pod_ages(output, now)
    end

    test "skips blank lines and treats unparseable timestamps as age 0" do
      now = DateTime.to_unix(~U[2026-01-01T00:01:40Z])

      output = "\nsymphony-eng-1-aaaa not-a-timestamp\n\n"

      assert [{"symphony-eng-1-aaaa", 0}] = Pod.parse_pod_ages(output, now)
    end
  end

  describe "build_manifest/3" do
    test "injects name, namespace, labels, activeDeadlineSeconds, share-PID and reader command" do
      k8s = %{
        namespace: "symphony",
        container: nil,
        active_deadline_seconds: 1800,
        pod_template: %{
          "spec" => %{"containers" => [%{"name" => "runner", "image" => "img"}]}
        }
      }

      manifest = Pod.build_manifest("symphony-eng-9-abcd", k8s, "ENG-9")

      assert manifest["apiVersion"] == "v1"
      assert manifest["kind"] == "Pod"
      assert manifest["metadata"]["name"] == "symphony-eng-9-abcd"
      assert manifest["metadata"]["namespace"] == "symphony"
      assert manifest["metadata"]["labels"]["app"] == Pod.runner_label()
      assert manifest["metadata"]["labels"]["symphony/issue"] == "eng-9"
      assert manifest["spec"]["activeDeadlineSeconds"] == 1800
      assert manifest["spec"]["shareProcessNamespace"] == true

      assert manifest["spec"]["containers"] == [
               %{
                 "name" => "runner",
                 "image" => "img",
                 "command" => @reader_command,
                 "stdin" => true,
                 "stdinOnce" => true
               }
             ]
    end

    test "injects the reader command into the configured container" do
      k8s = %{
        namespace: "symphony",
        container: "runner",
        active_deadline_seconds: 1800,
        pod_template: %{
          "spec" => %{
            "containers" => [
              %{"name" => "sidecar", "image" => "side"},
              %{"name" => "runner", "image" => "img"}
            ]
          }
        }
      }

      manifest = Pod.build_manifest("pod", k8s, nil)

      assert [%{"name" => "sidecar"} = sidecar, runner] = manifest["spec"]["containers"]
      refute Map.has_key?(sidecar, "command")
      refute Map.has_key?(sidecar, "stdin")
      assert runner["command"] == @reader_command
      assert runner["stdin"] == true
      assert runner["stdinOnce"] == true
    end

    test "respects a user-provided activeDeadlineSeconds and shareProcessNamespace" do
      k8s = %{
        namespace: "symphony",
        container: nil,
        active_deadline_seconds: 1800,
        pod_template: %{
          "spec" => %{"activeDeadlineSeconds" => 60, "shareProcessNamespace" => false}
        }
      }

      manifest = Pod.build_manifest("pod", k8s, nil)

      assert manifest["spec"]["activeDeadlineSeconds"] == 60
      assert manifest["spec"]["shareProcessNamespace"] == false
      refute Map.has_key?(manifest["metadata"]["labels"], "symphony/issue")
    end
  end

  describe "run_args/2" do
    test "builds a kubectl run -i --rm invocation carrying the manifest as overrides" do
      k8s = %{
        namespace: "symphony",
        container: nil,
        kubectl_context: "my-cluster",
        ready_timeout_ms: 90_000,
        active_deadline_seconds: 1800,
        pod_template: %{
          "spec" => %{"containers" => [%{"name" => "runner", "image" => "img"}]}
        }
      }

      manifest = Pod.build_manifest("symphony-eng-9-abcd", k8s, "ENG-9")
      args = Pod.run_args(manifest, k8s)

      assert ["--context", "my-cluster", "-n", "symphony", "run", "symphony-eng-9-abcd" | _] = args
      assert "--image=img" in args
      assert "--restart=Never" in args
      assert "--rm" in args
      assert "-i" in args
      assert "--pod-running-timeout=90s" in args
      assert List.last(args) == Protocol.reader_script()
      assert Enum.take(args, -3) == @reader_command

      overrides = Enum.find(args, &String.starts_with?(&1, "--overrides="))
      assert overrides
      assert "--overrides=" <> json = overrides
      assert Jason.decode!(json) == manifest
    end
  end
end
