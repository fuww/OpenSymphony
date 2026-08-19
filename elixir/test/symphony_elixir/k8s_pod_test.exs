defmodule SymphonyElixir.K8s.PodTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.K8s.Pod

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

  describe "build_manifest/3" do
    test "injects name, namespace, labels and activeDeadlineSeconds into the template" do
      k8s = %{
        namespace: "symphony",
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
      assert manifest["spec"]["containers"] == [%{"name" => "runner", "image" => "img"}]
    end

    test "respects a user-provided activeDeadlineSeconds" do
      k8s = %{
        namespace: "symphony",
        active_deadline_seconds: 1800,
        pod_template: %{"spec" => %{"activeDeadlineSeconds" => 60}}
      }

      manifest = Pod.build_manifest("pod", k8s, nil)

      assert manifest["spec"]["activeDeadlineSeconds"] == 60
      refute Map.has_key?(manifest["metadata"]["labels"], "symphony/issue")
    end
  end
end
