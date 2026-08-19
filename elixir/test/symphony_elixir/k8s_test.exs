defmodule SymphonyElixir.K8sTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.K8s

  @pod_template %{
    "spec" => %{"containers" => [%{"name" => "runner", "image" => "ghcr.io/org/runner:latest"}]}
  }

  setup do
    previous_path = System.get_env("PATH")
    test_root = Path.join(System.tmp_dir!(), "symphony-k8s-#{System.unique_integer([:positive])}")
    File.mkdir_p!(test_root)
    trace = Path.join(test_root, "kubectl.trace")
    fake_kubectl = Path.join(test_root, "kubectl")

    File.write!(fake_kubectl, """
    #!/bin/sh
    printf 'ARGV:%s\\n' "$*" >> "#{trace}"
    exit 0
    """)

    File.chmod!(fake_kubectl, 0o755)
    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    write_workflow_file!(Workflow.workflow_file_path(),
      agent_backend: "codex",
      worker_mode: "kubernetes",
      worker_kubernetes: %{
        namespace: "symphony-test",
        container: "runner",
        pod_template: @pod_template
      }
    )

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    %{trace: trace}
  end

  test "run execs the command inside the pod via kubectl", %{trace: trace} do
    assert {:ok, {_output, 0}} = K8s.run("symphony-pod-1", "echo hello")

    argv = File.read!(trace)
    assert argv =~ "exec -i"
    assert argv =~ "-n symphony-test"
    assert argv =~ "-c runner"
    assert argv =~ "symphony-pod-1 -- bash -lc echo hello"
  end

  test "start_port opens a streaming port on kubectl exec" do
    assert {:ok, port} = K8s.start_port("symphony-pod-2", "echo hi")
    assert is_port(port)

    receive do
      {^port, {:exit_status, _status}} -> :ok
    after
      2_000 -> :ok
    end
  end

  test "returns an error when kubectl is unavailable" do
    previous_path = System.get_env("PATH")
    System.put_env("PATH", "/nonexistent-bin")

    on_exit(fn -> restore_env("PATH", previous_path) end)

    assert {:error, :kubectl_not_found} = K8s.run("symphony-pod-3", "echo hi")
  end
end
