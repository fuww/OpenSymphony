defmodule SymphonyElixir.RemoteTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.Remote

  @pod_template %{"spec" => %{"containers" => [%{"name" => "runner", "image" => "img"}]}}

  setup do
    previous_path = System.get_env("PATH")
    test_root = Path.join(System.tmp_dir!(), "symphony-remote-#{System.unique_integer([:positive])}")
    File.mkdir_p!(test_root)

    trace = Path.join(test_root, "trace")
    write_fake(Path.join(test_root, "kubectl"), "kubectl", trace)
    write_fake(Path.join(test_root, "ssh"), "ssh", trace)

    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    %{trace: trace}
  end

  test "dispatches to kubectl in kubernetes mode", %{trace: trace} do
    write_workflow_file!(Workflow.workflow_file_path(),
      agent_backend: "codex",
      worker_mode: "kubernetes",
      worker_kubernetes: %{namespace: "symphony-test", pod_template: @pod_template}
    )

    assert {:ok, {_output, 0}} = Remote.run("some-pod", "echo hi")
    assert File.read!(trace) =~ "kubectl:"
  end

  test "dispatches to ssh in ssh mode", %{trace: trace} do
    write_workflow_file!(Workflow.workflow_file_path(),
      agent_backend: "codex",
      worker_ssh_hosts: ["worker-01"]
    )

    assert {:ok, {_output, 0}} = Remote.run("worker-01", "echo hi")
    assert File.read!(trace) =~ "ssh:"
  end

  defp write_fake(path, label, trace) do
    File.write!(path, """
    #!/bin/sh
    printf '#{label}:%s\\n' "$*" >> "#{trace}"
    exit 0
    """)

    File.chmod!(path, 0o755)
  end
end
