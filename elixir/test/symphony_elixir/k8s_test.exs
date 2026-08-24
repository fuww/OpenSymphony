defmodule SymphonyElixir.K8sTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.AgentStream
  alias SymphonyElixir.K8s
  alias SymphonyElixir.K8s.{Broker, Protocol}

  @pod_template %{
    "spec" => %{"containers" => [%{"name" => "runner", "image" => "ghcr.io/org/runner:latest"}]}
  }

  setup do
    write_workflow_file!(Workflow.workflow_file_path(),
      agent_backend: "codex",
      worker_mode: "kubernetes",
      worker_kubernetes: %{
        namespace: "symphony-test",
        container: "runner",
        pod_template: @pod_template
      }
    )

    :ok
  end

  # Drives a broker against a local bash process running the real protocol reader, so the
  # whole transport can be exercised without a cluster or kubectl.
  defp start_reader_broker(pod) do
    bash = System.find_executable("bash")
    {:ok, broker} = Broker.start_link(name: pod, executable: bash, args: ["-c", Protocol.reader_script()])
    on_exit(fn -> if Process.alive?(broker), do: Process.exit(broker, :kill) end)
    broker
  end

  test "run routes a command to the pod's broker and returns output + exit status" do
    pod = "symphony-pod-#{System.unique_integer([:positive])}"
    start_reader_broker(pod)

    assert {:ok, {output, 0}} = K8s.run(pod, "echo hello")
    assert output =~ "hello"

    assert {:ok, {_output, 3}} = K8s.run(pod, "exit 3")
  end

  test "run returns an error when no broker exists for the pod" do
    assert {:error, {:no_broker_for_pod, "missing-pod"}} = K8s.run("missing-pod", "echo hi")
  end

  test "open_agent_stream returns a broker-backed AgentStream that can be ended" do
    pod = "symphony-pod-#{System.unique_integer([:positive])}"
    start_reader_broker(pod)

    assert {:ok, %AgentStream{mode: :broker} = stream} = K8s.open_agent_stream(pod, "cat")
    assert :ok = AgentStream.close(stream)

    ref = stream.ref
    assert_receive {:agent_stream, ^ref, {:exit_status, _status}}, 2_000
  end

  test "kubectl_executable reports when kubectl is missing" do
    previous_path = System.get_env("PATH")
    System.put_env("PATH", "/nonexistent-bin")

    on_exit(fn -> restore_env("PATH", previous_path) end)

    assert {:error, :kubectl_not_found} = K8s.kubectl_executable()
  end
end
