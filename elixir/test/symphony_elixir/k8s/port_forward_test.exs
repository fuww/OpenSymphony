defmodule SymphonyElixir.K8s.PortForwardTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.K8s.PortForward

  # Drives `PortForward` against a fake `kubectl` on PATH, so the spawn/parse/teardown and
  # every error branch are exercised without a real cluster — the same fakes-on-PATH style
  # `K8s.Broker` and the claude ssh tests use.
  @kubernetes %{
    namespace: "symphony-test",
    pod_template: %{"spec" => %{"containers" => [%{"name" => "runner", "image" => "img"}]}}
  }

  setup do
    test_root = Path.join(System.tmp_dir!(), "symphony-port-forward-#{System.unique_integer([:positive])}")
    File.mkdir_p!(test_root)
    on_exit(fn -> File.rm_rf(test_root) end)
    %{test_root: test_root}
  end

  test "start parses the forwarded local port and keeps the connection open", %{test_root: test_root} do
    install_fake_kubectl!(test_root)
    configure_kubernetes!()

    assert {:ok, port, 12_345} = PortForward.start("success-pod", 8080, %{read_timeout_ms: 2_000})
    assert is_port(port)
    assert Port.info(port) != nil

    assert :ok = PortForward.stop(port)
    wait_for_port_closed!(port)
  end

  test "start surfaces a non-zero kubectl exit", %{test_root: test_root} do
    install_fake_kubectl!(test_root)
    configure_kubernetes!()

    assert {:error, {:port_forward_exit, 7}} =
             PortForward.start("exit-pod", 8080, %{read_timeout_ms: 2_000})
  end

  test "start times out when kubectl never announces a local port", %{test_root: test_root} do
    install_fake_kubectl!(test_root)
    configure_kubernetes!()

    assert {:error, :port_forward_timeout} =
             PortForward.start("timeout-pod", 8080, %{read_timeout_ms: 200})
  end

  test "start reports when kubectl is missing", %{test_root: test_root} do
    previous_path = System.get_env("PATH")
    System.put_env("PATH", Path.join(test_root, "empty-bin"))
    on_exit(fn -> restore_env("PATH", previous_path) end)

    configure_kubernetes!()

    assert {:error, :kubectl_not_found} = PortForward.start("any-pod", 8080)
  end

  test "start reports when kubernetes is not configured", %{test_root: test_root} do
    install_fake_kubectl!(test_root)
    # Default workflow leaves worker.kubernetes unset.
    assert {:error, :kubernetes_not_configured} = PortForward.start("any-pod", 8080)
  end

  test "stop is a no-op for nil and non-ports and idempotent on a closed port" do
    assert :ok = PortForward.stop(nil)
    assert :ok = PortForward.stop(:not_a_port)

    bash = System.find_executable("bash")

    port =
      Port.open(
        {:spawn_executable, String.to_charlist(bash)},
        [:binary, :exit_status, args: [~c"-c", ~c"while true; do sleep 1; done"]]
      )

    assert :ok = PortForward.stop(port)
    wait_for_port_closed!(port)
    assert :ok = PortForward.stop(port)
  end

  defp configure_kubernetes! do
    write_workflow_file!(Workflow.workflow_file_path(),
      agent_backend: "opencode",
      worker_mode: "kubernetes",
      worker_kubernetes: @kubernetes
    )
  end

  # A fake `kubectl` that dispatches on the `pod/<name>` argument so each test can pick a
  # behaviour without racing a shared env var. The success case prints a non-matching line
  # first (exercising the log-and-keep-reading path) before announcing the local port.
  defp install_fake_kubectl!(test_root) do
    bin = Path.join(test_root, "bin")
    File.mkdir_p!(bin)
    kubectl = Path.join(bin, "kubectl")

    File.write!(kubectl, """
    #!/bin/sh
    args="$*"
    case "$args" in
      *exit-pod*)
        echo "error: pod not found" >&2
        exit 7
        ;;
      *timeout-pod*)
        sleep 10
        ;;
      *)
        echo "Handling connection for 0"
        echo "Forwarding from 127.0.0.1:12345 -> 8080"
        echo "Forwarding from [::1]:12345 -> 8080"
        while true; do sleep 1; done
        ;;
    esac
    """)

    File.chmod!(kubectl, 0o755)

    previous_path = System.get_env("PATH")
    System.put_env("PATH", bin <> ":" <> (previous_path || ""))
    on_exit(fn -> restore_env("PATH", previous_path) end)

    kubectl
  end

  defp wait_for_port_closed!(port, attempts \\ 80)
  defp wait_for_port_closed!(_port, 0), do: flunk("timed out waiting for port to close")

  defp wait_for_port_closed!(port, attempts) do
    if :erlang.port_info(port) == :undefined do
      :ok
    else
      Process.sleep(25)
      wait_for_port_closed!(port, attempts - 1)
    end
  end
end
