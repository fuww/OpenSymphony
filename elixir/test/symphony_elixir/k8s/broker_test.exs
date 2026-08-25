defmodule SymphonyElixir.K8s.BrokerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias SymphonyElixir.K8s.{Broker, Protocol}

  # Every test drives a broker against a local bash process running the REAL protocol
  # reader script, so the sentinel framing and FIFO relay are exercised end to end
  # without a cluster.
  setup do
    bash = System.find_executable("bash")
    pod = "broker-test-#{System.unique_integer([:positive])}"

    {:ok, broker} =
      Broker.start_link(name: pod, executable: bash, args: ["-c", Protocol.reader_script()])

    on_exit(fn -> if Process.alive?(broker), do: Process.exit(broker, :kill) end)

    %{broker: broker, pod: pod}
  end

  test "run returns command stdout and a zero exit status", %{broker: broker} do
    assert {:ok, {output, 0}} = Broker.run(broker, "echo hello")
    assert output =~ "hello"
  end

  test "run captures a non-zero exit status", %{broker: broker} do
    assert {:ok, {_output, 7}} = Broker.run(broker, "exit 7")
  end

  test "run preserves tab-delimited marker output (workspace-style)", %{broker: broker} do
    assert {:ok, {output, 0}} =
             Broker.run(broker, "printf '%s\\t%s\\t%s\\n' MARK 1 /work/dir")

    assert output =~ "MARK\t1\t/work/dir"
    refute output =~ Protocol.marker()
  end

  test "runs are serialized and complete in order", %{broker: broker} do
    assert {:ok, {a, 0}} = Broker.run(broker, "echo first")
    assert {:ok, {b, 0}} = Broker.run(broker, "echo second")
    assert a =~ "first"
    assert b =~ "second"
  end

  test "agent stream relays input to the agent and forwards its stdout", %{broker: broker} do
    # `cat` echoes whatever we feed its stdin straight back to its stdout.
    assert {:ok, ref} = Broker.start_agent(broker, "cat")

    assert :ok = Broker.send_input(broker, ref, "ping\n")
    assert_receive {:agent_stream, ^ref, {:data, {:eol, "ping"}}}, 2_000

    assert :ok = Broker.end_agent(broker, ref)
    assert_receive {:agent_stream, ^ref, {:exit_status, _status}}, 2_000
  end

  test "a sync command runs after the agent ends (after_run ordering)", %{broker: broker} do
    {:ok, ref} = Broker.start_agent(broker, "cat")
    :ok = Broker.end_agent(broker, ref)
    assert_receive {:agent_stream, ^ref, {:exit_status, _status}}, 2_000

    assert {:ok, {output, 0}} = Broker.run(broker, "echo after-run")
    assert output =~ "after-run"
  end

  # A `kubectl run` that aborts (bad flag, admission denial, forbidden) prints to stderr and
  # exits non-zero before the pod exists, so `create/3`'s `kubectl wait` only sees `NotFound`.
  # The broker must surface that stderr — durably in a log (it usually dies before `create/3`
  # can query it) and, while still alive, via `recent_output/1`.
  test "logs the captured connection stderr at warning when the connection exits non-zero" do
    bash = System.find_executable("bash")
    pod = "broker-fail-#{System.unique_integer([:positive])}"

    log =
      capture_log(fn ->
        {:ok, broker} =
          Broker.start_link(
            name: pod,
            executable: bash,
            args: ["-c", "echo 'error: unknown flag: --container' >&2; exit 3"]
          )

        ref = Process.monitor(broker)
        assert_receive {:DOWN, ^ref, :process, ^broker, :normal}, 2_000
      end)

    assert log =~ "connection exited status=3"
    assert log =~ "unknown flag: --container"
  end

  test "recent_output returns [] once the broker has died" do
    dead = spawn(fn -> :ok end)
    ref = Process.monitor(dead)
    assert_receive {:DOWN, ^ref, :process, ^dead, _reason}, 1_000

    assert Broker.recent_output(dead) == []
  end
end
