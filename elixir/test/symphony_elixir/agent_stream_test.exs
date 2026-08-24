defmodule SymphonyElixir.AgentStreamTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.AgentStream

  describe ":port mode" do
    setup do
      bash = System.find_executable("bash")

      port =
        Port.open(
          {:spawn_executable, String.to_charlist(bash)},
          [:binary, :exit_status, :stderr_to_stdout, {:line, 1_048_576}, args: [~c"-c", ~c"cat"]]
        )

      on_exit(fn -> if Port.info(port), do: Port.close(port) end)
      %{port: port}
    end

    test "wraps a real port and reports it open with an os_pid", %{port: port} do
      stream = AgentStream.from_port(port)

      assert %AgentStream{mode: :port, port: ^port, ref: nil} = stream
      assert AgentStream.open?(stream)
      assert is_integer(AgentStream.os_pid(stream))
    end

    test "send_input writes to the port and the owner receives the echo", %{port: port} do
      stream = AgentStream.from_port(port)

      assert :ok = AgentStream.send_input(stream, "hi\n")
      assert_receive {^port, {:data, {:eol, "hi"}}}, 2_000
    end

    test "close tears the port down", %{port: port} do
      stream = AgentStream.from_port(port)

      assert :ok = AgentStream.close(stream)
      refute AgentStream.open?(stream)
    end
  end

  describe ":broker mode" do
    test "from_broker builds a broker-backed handle" do
      ref = make_ref()
      stream = AgentStream.from_broker(self(), ref)

      assert %AgentStream{mode: :broker, broker: broker, ref: ^ref, port: nil} = stream
      assert broker == self()
      assert AgentStream.open?(stream)
    end
  end
end
