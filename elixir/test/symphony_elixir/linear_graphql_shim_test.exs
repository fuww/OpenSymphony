defmodule SymphonyElixir.LinearGraphqlShimTest do
  # Drives the emitted Claude MCP shim (`.symphony/claude/linear_graphql_mcp.js`)
  # under `node` against a fake Linear endpoint. Every request the shim makes is
  # recorded, so the assertions are about wire behaviour: how many times Linear
  # was called, what it was asked, and what the MCP client got back.
  use ExUnit.Case, async: true

  alias SymphonyElixir.Linear.GraphqlTool

  @moduletag :tmp_dir

  @issue_query "query Issue($id: String!) { issue(id: $id) { id title } }"
  @issue_data %{"data" => %{"issue" => %{"id" => "abc", "title" => "Fix the widget"}}}
  @budget_headers [{"x-ratelimit-requests-limit", "2500"}, {"x-ratelimit-requests-remaining", "2499"}]

  defmodule FakeLinearState do
    use Agent

    def start_link(responses) do
      Agent.start_link(fn -> %{responses: responses, requests: []} end)
    end

    # The last scripted response repeats forever so "always rate limited"
    # scenarios do not need to know how many attempts the shim will make.
    def next_response(state, request) do
      Agent.get_and_update(state, fn
        %{responses: [response]} = current ->
          {response, %{current | requests: current.requests ++ [request]}}

        %{responses: [response | rest]} = current ->
          {response, %{current | responses: rest, requests: current.requests ++ [request]}}
      end)
    end

    def requests(state), do: Agent.get(state, & &1.requests)
  end

  defmodule FakeLinearPlug do
    import Plug.Conn

    def init(opts), do: opts

    def call(conn, opts) do
      state = Keyword.fetch!(opts, :state)
      {:ok, body, conn} = read_body(conn)
      request = Jason.decode!(body)

      {status, headers, payload} =
        case FakeLinearState.next_response(state, request) do
          response when is_function(response, 0) -> response.()
          response -> response
        end

      headers
      |> Enum.reduce(conn, fn {name, value}, acc -> put_resp_header(acc, name, value) end)
      |> put_resp_content_type("application/json")
      |> send_resp(status, Jason.encode!(payload))
    end
  end

  setup %{tmp_dir: tmp_dir} do
    node = System.find_executable("node")
    sh = System.find_executable("sh")

    if is_nil(node) or is_nil(sh) do
      flunk("node and sh are required to exercise the emitted MCP shim")
    end

    script = Path.join(tmp_dir, "linear_graphql_mcp.js")
    File.write!(script, GraphqlTool.claude_mcp_server_source())

    {:ok, %{node: node, sh: sh, script: script, tmp_dir: tmp_dir}}
  end

  test "retries a RATELIMITED rejection after x-ratelimit-requests-reset and then succeeds", ctx do
    reset_in_ms = 60

    shim =
      start_shim!(ctx, [
        fn ->
          {400, [{"x-ratelimit-requests-reset", epoch_ms_in(reset_in_ms)} | exhausted_headers()], rate_limited_body()}
        end,
        {200, @budget_headers, @issue_data}
      ])

    started = System.monotonic_time(:millisecond)
    response = call(shim, 1, %{"query" => @issue_query, "variables" => %{"id" => "abc"}})
    elapsed = System.monotonic_time(:millisecond) - started

    assert {false, @issue_data} = decode_result(response)
    assert length(FakeLinearState.requests(shim.state)) == 2
    assert elapsed >= reset_in_ms - 10, "expected the shim to wait for the reset, waited #{elapsed}ms"
  end

  test "never sleeps zero for a reset in the past", ctx do
    shim =
      start_shim!(ctx, [
        fn ->
          {400, [{"x-ratelimit-requests-reset", epoch_ms_in(-5_000)} | exhausted_headers()], rate_limited_body()}
        end,
        {200, @budget_headers, @issue_data}
      ])

    started = System.monotonic_time(:millisecond)
    response = call(shim, 1, %{"query" => @issue_query, "variables" => %{"id" => "abc"}})
    elapsed = System.monotonic_time(:millisecond) - started

    assert {false, @issue_data} = decode_result(response)
    assert length(FakeLinearState.requests(shim.state)) == 2
    # A reset already in the past is floored to the 20ms minimum.
    assert elapsed >= 20
  end

  # A retry sent before the advertised reset is rejected again and spends one
  # more request of the shared budget, so a reset further out than
  # SYMPHONY_LINEAR_MAX_BACKOFF_MS is reported at once rather than retried early.
  # The decoy is the pre-fix behaviour: capping the wait and retrying anyway made
  # every retry here land while the budget was still exhausted.
  test "does not retry before a reset beyond SYMPHONY_LINEAR_MAX_BACKOFF_MS", ctx do
    reset_at = epoch_ms_in(3_600_000)

    shim =
      start_shim!(ctx, [
        {400, [{"x-ratelimit-requests-reset", reset_at} | exhausted_headers()], rate_limited_body()},
        {200, @budget_headers, @issue_data}
      ])

    started = System.monotonic_time(:millisecond)
    response = call(shim, 1, %{"query" => @issue_query, "variables" => %{"id" => "abc"}})
    elapsed = System.monotonic_time(:millisecond) - started

    assert {true, payload} = decode_result(response)
    assert payload["error"]["status"] == 400
    assert %{"attempts" => 1, "remaining" => "0"} = payload["error"]["rateLimit"]
    assert payload["error"]["rateLimit"]["resetAt"] == String.to_integer(reset_at)
    assert length(FakeLinearState.requests(shim.state)) == 1
    assert elapsed < 2_000, "the shim waited #{elapsed}ms for a reset it was never going to reach"
    assert File.read!(shim.log) =~ "ratelimited reset_beyond_cap"
  end

  test "gives up after SYMPHONY_LINEAR_MAX_RETRIES and reports the budget it saw", ctx do
    shim = start_shim!(ctx, [{400, exhausted_headers(), rate_limited_body()}], [{"SYMPHONY_LINEAR_MAX_RETRIES", "2"}])

    started = System.monotonic_time(:millisecond)
    response = call(shim, 1, %{"query" => @issue_query, "variables" => %{"id" => "abc"}})
    elapsed = System.monotonic_time(:millisecond) - started

    assert {true, payload} = decode_result(response)
    assert payload["error"]["status"] == 400
    assert payload["error"]["body"] == rate_limited_body()
    assert %{"attempts" => 3, "remaining" => "0", "limit" => "2500"} = payload["error"]["rateLimit"]
    assert length(FakeLinearState.requests(shim.state)) == 3
    # No reset header: exponential fallback from the 20ms floor, still never zero.
    assert elapsed >= 20 + 40
  end

  test "detects RATELIMITED from errors[].extensions.code, not from the response text", ctx do
    decoy_error = %{
      "errors" => [
        %{
          "message" => "Argument Validation Error: title must not be RATELIMITED",
          "extensions" => %{"code" => "INVALID_INPUT", "type" => "invalid input"}
        }
      ]
    }

    decoy_data = %{"data" => %{"issue" => %{"id" => "abc", "title" => "RATELIMITED banner on the homepage"}}}

    shim = start_shim!(ctx, [{400, @budget_headers, decoy_error}, {200, @budget_headers, decoy_data}])

    # A substring check is fooled by both bodies; the structural check must not be.
    assert Jason.encode!(decoy_error) =~ "RATELIMITED"
    assert Jason.encode!(decoy_data) =~ "RATELIMITED"

    assert {true, payload} = decode_result(call(shim, 1, %{"query" => @issue_query, "variables" => %{"id" => "abc"}}))
    assert payload["error"]["body"] == decoy_error
    refute Map.has_key?(payload["error"], "rateLimit")
    assert length(FakeLinearState.requests(shim.state)) == 1

    assert {false, ^decoy_data} =
             decode_result(call(shim, 2, %{"query" => @issue_query, "variables" => %{"id" => "abc"}}))

    assert length(FakeLinearState.requests(shim.state)) == 2
  end

  test "never replays an error that waiting cannot fix", ctx do
    auth_error = %{
      "errors" => [
        %{
          "message" => "Authentication required, not authenticated",
          "extensions" => %{"code" => "AUTHENTICATION_ERROR"}
        }
      ]
    }

    shim = start_shim!(ctx, [{401, @budget_headers, auth_error}])

    assert {true, payload} = decode_result(call(shim, 1, %{"query" => @issue_query, "variables" => %{"id" => "abc"}}))
    assert payload["error"]["status"] == 401
    assert payload["error"]["body"] == auth_error
    assert length(FakeLinearState.requests(shim.state)) == 1
  end

  test "serves repeat reads of the same query and variables from the per-run cache", ctx do
    other_issue = %{"data" => %{"issue" => %{"id" => "def", "title" => "Another"}}}

    shim = start_shim!(ctx, [{200, @budget_headers, @issue_data}, {200, @budget_headers, other_issue}])

    first = %{"query" => @issue_query, "variables" => %{"id" => "abc", "first" => 1}}
    reordered = %{"query" => @issue_query, "variables" => %{"first" => 1, "id" => "abc"}}

    assert {false, @issue_data} = decode_result(call(shim, 1, first))
    assert {false, @issue_data} = decode_result(call(shim, 2, first))
    assert {false, @issue_data} = decode_result(call(shim, 3, reordered))
    assert length(FakeLinearState.requests(shim.state)) == 1

    assert {false, ^other_issue} =
             decode_result(call(shim, 4, %{"query" => @issue_query, "variables" => %{"id" => "def"}}))

    assert length(FakeLinearState.requests(shim.state)) == 2

    assert File.read!(shim.log) =~ "cache=hit"
  end

  test "a mutation is never cached and flushes cached reads", ctx do
    mutation = "mutation Update($id: String!) { issueUpdate(id: $id, input: { title: \"x\" }) { success } }"
    mutation_data = %{"data" => %{"issueUpdate" => %{"success" => true}}}
    refreshed = %{"data" => %{"issue" => %{"id" => "abc", "title" => "x"}}}

    shim =
      start_shim!(ctx, [
        {200, @budget_headers, @issue_data},
        {200, @budget_headers, mutation_data},
        {200, @budget_headers, mutation_data},
        {200, @budget_headers, refreshed}
      ])

    read = %{"query" => @issue_query, "variables" => %{"id" => "abc"}}

    assert {false, @issue_data} = decode_result(call(shim, 1, read))

    assert {false, ^mutation_data} =
             decode_result(call(shim, 2, %{"query" => mutation, "variables" => %{"id" => "abc"}}))

    assert {false, ^mutation_data} =
             decode_result(call(shim, 3, %{"query" => mutation, "variables" => %{"id" => "abc"}}))

    assert {false, ^refreshed} = decode_result(call(shim, 4, read))

    assert Enum.map(FakeLinearState.requests(shim.state), & &1["query"]) == [
             @issue_query,
             mutation,
             mutation,
             @issue_query
           ]
  end

  test "a fragment before a mutation still counts as a mutation and an anonymous selection as a query", ctx do
    with_fragment = """
    # Update, then re-read through a fragment.
    fragment IssueFields on Issue { id title }
    mutation Update($id: String!) { issueUpdate(id: $id, input: { title: "x" }) { issue { ...IssueFields } } }
    """

    anonymous = "{ viewer { id } }"

    shim = start_shim!(ctx, [{200, @budget_headers, @issue_data}])

    assert {false, @issue_data} = decode_result(call(shim, 1, %{"query" => with_fragment}))
    assert {false, @issue_data} = decode_result(call(shim, 2, %{"query" => with_fragment}))
    assert length(FakeLinearState.requests(shim.state)) == 2

    assert {false, @issue_data} = decode_result(call(shim, 3, %{"query" => anonymous}))
    assert {false, @issue_data} = decode_result(call(shim, 4, %{"query" => anonymous}))
    assert length(FakeLinearState.requests(shim.state)) == 3
  end

  test "failed reads are not cached", ctx do
    validation_error = %{
      "errors" => [%{"message" => "Entity not found", "extensions" => %{"code" => "ENTITY_NOT_FOUND"}}]
    }

    shim = start_shim!(ctx, [{200, @budget_headers, validation_error}, {200, @budget_headers, @issue_data}])
    read = %{"query" => @issue_query, "variables" => %{"id" => "abc"}}

    assert {true, ^validation_error} = decode_result(call(shim, 1, read))
    assert {false, @issue_data} = decode_result(call(shim, 2, read))
    assert length(FakeLinearState.requests(shim.state)) == 2
  end

  test "SYMPHONY_LINEAR_CACHE_TTL_MS=0 disables the cache", ctx do
    shim = start_shim!(ctx, [{200, @budget_headers, @issue_data}], [{"SYMPHONY_LINEAR_CACHE_TTL_MS", "0"}])
    read = %{"query" => @issue_query, "variables" => %{"id" => "abc"}}

    assert {false, @issue_data} = decode_result(call(shim, 1, read))
    assert {false, @issue_data} = decode_result(call(shim, 2, read))
    assert length(FakeLinearState.requests(shim.state)) == 2
  end

  test "logs the remaining request budget on every response", ctx do
    shim = start_shim!(ctx, [{200, @budget_headers, @issue_data}])

    assert {false, @issue_data} =
             decode_result(call(shim, 1, %{"query" => @issue_query, "variables" => %{"id" => "abc"}}))

    log = File.read!(shim.log)
    assert log =~ "linear_graphql budget remaining=2499 limit=2500"
    assert log =~ "status=200"
    assert File.read!(shim.stderr) =~ "remaining=2499"
  end

  test "still runs and logs when the workspace package.json makes the script an ES module", ctx do
    File.write!(Path.join(ctx.tmp_dir, "package.json"), ~s({"type": "module"}))
    shim = start_shim!(ctx, [{200, @budget_headers, @issue_data}], [{"SYMPHONY_LINEAR_LOG_PATH", nil}])

    assert {false, @issue_data} =
             decode_result(call(shim, 1, %{"query" => @issue_query, "variables" => %{"id" => "abc"}}))

    assert {false, @issue_data} =
             decode_result(call(shim, 2, %{"query" => @issue_query, "variables" => %{"id" => "abc"}}))

    assert length(FakeLinearState.requests(shim.state)) == 1

    assert File.read!(shim.stderr) =~ "remaining=2499"
    assert File.read!(Path.join(ctx.tmp_dir, "linear_graphql.log")) =~ "remaining=2499"
  end

  test "keeps the argument validation and missing-key failures", ctx do
    shim = start_shim!(ctx, [{200, @budget_headers, @issue_data}], [{"SYMPHONY_LINEAR_API_KEY", ""}])

    assert {true, %{"error" => %{"message" => message}}} = decode_result(call(shim, 1, %{"query" => "   "}))
    assert message =~ "non-empty `query`"

    assert {true, %{"error" => %{"message" => message}}} = decode_result(call(shim, 2, %{"query" => @issue_query}))
    assert message =~ "missing Linear auth"
    assert FakeLinearState.requests(shim.state) == []
  end

  defp start_shim!(ctx, responses, extra_env \\ []) do
    {:ok, state} = start_supervised(%{id: FakeLinearState, start: {FakeLinearState, :start_link, [responses]}})
    bandit = start_supervised!({Bandit, plug: {FakeLinearPlug, state: state}, port: 0, ip: {127, 0, 0, 1}})
    {:ok, {_ip, port_number}} = ThousandIsland.listener_info(bandit)

    log = Path.join(ctx.tmp_dir, "linear_graphql.log")
    stderr = Path.join(ctx.tmp_dir, "stderr.log")

    env =
      [
        {"SYMPHONY_LINEAR_ENDPOINT", "http://127.0.0.1:#{port_number}/graphql"},
        {"SYMPHONY_LINEAR_API_KEY", "lin_api_test"},
        {"SYMPHONY_LINEAR_MIN_BACKOFF_MS", "20"},
        {"SYMPHONY_LINEAR_MAX_BACKOFF_MS", "80"},
        {"SYMPHONY_LINEAR_MAX_RETRIES", "3"},
        {"SYMPHONY_LINEAR_LOG_PATH", log}
      ]
      |> Map.new()
      |> Map.merge(Map.new(extra_env))
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
      |> Enum.map(fn {key, value} -> {String.to_charlist(key), String.to_charlist(value)} end)

    # stderr is redirected to a file so the stdout MCP framing stays clean and
    # the budget lines can still be asserted on.
    port =
      Port.open({:spawn_executable, ctx.sh}, [
        :binary,
        :exit_status,
        args: ["-c", ~s(exec "$0" "$1" 2>"$2"), ctx.node, ctx.script, stderr],
        env: env
      ])

    on_exit(fn ->
      try do
        Port.close(port)
      rescue
        ArgumentError -> :ok
      end
    end)

    %{port: port, state: state, log: log, stderr: stderr}
  end

  defp call(shim, id, arguments) do
    request(shim, %{
      "jsonrpc" => "2.0",
      "id" => id,
      "method" => "tools/call",
      "params" => %{"name" => GraphqlTool.tool_name(), "arguments" => arguments}
    })
  end

  defp request(shim, message) do
    payload = Jason.encode!(message)
    true = Port.command(shim.port, "Content-Length: #{byte_size(payload)}\r\n\r\n#{payload}")
    read_frame(shim.port, "")
  end

  defp read_frame(port, buffer) do
    case parse_frame(buffer) do
      {:ok, message} ->
        message

      :incomplete ->
        receive do
          {^port, {:data, chunk}} -> read_frame(port, buffer <> chunk)
          {^port, {:exit_status, status}} -> flunk("shim exited with status #{status}")
        after
          5_000 -> flunk("timed out waiting for the shim; buffer=#{inspect(buffer)}")
        end
    end
  end

  defp parse_frame(buffer) do
    with [header, rest] <- String.split(buffer, "\r\n\r\n", parts: 2),
         [_, length] <- Regex.run(~r/content-length:\s*(\d+)/i, header),
         length = String.to_integer(length),
         true <- byte_size(rest) >= length do
      {:ok, rest |> binary_part(0, length) |> Jason.decode!()}
    else
      _ -> :incomplete
    end
  end

  defp decode_result(%{"result" => %{"isError" => is_error, "content" => [%{"type" => "text", "text" => text}]}}) do
    {is_error, Jason.decode!(text)}
  end

  defp rate_limited_body do
    %{
      "errors" => [
        %{
          "message" => "Rate limit exceeded",
          "extensions" => %{"code" => "RATELIMITED", "type" => "rate limited"}
        }
      ]
    }
  end

  defp exhausted_headers do
    [{"x-ratelimit-requests-limit", "2500"}, {"x-ratelimit-requests-remaining", "0"}]
  end

  defp epoch_ms_in(offset_ms) do
    Integer.to_string(System.os_time(:millisecond) + offset_ms)
  end
end
