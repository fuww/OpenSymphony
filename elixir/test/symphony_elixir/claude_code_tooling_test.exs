defmodule SymphonyElixir.ClaudeCodeToolingTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.ClaudeCode.Tooling
  alias SymphonyElixir.Linear.GraphqlTool

  test "bootstrap_workspace writes Claude MCP config, server, and git exclude entry" do
    test_root = temp_root!("linear")

    try do
      workspace_root = Path.join(test_root, "workspaces")
      workspace = Path.join(workspace_root, "MT-CLAUDE-TOOLING")
      git_info_dir = Path.join([workspace, ".git", "info"])

      File.mkdir_p!(git_info_dir)

      write_workflow_file!(Workflow.workflow_file_path(),
        agent_backend: "claude",
        workspace_root: workspace_root
      )

      assert :ok = Tooling.bootstrap_workspace(workspace)

      config_path = Path.join(workspace, ".symphony/claude/mcp.json")
      server_path = Path.join(workspace, ".symphony/claude/linear_graphql_mcp.js")
      exclude_path = Path.join(git_info_dir, "exclude")

      assert File.exists?(config_path)
      assert File.exists?(server_path)
      assert File.exists?(exclude_path)

      assert %{
               "mcpServers" => %{
                 "symphony-linear" => %{
                   "command" => "node",
                   "args" => [".symphony/claude/linear_graphql_mcp.js"]
                 }
               }
             } = config_path |> File.read!() |> Jason.decode!()

      server_source = File.read!(server_path)
      assert server_source =~ "linear_graphql"
      # MCP stdio is newline-delimited JSON. LSP-style Content-Length framing
      # never matched Claude Code's NDJSON `initialize`, so the server never
      # replied and every session timed out connecting to it.
      refute server_source =~ "Content-Length"
      assert File.read!(exclude_path) =~ ".symphony/"
    after
      File.rm_rf(test_root)
    end
  end

  @node System.find_executable("node")

  @tag skip: if(@node, do: false, else: "node is not installed")
  test "generated Claude MCP server speaks newline-delimited JSON on stdio" do
    test_root = temp_root!("ndjson")
    File.mkdir_p!(test_root)

    try do
      server_path = Path.join(test_root, "linear_graphql_mcp.js")
      File.write!(server_path, GraphqlTool.claude_mcp_server_source())

      # Two messages in one chunk, one split across a chunk boundary, and a
      # blank line to skip: the server must frame purely on newlines.
      input =
        Enum.map_join(
          [
            %{
              "jsonrpc" => "2.0",
              "id" => 1,
              "method" => "initialize",
              "params" => %{
                "protocolVersion" => "2025-06-18",
                "capabilities" => %{},
                "clientInfo" => %{"name" => "test", "version" => "0"}
              }
            },
            %{"jsonrpc" => "2.0", "method" => "notifications/initialized"},
            %{"jsonrpc" => "2.0", "id" => 2, "method" => "tools/list"}
          ],
          "\n",
          &Jason.encode!/1
        ) <> "\n\n"

      output = run_node_server(server_path, input, expected_lines: 2)

      responses =
        output
        |> String.split("\n", trim: true)
        |> Enum.map(&Jason.decode!/1)

      assert [
               %{
                 "jsonrpc" => "2.0",
                 "id" => 1,
                 "result" => %{
                   "protocolVersion" => "2025-06-18",
                   "capabilities" => %{"tools" => %{}},
                   "serverInfo" => %{"name" => "symphony-linear-graphql"}
                 }
               },
               %{"jsonrpc" => "2.0", "id" => 2, "result" => %{"tools" => [%{"name" => "linear_graphql"}]}}
             ] = responses

      refute output =~ "Content-Length"
    after
      File.rm_rf(test_root)
    end
  end

  # Drives the generated server the way an MCP client does: keep stdin open,
  # write NDJSON, and read NDJSON replies until `expected_lines` arrived.
  defp run_node_server(server_path, input, expected_lines: expected_lines) do
    port =
      Port.open({:spawn_executable, @node}, [
        :binary,
        :use_stdio,
        :stderr_to_stdout,
        args: [server_path]
      ])

    # Deliver the payload in two chunks so a message straddles the boundary.
    {first, second} = String.split_at(input, div(byte_size(input), 2))
    Port.command(port, first)
    Port.command(port, second)

    try do
      collect_port_output(port, "", expected_lines)
    after
      Port.close(port)
    end
  end

  defp collect_port_output(port, acc, expected_lines) do
    if length(String.split(acc, "\n", trim: true)) >= expected_lines and String.ends_with?(acc, "\n") do
      acc
    else
      receive do
        {^port, {:data, data}} -> collect_port_output(port, acc <> data, expected_lines)
      after
        5_000 ->
          flunk("node MCP server sent no NDJSON reply within 5s; output so far: #{inspect(acc)}")
      end
    end
  end

  test "bootstrap_workspace omits Linear MCP wiring when the tracker is not Linear" do
    test_root = temp_root!("memory")

    try do
      workspace_root = Path.join(test_root, "workspaces")
      workspace = Path.join(workspace_root, "MT-CLAUDE-MEMORY")
      git_info_dir = Path.join([workspace, ".git", "info"])
      server_path = Path.join(workspace, ".symphony/claude/linear_graphql_mcp.js")

      File.mkdir_p!(git_info_dir)
      File.mkdir_p!(Path.dirname(server_path))
      File.write!(server_path, "stale server")

      write_workflow_file!(Workflow.workflow_file_path(),
        agent_backend: "claude",
        tracker_kind: "memory",
        workspace_root: workspace_root
      )

      assert :ok = Tooling.bootstrap_workspace(workspace)

      config_path = Path.join(workspace, ".symphony/claude/mcp.json")
      exclude_path = Path.join(git_info_dir, "exclude")

      assert File.exists?(config_path)
      refute File.exists?(server_path)
      assert File.read!(exclude_path) =~ ".symphony/"
      assert %{"mcpServers" => %{}} = config_path |> File.read!() |> Jason.decode!()
    after
      File.rm_rf(test_root)
    end
  end

  defp temp_root!(suffix) do
    Path.join(
      System.tmp_dir!(),
      "symphony-elixir-claude-tooling-#{suffix}-#{System.unique_integer([:positive])}"
    )
  end
end
