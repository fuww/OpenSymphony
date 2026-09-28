defmodule SymphonyElixir.Linear.GraphqlTool do
  @moduledoc false

  alias SymphonyElixir.Linear.Client

  @tool_name "linear_graphql"
  @description """
  Execute a raw GraphQL query or mutation against Linear using Symphony's configured auth.
  """
  @input_schema %{
    "type" => "object",
    "additionalProperties" => false,
    "required" => ["query"],
    "properties" => %{
      "query" => %{
        "type" => "string",
        "description" => "GraphQL query or mutation document to execute against Linear."
      },
      "variables" => %{
        "type" => ["object", "null"],
        "description" => "Optional GraphQL variables object.",
        "additionalProperties" => true
      }
    }
  }

  @spec tool_name() :: String.t()
  def tool_name, do: @tool_name

  @spec description() :: String.t()
  def description, do: @description

  @spec input_schema() :: map()
  def input_schema, do: @input_schema

  @spec tool_spec() :: map()
  def tool_spec do
    %{
      "name" => tool_name(),
      "description" => description(),
      "inputSchema" => input_schema()
    }
  end

  @spec supported_tool_names() :: [String.t()]
  def supported_tool_names, do: [tool_name()]

  @spec execute(term(), keyword()) :: map()
  def execute(arguments, opts \\ []) do
    linear_client = Keyword.get(opts, :linear_client, &Client.graphql/3)

    with {:ok, query, variables} <- normalize_arguments(arguments),
         {:ok, response} <- linear_client.(query, variables, []) do
      graphql_response(response)
    else
      {:error, reason} ->
        failure_response(tool_error_payload(reason))
    end
  end

  @spec open_code_tool_source() :: String.t()
  def open_code_tool_source do
    missing_query = tool_error_payload(:missing_query) |> Jason.encode!(pretty: true)
    missing_api_key = tool_error_payload(:missing_linear_api_token) |> Jason.encode!(pretty: true)
    transport_failure = transport_failure_message()
    http_failure_prefix = http_failure_message_prefix()

    """
    import { tool } from "@opencode-ai/plugin";
    import { z } from "zod";

    const ENDPOINT = process.env.SYMPHONY_LINEAR_ENDPOINT || "https://api.linear.app/graphql";
    const API_KEY = process.env.SYMPHONY_LINEAR_API_KEY;
    const MISSING_QUERY_PAYLOAD = #{inspect(missing_query)};
    const MISSING_API_KEY_PAYLOAD = #{inspect(missing_api_key)};
    const TRANSPORT_FAILURE_MESSAGE = #{inspect(transport_failure)};
    const HTTP_FAILURE_PREFIX = #{inspect(http_failure_prefix)};

    const format = (value: unknown) => JSON.stringify(value, null, 2);

    const fail = (payload: unknown): never => {
      throw new Error(format(payload));
    };

    export default tool({
      description: #{inspect(description())},
      args: {
        query: z.string().min(1),
        variables: z.record(z.string(), z.unknown()).nullable().optional(),
      },
      async execute(args) {
        const query = args.query.trim();

        if (!query) {
          fail(JSON.parse(MISSING_QUERY_PAYLOAD));
        }

        if (!API_KEY) {
          fail(JSON.parse(MISSING_API_KEY_PAYLOAD));
        }

        try {
          const response = await fetch(ENDPOINT, {
            method: "POST",
            headers: {
              Authorization: API_KEY,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              query,
              variables: args.variables ?? {},
            }),
          });

          const json = await response.json();

          if (!response.ok) {
            fail({
              error: {
                message: `${HTTP_FAILURE_PREFIX}${response.status}.`,
                status: response.status,
                body: json,
              },
            });
          }

          if (Array.isArray(json?.errors) && json.errors.length > 0) {
            fail(json);
          }

          return format(json);
        } catch (error) {
          fail({
            error: {
              message: TRANSPORT_FAILURE_MESSAGE,
              reason: error instanceof Error ? error.message : String(error),
            },
          });
        }
      },
    });
    """
  end

  @spec claude_mcp_server_source() :: String.t()
  def claude_mcp_server_source do
    missing_query = tool_error_payload(:missing_query) |> Jason.encode!(pretty: true)
    invalid_arguments = tool_error_payload(:invalid_arguments) |> Jason.encode!(pretty: true)
    invalid_variables = tool_error_payload(:invalid_variables) |> Jason.encode!(pretty: true)
    missing_api_key = tool_error_payload(:missing_linear_api_token) |> Jason.encode!(pretty: true)
    transport_failure = transport_failure_message()
    http_failure_prefix = http_failure_message_prefix()
    unsupported = tool_error_payload({:unsupported_tool, supported_tool_names()}) |> Jason.encode!(pretty: true)

    """
    #!/usr/bin/env node
    // The nearest package.json decides whether node treats this file as CommonJS
    // or ESM, so neither `require` nor `import` can be assumed. Built-ins are
    // resolved through whichever loader exists; without one, file logging is off.
    const fs = loadBuiltin("node:fs");
    const path = loadBuiltin("node:path");
    const TOOL_NAME = #{inspect(tool_name())};
    const DESCRIPTION = #{inspect(description())};
    const INPUT_SCHEMA = #{Jason.encode!(input_schema())};
    const MISSING_QUERY_PAYLOAD = #{inspect(missing_query)};
    const INVALID_ARGUMENTS_PAYLOAD = #{inspect(invalid_arguments)};
    const INVALID_VARIABLES_PAYLOAD = #{inspect(invalid_variables)};
    const MISSING_API_KEY_PAYLOAD = #{inspect(missing_api_key)};
    const UNSUPPORTED_PAYLOAD = #{inspect(unsupported)};
    const TRANSPORT_FAILURE_MESSAGE = #{inspect(transport_failure)};
    const HTTP_FAILURE_PREFIX = #{inspect(http_failure_prefix)};
    const ENDPOINT = process.env.SYMPHONY_LINEAR_ENDPOINT || "https://api.linear.app/graphql";
    const API_KEY = process.env.SYMPHONY_LINEAR_API_KEY;
    // Linear's request budget is per user, so every session running under one
    // API key shares the same pool. The shim therefore backs off on a rate-limit
    // rejection, serves repeat reads from a per-run cache and logs the remaining
    // budget on every response. The bounds are tunable through the environment.
    const MIN_BACKOFF_MS = readInt(process.env.SYMPHONY_LINEAR_MIN_BACKOFF_MS, 1000, 1);
    const MAX_BACKOFF_MS = Math.max(MIN_BACKOFF_MS, readInt(process.env.SYMPHONY_LINEAR_MAX_BACKOFF_MS, 900000, 1));
    const MAX_RETRIES = readInt(process.env.SYMPHONY_LINEAR_MAX_RETRIES, 4, 0);
    const CACHE_TTL_MS = readInt(process.env.SYMPHONY_LINEAR_CACHE_TTL_MS, 300000, 0);
    const LOG_PATH =
      process.env.SYMPHONY_LINEAR_LOG_PATH === undefined
        ? defaultLogPath()
        : process.env.SYMPHONY_LINEAR_LOG_PATH;
    // Linear signals exhaustion as HTTP 400 with this GraphQL error code (not
    // 429, and without Retry-After); x-ratelimit-requests-reset (epoch ms) says
    // when the budget refills.
    const RATE_LIMIT_ERROR_CODE = "RATELIMITED";
    const RESET_HEADER = "x-ratelimit-requests-reset";
    const REMAINING_HEADER = "x-ratelimit-requests-remaining";
    const LIMIT_HEADER = "x-ratelimit-requests-limit";
    const OPERATION_TOKEN = /\\b(query|mutation|subscription|fragment)\\b|[{}]/g;

    const cache = new Map();
    let buffer = "";

    function loadBuiltin(name) {
      if (typeof require === "function") {
        return require(name);
      }

      if (typeof process.getBuiltinModule === "function") {
        return process.getBuiltinModule(name);
      }

      return null;
    }

    function defaultLogPath() {
      const script = process.argv[1];

      if (!path || typeof script !== "string" || script === "") {
        return "";
      }

      return path.join(path.dirname(script), "linear_graphql.log");
    }

    function readInt(raw, fallback, minimum) {
      const value = Number.parseInt(raw ?? "", 10);
      return Number.isFinite(value) && value >= minimum ? value : fallback;
    }

    function log(fields) {
      const line = `${new Date().toISOString()} linear_graphql ${fields}\\n`;
      process.stderr.write(line);

      if (LOG_PATH && fs) {
        try {
          fs.appendFileSync(LOG_PATH, line);
        } catch (_error) {
          // Logging must never fail a request.
        }
      }
    }

    function sleep(ms) {
      return new Promise((resolve) => setTimeout(resolve, ms));
    }

    // Classifies the document by its first top-level definition. Fragments may
    // precede the operation; an anonymous selection set is a query.
    function operationType(query) {
      const source = query.replace(/#[^\\n\\r]*/g, "");
      let depth = 0;
      let inFragment = false;

      for (const match of source.matchAll(OPERATION_TOKEN)) {
        const token = match[0];

        if (token === "{") {
          if (depth === 0 && !inFragment) {
            return "query";
          }

          depth += 1;
        } else if (token === "}") {
          depth = Math.max(0, depth - 1);
          inFragment = inFragment && depth > 0;
        } else if (depth === 0) {
          if (token !== "fragment") {
            return token;
          }

          inFragment = true;
        }
      }

      return "unknown";
    }

    function stableStringify(value) {
      if (Array.isArray(value)) {
        return `[${value.map(stableStringify).join(",")}]`;
      }

      if (value && typeof value === "object") {
        const entries = Object.keys(value)
          .sort()
          .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`);
        return `{${entries.join(",")}}`;
      }

      return JSON.stringify(value) ?? "null";
    }

    function rateLimited(response, payload) {
      if (response.status === 429) {
        return true;
      }

      const errors = Array.isArray(payload?.errors) ? payload.errors : [];

      return errors.some(
        (entry) =>
          entry !== null &&
          typeof entry === "object" &&
          entry.extensions !== null &&
          typeof entry.extensions === "object" &&
          entry.extensions.code === RATE_LIMIT_ERROR_CODE,
      );
    }

    function readBudget(response) {
      const reset = Number.parseInt(response.headers.get(RESET_HEADER) ?? "", 10);

      return {
        remaining: response.headers.get(REMAINING_HEADER) ?? "unknown",
        limit: response.headers.get(LIMIT_HEADER) ?? "unknown",
        resetAt: Number.isFinite(reset) && reset > 0 ? reset : null,
      };
    }

    // Wait until the budget resets, never less than the floor, with a little
    // jitter so ten sessions do not retry in lockstep. Without a usable reset
    // header fall back to exponential growth from the floor, capped. A reset
    // further out than the cap returns null: a retry sent before the reset is
    // rejected again and only spends more of the shared budget.
    function backoffMs(resetAt, attempt) {
      const wanted = resetAt === null ? MIN_BACKOFF_MS * 2 ** attempt : resetAt - Date.now();

      if (resetAt !== null && wanted > MAX_BACKOFF_MS) {
        return null;
      }

      const bounded = Math.min(MAX_BACKOFF_MS, Math.max(MIN_BACKOFF_MS, wanted));
      const jitter = Math.floor(Math.random() * Math.max(1, Math.floor(bounded / 10)));
      return Math.min(MAX_BACKOFF_MS, bounded + jitter);
    }

    function send(message) {
      const payload = JSON.stringify(message);
      process.stdout.write(`Content-Length: ${Buffer.byteLength(payload, "utf8")}\\r\\n\\r\\n${payload}`);
    }

    function sendResult(id, result) {
      send({ jsonrpc: "2.0", id, result });
    }

    function sendError(id, code, message, data) {
      send({ jsonrpc: "2.0", id, error: { code, message, data } });
    }

    function parseJson(value, fallback) {
      try {
        return JSON.parse(value);
      } catch (_error) {
        return fallback;
      }
    }

    function format(value) {
      return JSON.stringify(value, null, 2);
    }

    function successResponse(payload) {
      const text = format(payload);
      return { content: [{ type: "text", text }], isError: false };
    }

    function failureResponse(payload) {
      const text = format(payload);
      return { content: [{ type: "text", text }], isError: true };
    }

    function normalizeArguments(args) {
      if (typeof args === "string") {
        const query = args.trim();
        if (!query) {
          return { ok: false, payload: parseJson(MISSING_QUERY_PAYLOAD, { error: { message: "Missing query" } }) };
        }

        return { ok: true, query, variables: {} };
      }

      if (!args || typeof args !== "object" || Array.isArray(args)) {
        return { ok: false, payload: parseJson(INVALID_ARGUMENTS_PAYLOAD, { error: { message: "Invalid arguments" } }) };
      }

      const query = typeof args.query === "string" ? args.query.trim() : "";

      if (!query) {
        return { ok: false, payload: parseJson(MISSING_QUERY_PAYLOAD, { error: { message: "Missing query" } }) };
      }

      const variables = args.variables ?? {};

      if (variables === null) {
        return { ok: true, query, variables: {} };
      }

      if (typeof variables !== "object" || Array.isArray(variables)) {
        return { ok: false, payload: parseJson(INVALID_VARIABLES_PAYLOAD, { error: { message: "Invalid variables" } }) };
      }

      return { ok: true, query, variables };
    }

    async function executeTool(args) {
      const normalized = normalizeArguments(args);

      if (!normalized.ok) {
        return failureResponse(normalized.payload);
      }

      if (!API_KEY) {
        return failureResponse(parseJson(MISSING_API_KEY_PAYLOAD, { error: { message: "Missing API key" } }));
      }

      const operation = operationType(normalized.query);
      const cacheable = operation === "query" && CACHE_TTL_MS > 0;
      const key = cacheable ? stableStringify([normalized.query, normalized.variables]) : null;

      if (cacheable) {
        const entry = cache.get(key);

        if (entry && entry.expiresAt > Date.now()) {
          log(`cache=hit op=${operation} age_ms=${Date.now() - entry.storedAt}`);
          return successResponse(entry.payload);
        }

        cache.delete(key);
      }

      if (operation !== "query") {
        // Anything that may write invalidates every cached read.
        cache.clear();
      }

      const outcome = await requestWithBackoff(normalized, operation);

      if (outcome.ok && cacheable) {
        const now = Date.now();
        cache.set(key, { payload: outcome.payload, storedAt: now, expiresAt: now + CACHE_TTL_MS });
      }

      return outcome.ok ? successResponse(outcome.payload) : failureResponse(outcome.payload);
    }

    async function requestWithBackoff(normalized, operation) {
      for (let attempt = 0; ; attempt += 1) {
        let response;
        let payload;

        try {
          response = await fetch(ENDPOINT, {
            method: "POST",
            headers: {
              Authorization: API_KEY,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              query: normalized.query,
              variables: normalized.variables,
            }),
          });

          payload = await response.json();
        } catch (error) {
          return {
            ok: false,
            payload: {
              error: {
                message: TRANSPORT_FAILURE_MESSAGE,
                reason: error instanceof Error ? error.message : String(error),
              },
            },
          };
        }

        const budget = readBudget(response);
        const limited = rateLimited(response, payload);
        const resetIn = budget.resetAt === null ? "unknown" : Math.max(0, budget.resetAt - Date.now());

        log(
          `budget remaining=${budget.remaining} limit=${budget.limit} reset_in_ms=${resetIn} ` +
            `status=${response.status} op=${operation} attempt=${attempt + 1} ratelimited=${limited}`,
        );

        if (limited && attempt < MAX_RETRIES) {
          const waitMs = backoffMs(budget.resetAt, attempt);

          if (waitMs === null) {
            log(`ratelimited reset_beyond_cap reset_in_ms=${resetIn} max_backoff_ms=${MAX_BACKOFF_MS}`);
          } else {
            log(`ratelimited retry=${attempt + 1}/${MAX_RETRIES} wait_ms=${waitMs}`);
            await sleep(waitMs);
            continue;
          }
        }

        // Only a rate-limit rejection is retried: an auth, validation or
        // server error replayed after a wait just spends more of the budget.
        const rateLimit = limited
          ? { attempts: attempt + 1, remaining: budget.remaining, limit: budget.limit, resetAt: budget.resetAt }
          : null;

        if (!response.ok) {
          return {
            ok: false,
            payload: {
              error: {
                message: `${HTTP_FAILURE_PREFIX}${response.status}.`,
                status: response.status,
                body: payload,
                ...(rateLimit ? { rateLimit } : {}),
              },
            },
          };
        }

        if (Array.isArray(payload?.errors) && payload.errors.length > 0) {
          return { ok: false, payload: rateLimit ? { ...payload, rateLimit } : payload };
        }

        return { ok: true, payload };
      }
    }

    async function handleMessage(message) {
      const id = message?.id ?? null;
      const method = message?.method;

      if (method === "initialize") {
        const protocolVersion =
          typeof message?.params?.protocolVersion === "string" && message.params.protocolVersion !== ""
            ? message.params.protocolVersion
            : "2024-11-05";

        sendResult(id, {
          protocolVersion,
          capabilities: { tools: {} },
          serverInfo: {
            name: "symphony-linear-graphql",
            version: "0.1.0",
          },
        });
        return;
      }

      if (method === "notifications/initialized") {
        return;
      }

      if (method === "tools/list") {
        sendResult(id, {
          tools: [
            {
              name: TOOL_NAME,
              description: DESCRIPTION,
              inputSchema: INPUT_SCHEMA,
            },
          ],
        });
        return;
      }

      if (method === "tools/call") {
        const name = message?.params?.name;

        if (name !== TOOL_NAME) {
          sendResult(id, failureResponse(parseJson(UNSUPPORTED_PAYLOAD, { error: { message: "Unsupported tool" } })));
          return;
        }

        const response = await executeTool(message?.params?.arguments);
        sendResult(id, response);
        return;
      }

      if (id !== null) {
        sendError(id, -32601, "Method not found", { method });
      }
    }

    function readMessages() {
      while (true) {
        const headerEnd = buffer.indexOf("\\r\\n\\r\\n");

        if (headerEnd === -1) {
          return;
        }

        const header = buffer.slice(0, headerEnd);
        const contentLengthLine = header
          .split("\\r\\n")
          .find((line) => line.toLowerCase().startsWith("content-length:"));

        if (!contentLengthLine) {
          buffer = "";
          return;
        }

        const contentLength = Number(contentLengthLine.split(":")[1]?.trim() || "");

        if (!Number.isFinite(contentLength) || contentLength < 0) {
          buffer = "";
          return;
        }

        const bodyStart = headerEnd + 4;

        if (buffer.length < bodyStart + contentLength) {
          return;
        }

        const body = buffer.slice(bodyStart, bodyStart + contentLength);
        buffer = buffer.slice(bodyStart + contentLength);

        let message;

        try {
          message = JSON.parse(body);
        } catch (error) {
          sendError(null, -32700, "Parse error", { reason: String(error) });
          continue;
        }

        Promise.resolve(handleMessage(message)).catch((error) => {
          if (message?.id !== null && message?.id !== undefined) {
            sendError(message.id, -32603, "Internal error", { reason: String(error) });
          }
        });
      }
    }

    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => {
      buffer += chunk;
      readMessages();
    });
    process.stdin.on("end", () => process.exit(0));
    process.stdin.resume();
    """
  end

  @spec tool_error_payload(term()) :: map()
  def tool_error_payload(:missing_query) do
    %{
      "error" => %{
        "message" => "`linear_graphql` requires a non-empty `query` string."
      }
    }
  end

  def tool_error_payload(:invalid_arguments) do
    %{
      "error" => %{
        "message" =>
          "`linear_graphql` expects either a GraphQL query string or an object with `query` and optional `variables`."
      }
    }
  end

  def tool_error_payload(:invalid_variables) do
    %{
      "error" => %{
        "message" => "`linear_graphql.variables` must be a JSON object when provided."
      }
    }
  end

  def tool_error_payload(:missing_linear_api_token) do
    %{
      "error" => %{
        "message" =>
          "Symphony is missing Linear auth. Set `tracker.api_key` in `symphony.yml` or export `LINEAR_API_KEY`."
      }
    }
  end

  def tool_error_payload({:linear_api_status, status}) do
    %{
      "error" => %{
        "message" => "#{http_failure_message_prefix()}#{status}.",
        "status" => status
      }
    }
  end

  def tool_error_payload({:linear_api_request, reason}) do
    %{
      "error" => %{
        "message" => transport_failure_message(),
        "reason" => inspect(reason)
      }
    }
  end

  def tool_error_payload({:unsupported_tool, supported_tools}) when is_list(supported_tools) do
    %{
      "error" => %{
        "message" => "Unsupported dynamic tool.",
        "supportedTools" => supported_tools
      }
    }
  end

  def tool_error_payload(reason) do
    %{
      "error" => %{
        "message" => "Linear GraphQL tool execution failed.",
        "reason" => inspect(reason)
      }
    }
  end

  @spec normalize_arguments(term()) :: {:ok, String.t(), map()} | {:error, term()}
  def normalize_arguments(arguments) when is_binary(arguments) do
    case String.trim(arguments) do
      "" -> {:error, :missing_query}
      query -> {:ok, query, %{}}
    end
  end

  def normalize_arguments(arguments) when is_map(arguments) do
    case normalize_query(arguments) do
      {:ok, query} ->
        case normalize_variables(arguments) do
          {:ok, variables} ->
            {:ok, query, variables}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def normalize_arguments(_arguments), do: {:error, :invalid_arguments}

  defp normalize_query(arguments) do
    case Map.get(arguments, "query") || Map.get(arguments, :query) do
      query when is_binary(query) ->
        case String.trim(query) do
          "" -> {:error, :missing_query}
          trimmed -> {:ok, trimmed}
        end

      _ ->
        {:error, :missing_query}
    end
  end

  defp normalize_variables(arguments) do
    case Map.get(arguments, "variables") || Map.get(arguments, :variables) || %{} do
      nil -> {:ok, %{}}
      variables when is_map(variables) -> {:ok, variables}
      _ -> {:error, :invalid_variables}
    end
  end

  defp graphql_response(response) do
    success =
      case response do
        %{"errors" => errors} when is_list(errors) and errors != [] -> false
        %{errors: errors} when is_list(errors) and errors != [] -> false
        _ -> true
      end

    dynamic_tool_response(success, encode_payload(response))
  end

  defp failure_response(payload) do
    dynamic_tool_response(false, encode_payload(payload))
  end

  defp dynamic_tool_response(success, output) when is_boolean(success) and is_binary(output) do
    %{
      "success" => success,
      "output" => output,
      "contentItems" => [
        %{
          "type" => "inputText",
          "text" => output
        }
      ]
    }
  end

  defp encode_payload(payload) when is_map(payload) or is_list(payload) do
    Jason.encode!(payload, pretty: true)
  end

  defp encode_payload(payload), do: inspect(payload)

  defp transport_failure_message do
    "Linear GraphQL request failed before receiving a successful response."
  end

  defp http_failure_message_prefix do
    "Linear GraphQL request failed with HTTP "
  end
end
