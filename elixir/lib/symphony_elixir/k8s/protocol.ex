defmodule SymphonyElixir.K8s.Protocol do
  @moduledoc false

  # Line-oriented, tab-delimited framing spoken over the single `kubectl run -i`
  # connection between the orchestrator (`SymphonyElixir.K8s.Broker`) and the pod's PID 1
  # protocol-reader shell (`reader_script/0`). Binary/multiline payloads are base64 so a
  # frame is always exactly one line and never collides with agent JSON output.
  #
  #   broker -> pod (written to the reader's stdin), one per line:
  #     __SYMPHONY__<TAB>RUN<TAB><base64(command)>
  #     __SYMPHONY__<TAB>AGENT_START<TAB><base64(command)>
  #     __SYMPHONY__<TAB>AGENT_IN<TAB><base64(bytes)>
  #     __SYMPHONY__<TAB>AGENT_EOF
  #
  #   pod -> broker (emitted by the reader on the pod's stdout, alongside command output
  #   and agent JSON, all of which is also what `kubectl logs` shows):
  #     __SYMPHONY__<TAB>DONE<TAB><exit_code>          (a RUN finished)
  #     __SYMPHONY__<TAB>AGENT_PID<TAB><pid>           (the agent was launched)
  #     __SYMPHONY__<TAB>AGENT_EXIT<TAB><exit_code>    (the agent exited after AGENT_EOF)

  @marker "__SYMPHONY__"

  @spec marker() :: String.t()
  def marker, do: @marker

  @spec frame(:agent_eof) :: iodata()
  def frame(:agent_eof), do: [@marker, "\t", "AGENT_EOF", "\n"]

  @spec frame(:run | :agent_start | :agent_in, iodata()) :: iodata()
  def frame(:run, command), do: control_line("RUN", command)
  def frame(:agent_start, command), do: control_line("AGENT_START", command)
  def frame(:agent_in, data), do: control_line("AGENT_IN", data)

  defp control_line(verb, payload) do
    [@marker, "\t", verb, "\t", Base.encode64(IO.iodata_to_binary(payload)), "\n"]
  end

  @doc """
  Classifies one full output line from the pod. Control replies are recognised by the
  marker prefix; everything else is command or agent output and returns `:not_control`.
  """
  @spec parse_reply(String.t()) ::
          {:done, integer()} | {:agent_pid, integer()} | {:agent_exit, integer()} | :not_control
  def parse_reply(line) when is_binary(line) do
    case String.split(line, "\t") do
      [@marker, "DONE", code] -> {:done, to_int(code)}
      [@marker, "AGENT_PID", pid] -> {:agent_pid, to_int(pid)}
      [@marker, "AGENT_EXIT", code] -> {:agent_exit, to_int(code)}
      _ -> :not_control
    end
  end

  defp to_int(value) do
    case Integer.parse(String.trim(value)) do
      {int, _rest} -> int
      :error -> 0
    end
  end

  # The literal marker below MUST equal `@marker`.
  @reader_script ~S"""
  marker='__SYMPHONY__'
  if printf '' | base64 -d >/dev/null 2>&1; then B64D='base64 -d'; else B64D='base64 -D'; fi
  agent_pid=''
  fifo=''
  while IFS=$'\t' read -r f1 f2 f3; do
    [ "$f1" = "$marker" ] || continue
    case "$f2" in
      RUN)
        cmd="$(printf '%s' "$f3" | $B64D)"
        bash -lc "$cmd"
        code=$?
        printf '%s\t%s\t%s\n' "$marker" 'DONE' "$code"
        ;;
      AGENT_START)
        cmd="$(printf '%s' "$f3" | $B64D)"
        fifo="$(mktemp -u)"
        mkfifo "$fifo"
        bash -lc "$cmd" < "$fifo" &
        agent_pid=$!
        exec 9> "$fifo"
        printf '%s\t%s\t%s\n' "$marker" 'AGENT_PID' "$agent_pid"
        ;;
      AGENT_IN)
        printf '%s' "$f3" | $B64D >&9
        ;;
      AGENT_EOF)
        exec 9>&-
        wait "$agent_pid" 2>/dev/null
        code=$?
        rm -f "$fifo"
        agent_pid=''
        printf '%s\t%s\t%s\n' "$marker" 'AGENT_EXIT' "$code"
        ;;
    esac
  done
  """

  @doc """
  The pod's PID 1 command: a bash loop that reads frames from stdin and runs everything
  as its own children so all output flows to the pod's stdout (hence `kubectl logs`). On
  stdin EOF the loop ends and PID 1 exits, which — with `kubectl run --rm` — deletes the
  pod. Requires `bash`, `mkfifo`, `mktemp`, and a `base64` that decodes with `-d` or `-D`.
  """
  @spec reader_script() :: String.t()
  def reader_script, do: @reader_script
end
