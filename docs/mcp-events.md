# Native MCP Events for GitHub Actions

This fork adds a read-only, stdio MCP server using the current `gh` login.
Run the executable built from this fork in your project's directory:

```sh
gh mcp serve
```

Repository selection reuses the same resolver as `gh browse`, including
`gh repo set-default`, Git remotes, and GitHub Enterprise hosts. No repository
override is needed. The configured HTTP client supplies authentication and API
gateway routing. No webhook endpoint,
Hookdeck account, additional token, or GitHub repository mutation is required.

The server implements [SEP-3415: Events Extension](https://github.com/modelcontextprotocol/modelcontextprotocol/pull/3415)
with native `events/list` and `events/poll`. The event `github.workflow_run`
advertises **poll** delivery. The extension allows any nonempty subset of delivery
modes. Push and webhook delivery are not advertised. Discovery uses MCP
`server/discover`, protocol version `2026-07-28`; this command does not implement
legacy `initialize`. Discovery declares event support under
`capabilities.extensions["io.modelcontextprotocol/events"]`, with no dynamic
catalog notifications. Every successful result includes `resultType: "complete"`
and identifies the server in `_meta["io.modelcontextprotocol/serverInfo"]`.
A read-only `list_workflow_runs` MCP tool exposes current
run snapshots for context. There are no `wait_for_event` or `poll_events` tools.

Every request, including `server/discover`, must include the standard per-request
metadata in `params`:

```json
{
  "_meta": {
    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    "io.modelcontextprotocol/clientCapabilities": {}
  }
}
```

The client may declare `io.modelcontextprotocol/events` in its capability
`extensions` map. This declaration is optional for client-initiated event
requests. Missing required metadata returns `InvalidParams` (`-32602`). An
unsupported version returns `UnsupportedProtocolVersion` (`-32022`) with the
requested and supported versions.

The optional `repository` subscription argument selects any accessible repository
as `OWNER/REPO`, `HOST/OWNER/REPO`, or a full repository URL. Omitting it uses the
server's selected repository, including `-R` and `GH_REPO`. An `OWNER/REPO` value
uses the server's selected host. The `list_workflow_runs` tool accepts the same
argument. One server can watch multiple repositories and GitHub Enterprise hosts:

```json
[
  {"repository": "github.com/listenbox/client", "branch": "master", "status": "completed"},
  {"repository": "github.com/listenbox/listenbox2", "branch": "master", "status": "completed"}
]
```

Optional subscription arguments `branch`, `workflow`, `status`, and `conclusion`
are exact-match string filters, including an explicitly empty string. Null values
are invalid. Omitted filters watch every observed change in the
selected repository. A null or omitted cursor bootstraps from now, returning
no historical events. A client retains the response-level cursor and polls
again after `nextPollMs` (default 15000); `hasMore` drains a partial batch
immediately. `maxEvents` caps each batch, with a default of 50 and a server ceiling
of 100; larger requested caps are accepted. `maxAgeMs` optionally bounds replay
by age and is ignored when starting from now. `--interval` controls the minimum
time between shared API reads.
Concurrent subscriptions for the same repository reuse its observation cache.
Repositories have independent API requests, cooldowns, checkpoints and cursors;
a stalled or inaccessible repository does not block another repository's watch.

The source samples the latest 100 Actions runs. Intermediate transitions may
coalesce between reads, and a busy repository can have runs outside that
window. This is an observed-state feed, not GitHub's durable webhook history.
The server retains 512 observed changes with stable event IDs for deduplication.
Observations and their cursor generation are atomically checkpointed in the gh
cache directory, under `mcp-events`, before they are returned to a client.
Restarting `go run` resumes that history and compares the latest API snapshot
with the saved runs, catching changes observed after the outage. An expired
buffer position, mismatched filter, missing checkpoint, or replay age limit
reports `truncated: true`. Cancelled JSON-RPC requests cancel their GitHub API
requests; stdin EOF or process termination releases the whole server.

Upstream failures put the shared source into a cooldown, so parallel subscribers
cannot repeatedly hit GitHub while it is unavailable. Errors carry
`retryAfterMs`; GitHub's `Retry-After` (seconds or HTTP date) and rate-limit reset
headers determine the minimum retry delay. Clients should retry transient errors
with bounded backoff instead of discarding a saved watch.

A native Events client such as Happen owns the polling loop between agent
turns. While Happen's server is running, a subscription owned by a saved thread
queues each occurrence and wakes that thread when idle. Merely launching this
command in a client without native Events support does not provide that loop.

Example project-local `.happen/config.yaml`:

```yaml
mcp_servers:
  github_events:
    command: go
    args: [run, github.com/cli/cli/v2/cmd/gh, mcp, serve]
    cwd: /absolute/path/to/project
    env:
      GOWORK: /absolute/path/to/project/.happen/go.work
    legacy_fallback: false
```

For this source-run configuration, create `.happen/go.work` in the project:

```go
go 1.27.0

use /absolute/path/to/gh-cli
```

The Go workspace selects the local fork's source while preserving the project's
working directory, so `gh repo set-default` is honored. With a built fork
executable instead, use its absolute path as `command` and `args: [mcp, serve]`;
the Go workspace is then unnecessary.
