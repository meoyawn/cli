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

The server implements the experimental [MCP Events draft](https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/main/docs/design-sketch-proposal.md)
with native `events/list` and `events/poll`. The event `github.workflow_run`
advertises **poll** delivery. The draft allows any nonempty subset of delivery
modes. Push and webhook delivery are not advertised. Discovery uses MCP
`server/discover`, protocol version `2026-07-28`; this command does not implement
legacy `initialize`. A read-only `list_workflow_runs` MCP tool exposes current
run snapshots for context. There are no `wait_for_event` or `poll_events` tools.

Optional subscription arguments `branch`, `workflow`, `status`, and `conclusion`
are exact-match filters. Omitted arguments watch every observed change in the
selected repository. A null or omitted cursor bootstraps from now, returning
no historical events. A client retains the response-level cursor and polls
again after `nextPollMs` (default 15000); `hasMore` drains a partial batch
immediately. `--interval` controls the minimum time between shared API reads.
Concurrent subscriptions reuse the same observation cache.

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
