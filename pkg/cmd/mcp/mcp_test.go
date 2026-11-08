package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/cli/cli/v2/internal/ghrepo"
	"github.com/cli/cli/v2/pkg/cmdutil"
	"github.com/cli/cli/v2/pkg/httpmock"
	"github.com/cli/cli/v2/pkg/iostreams"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type peer struct {
	input  *os.File
	output *os.File
	lines  *bufio.Scanner
}

func startPeer(t *testing.T, registry *httpmock.Registry) *peer {
	t.Helper()
	input, writer, err := os.Pipe()
	require.NoError(t, err)
	reader, output, err := os.Pipe()
	require.NoError(t, err)
	ios, _, _, stderr := iostreams.Test()
	ios.In, ios.Out = input, output
	factory := &cmdutil.Factory{
		IOStreams:  ios,
		BaseRepo:   func() (ghrepo.Interface, error) { return nil, fmt.Errorf("repository override was ignored") },
		HttpClient: func() (*http.Client, error) { return &http.Client{Transport: registry}, nil },
	}
	root := &cobra.Command{Use: "gh", SilenceUsage: true, SilenceErrors: true}
	root.AddGroup(&cobra.Group{ID: "actions", Title: "GitHub Actions"})
	root.AddCommand(NewCmdMCP(factory))
	root.SetArgs([]string{"mcp", "serve", "--repo", "github.example/octocat/hello", "--interval", "1s"})
	ctx, cancel := context.WithCancel(context.Background())
	group := &errgroup.Group{}
	group.Go(func() error { return root.ExecuteContext(ctx) })
	t.Cleanup(func() {
		cancel()
		_ = writer.Close()
		_ = reader.Close()
		require.NoError(t, group.Wait())
		require.Empty(t, stderr.String())
		registry.Verify(t)
	})
	return &peer{input: writer, output: reader, lines: bufio.NewScanner(reader)}
}

func (p *peer) send(t *testing.T, id any, method string, params any) {
	t.Helper()
	frame := map[string]any{"jsonrpc": "2.0", "method": method, "params": params}
	if id != nil {
		frame["id"] = id
	}
	require.NoError(t, json.NewEncoder(p.input).Encode(frame))
}

func (p *peer) read(t *testing.T) map[string]any {
	t.Helper()
	require.NoError(t, p.output.SetReadDeadline(time.Now().Add(5*time.Second)))
	require.True(t, p.lines.Scan(), "MCP response: %v", p.lines.Err())
	var frame map[string]any
	require.NoError(t, json.Unmarshal(p.lines.Bytes(), &frame))
	return frame
}

func (p *peer) call(t *testing.T, method string, params any) map[string]any {
	t.Helper()
	p.send(t, "request", method, params)
	frame := p.read(t)
	require.Equal(t, "request", frame["id"])
	require.NotContains(t, frame, "error")
	return frame["result"].(map[string]any)
}

func isolatedConfig(t *testing.T) {
	t.Helper()
	t.Setenv("GH_CONFIG_DIR", t.TempDir())
	for _, name := range []string{"GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN", "GH_HOST", "GH_REPO"} {
		t.Setenv(name, "")
	}
}

func actionsRequest(t *testing.T) httpmock.Matcher {
	t.Helper()
	return func(request *http.Request) bool {
		return request.Method == http.MethodGet && request.URL.Host == "github.example" && request.URL.Path == "/api/v3/repos/octocat/hello/actions/runs" && request.URL.Query().Get("per_page") == "100"
	}
}

func run(id int, branch, status string) map[string]any {
	return map[string]any{"id": id, "name": "CI", "status": status, "conclusion": "success", "head_branch": branch,
		"head_sha": "abc123", "html_url": fmt.Sprintf("https://github.example/octocat/hello/actions/runs/%d", id), "run_attempt": 1, "updated_at": "2026-11-08T12:00:00Z"}
}

func TestNativeEventsReplayAndRepositoryOverride(t *testing.T) {
	isolatedConfig(t)
	registry := &httpmock.Registry{}
	registry.Register(actionsRequest(t), httpmock.JSONResponse(map[string]any{"workflow_runs": []any{run(1, "main", "in_progress")}}))
	registry.Register(actionsRequest(t), httpmock.JSONResponse(map[string]any{"workflow_runs": []any{run(1, "main", "completed"), run(2, "topic", "completed")}}))
	client := startPeer(t, registry)
	discovery := client.call(t, "server/discover", map[string]any{})
	require.Equal(t, []any{"2026-07-28"}, discovery["supportedVersions"])
	catalog := client.call(t, "events/list", map[string]any{})["events"].([]any)
	name := catalog[0].(map[string]any)["name"]
	poll := func(arguments map[string]any, cursor any, limit int) map[string]any {
		return client.call(t, "events/poll", map[string]any{"name": name, "arguments": arguments, "cursor": cursor, "maxEvents": limit})
	}
	first := poll(map[string]any{}, nil, 1)
	require.Empty(t, first["events"])
	require.Equal(t, false, first["truncated"])
	filtered := poll(map[string]any{"branch": "main"}, nil, 100)
	// Follow the server's real poll interval, rather than replacing its clock.
	time.Sleep(1020 * time.Millisecond)
	page := poll(map[string]any{}, first["cursor"], 1)
	events := page["events"].([]any)
	require.Len(t, events, 1)
	require.Equal(t, true, page["hasMore"])
	require.Equal(t, float64(1), events[0].(map[string]any)["data"].(map[string]any)["run_id"])
	require.NotContains(t, events[0], "cursor", "poll cursors belong to the response")
	last := poll(map[string]any{}, page["cursor"], 1)
	require.Len(t, last["events"], 1)
	require.Equal(t, false, last["hasMore"])
	require.Equal(t, float64(2), last["events"].([]any)[0].(map[string]any)["data"].(map[string]any)["run_id"])
	replayed := poll(map[string]any{}, first["cursor"], 1)
	require.Equal(t, events, replayed["events"], "replay must retain stable event IDs")
	require.Empty(t, poll(map[string]any{}, last["cursor"], 100)["events"])
	require.Len(t, poll(map[string]any{"branch": "main"}, filtered["cursor"], 100)["events"], 1)
	wrongFilter := poll(map[string]any{"branch": "topic"}, filtered["cursor"], 100)
	require.Equal(t, true, wrongFilter["truncated"])
	require.Empty(t, wrongFilter["events"])
	otherRegistry := &httpmock.Registry{}
	otherRegistry.Register(actionsRequest(t), httpmock.JSONResponse(map[string]any{"workflow_runs": []any{run(1, "main", "completed")}}))
	restarted := startPeer(t, otherRegistry)
	gap := restarted.call(t, "events/poll", map[string]any{"name": name, "cursor": last["cursor"]})
	require.Equal(t, true, gap["truncated"], "a process restart must expose lost replay history")
	require.Empty(t, gap["events"])
}

func TestCancellationReleasesAPICallAndOtherRequestsContinue(t *testing.T) {
	isolatedConfig(t)
	registry := &httpmock.Registry{}
	entered, cancelled := make(chan struct{}), make(chan struct{})
	registry.Register(actionsRequest(t), func(request *http.Request) (*http.Response, error) {
		close(entered)
		<-request.Context().Done()
		close(cancelled)
		return nil, request.Context().Err()
	})
	registry.Register(actionsRequest(t), httpmock.JSONResponse(map[string]any{"workflow_runs": []any{}}))
	client := startPeer(t, registry)
	client.send(t, 1, "events/poll", map[string]any{"name": "github.workflow_run"})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("GitHub request did not start")
	}
	client.send(t, nil, "notifications/cancelled", map[string]any{"requestId": 1})
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("GitHub request was not cancelled")
	}
	require.NotEmpty(t, client.call(t, "server/discover", map[string]any{})["supportedVersions"])
	require.Empty(t, client.call(t, "events/poll", map[string]any{"name": "github.workflow_run"})["events"])
}

func TestProtocolErrorsAndUpstreamAccess(t *testing.T) {
	isolatedConfig(t)
	registry := &httpmock.Registry{}
	registry.Register(actionsRequest(t), httpmock.StatusJSONResponse(429, map[string]any{"message": "rate limited"}))
	registry.Register(actionsRequest(t), httpmock.StatusJSONResponse(403, map[string]any{"message": "forbidden"}))
	client := startPeer(t, registry)
	for _, test := range []struct {
		method string
		params any
		code   float64
	}{
		{"events/poll", map[string]any{"name": "missing"}, -32023},
		{"events/poll", map[string]any{"name": "github.workflow_run", "maxEvents": 0}, -32602},
		{"events/poll", map[string]any{"name": "github.workflow_run", "arguments": map[string]any{"typo": true}}, -32602},
		{"events/stream", map[string]any{"name": "github.workflow_run"}, -32026},
		{"events/poll", map[string]any{"name": "github.workflow_run"}, -32025},
		{"events/poll", map[string]any{"name": "github.workflow_run"}, -32024},
	} {
		client.send(t, "request", test.method, test.params)
		frame := client.read(t)
		require.Equal(t, test.code, frame["error"].(map[string]any)["code"])
	}
	require.NotEmpty(t, client.call(t, "events/list", map[string]any{})["events"], "a bad request must not kill the server")
}
