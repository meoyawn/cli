package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
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
	if id != nil {
		if params == nil {
			params = map[string]any{}
		}
		if fields, ok := params.(map[string]any); ok {
			copy := make(map[string]any, len(fields)+1)
			maps.Copy(copy, fields)
			if _, exists := copy["_meta"]; !exists {
				copy["_meta"] = map[string]any{
					"io.modelcontextprotocol/protocolVersion":    "2026-07-28",
					"io.modelcontextprotocol/clientCapabilities": map[string]any{},
				}
			}
			params = copy
		}
	}
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
	result := frame["result"].(map[string]any)
	require.Equal(t, "complete", result["resultType"])
	return result
}

func isolatedConfig(t *testing.T) {
	t.Helper()
	t.Setenv("GH_CONFIG_DIR", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
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
	capabilities := discovery["capabilities"].(map[string]any)
	require.NotContains(t, capabilities, "events", "SEP-3415 replaces the prototype capability")
	require.Contains(t, capabilities, "extensions")
	require.Equal(t, map[string]any{}, capabilities["extensions"].(map[string]any)["io.modelcontextprotocol/events"])
	require.Equal(t, map[string]any{"name": "gh-mcp-events", "version": "0.1.0"}, discovery["_meta"].(map[string]any)["io.modelcontextprotocol/serverInfo"])
	catalog := client.call(t, "events/list", map[string]any{})["events"].([]any)
	require.Equal(t, []any{"poll"}, catalog[0].(map[string]any)["delivery"])
	name := catalog[0].(map[string]any)["name"]
	poll := func(arguments map[string]any, cursor any, limit int) map[string]any {
		return client.call(t, "events/poll", map[string]any{"name": name, "arguments": arguments, "cursor": cursor, "maxEvents": limit})
	}
	first := poll(map[string]any{}, nil, 101)
	require.Empty(t, first["events"])
	require.Equal(t, false, first["truncated"])
	filtered := poll(map[string]any{"branch": "main"}, nil, 100)
	emptyBranch := poll(map[string]any{"branch": ""}, nil, 100)
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
	filteredPage := poll(map[string]any{"branch": "main"}, filtered["cursor"], 1)
	require.Len(t, filteredPage["events"], 1)
	require.Equal(t, false, filteredPage["hasMore"], "nonmatching observations must not create another page")
	require.Empty(t, poll(map[string]any{"branch": ""}, emptyBranch["cursor"], 100)["events"], "an explicit empty filter must match exactly")
	wrongFilter := poll(map[string]any{"branch": "topic"}, filtered["cursor"], 100)
	require.Equal(t, true, wrongFilter["truncated"])
	require.Empty(t, wrongFilter["events"])
	otherRegistry := &httpmock.Registry{}
	otherRegistry.Register(actionsRequest(t), httpmock.JSONResponse(map[string]any{"workflow_runs": []any{
		run(1, "main", "completed"), run(2, "topic", "completed"), run(3, "main", "completed"),
	}}))
	restarted := startPeer(t, otherRegistry)
	gap := restarted.call(t, "events/poll", map[string]any{"name": name, "cursor": last["cursor"]})
	require.Equal(t, false, gap["truncated"], "a process restart preserves the saved observation cursor")
	require.Len(t, gap["events"], 1)
	require.Equal(t, float64(3), gap["events"].([]any)[0].(map[string]any)["data"].(map[string]any)["run_id"])
	retained := restarted.call(t, "events/poll", map[string]any{"name": name, "cursor": first["cursor"]})
	require.Len(t, retained["events"], 3, "old observations and the offline change remain replayable")
	require.Equal(t, events[0], retained["events"].([]any)[0], "event IDs survive a server restart")
}

func TestPerRequestProtocolMetadata(t *testing.T) {
	isolatedConfig(t)
	registry := &httpmock.Registry{}
	registry.Register(actionsRequest(t), httpmock.JSONResponse(map[string]any{"workflow_runs": []any{}}))
	client := startPeer(t, registry)
	for _, test := range []struct {
		name   string
		params any
		code   float64
	}{
		{"missing params", nil, -32602},
		{"null params", json.RawMessage(`null`), -32602},
		{"non-object params", []any{}, -32602},
		{"missing metadata", map[string]any{}, -32602},
		{"null metadata", map[string]any{"_meta": nil}, -32602},
		{"missing version", map[string]any{"_meta": map[string]any{"io.modelcontextprotocol/clientCapabilities": map[string]any{}}}, -32602},
		{"missing capabilities", map[string]any{"_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": "2026-07-28"}}, -32602},
		{"null capabilities", map[string]any{"_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": nil}}, -32602},
		{"non-object capabilities", map[string]any{"_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": "2026-07-28", "io.modelcontextprotocol/clientCapabilities": []any{}}}, -32602},
		{"legacy version", map[string]any{"_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": "2025-11-25", "io.modelcontextprotocol/clientCapabilities": map[string]any{}}}, -32022},
		{"future version", map[string]any{"_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": "2099-01-01", "io.modelcontextprotocol/clientCapabilities": map[string]any{}}}, -32022},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := map[string]any{"jsonrpc": "2.0", "id": "metadata", "method": "server/discover"}
			if test.params != nil {
				request["params"] = test.params
			}
			require.NoError(t, json.NewEncoder(client.input).Encode(request))
			frame := client.read(t)
			require.Equal(t, "metadata", frame["id"])
			require.NotContains(t, frame, "result")
			failure, ok := frame["error"].(map[string]any)
			require.True(t, ok, "invalid request metadata must be rejected: %v", frame)
			require.Equal(t, test.code, failure["code"])
			if test.code == -32022 {
				data := failure["data"].(map[string]any)
				require.Equal(t, []any{"2026-07-28"}, data["supported"])
				require.Equal(t, test.params.(map[string]any)["_meta"].(map[string]any)["io.modelcontextprotocol/protocolVersion"], data["requested"])
			}
		})
	}
	client.call(t, "ping", nil)
	tools := client.call(t, "tools/list", nil)["tools"].([]any)
	require.Len(t, tools, 1)
	result := client.call(t, "tools/call", map[string]any{"name": "list_workflow_runs"})
	require.Equal(t, map[string]any{"repository": "octocat/hello", "workflow_runs": []any{}}, result["structuredContent"])
}

func TestNullRequestIDIsRejected(t *testing.T) {
	isolatedConfig(t)
	client := startPeer(t, &httpmock.Registry{})
	client.send(t, json.RawMessage(`null`), "ping", nil)
	frame := client.read(t)
	require.NotContains(t, frame, "result")
	require.Nil(t, frame["id"])
	require.Equal(t, float64(-32600), frame["error"].(map[string]any)["code"])
	client.send(t, "null", "ping", nil)
	frame = client.read(t)
	require.Equal(t, "null", frame["id"])
	require.Equal(t, "complete", frame["result"].(map[string]any)["resultType"])
}

func TestReplayAgeAndCursorRecovery(t *testing.T) {
	isolatedConfig(t)
	registry := &httpmock.Registry{}
	registry.Register(actionsRequest(t), httpmock.JSONResponse(map[string]any{"workflow_runs": []any{}}))
	oldRun, recentRun := run(1, "main", "completed"), run(2, "main", "completed")
	oldRun["updated_at"] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	recentRun["updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	registry.Register(actionsRequest(t), httpmock.JSONResponse(map[string]any{"workflow_runs": []any{oldRun, recentRun}}))
	client := startPeer(t, registry)
	first := client.call(t, "events/poll", map[string]any{"name": "github.workflow_run", "maxAgeMs": 0})
	time.Sleep(1020 * time.Millisecond)
	page := client.call(t, "events/poll", map[string]any{"name": "github.workflow_run", "cursor": first["cursor"], "maxAgeMs": 60000})
	require.Equal(t, true, page["truncated"])
	events := page["events"].([]any)
	require.Len(t, events, 1)
	require.Equal(t, float64(2), events[0].(map[string]any)["data"].(map[string]any)["run_id"])
	require.Equal(t, false, page["hasMore"])
	replay := client.call(t, "events/poll", map[string]any{"name": "github.workflow_run", "cursor": first["cursor"]})
	require.Len(t, replay["events"], 2, "replay bounds belong to each request")
	require.Equal(t, false, replay["truncated"])
	gap := client.call(t, "events/poll", map[string]any{"name": "github.workflow_run", "cursor": "invalid!"})
	require.Equal(t, true, gap["truncated"])
	require.Empty(t, gap["events"])
	require.NotEmpty(t, gap["cursor"])
	resumed := client.call(t, "events/poll", map[string]any{"name": "github.workflow_run", "cursor": gap["cursor"]})
	require.Equal(t, false, resumed["truncated"])
	require.Empty(t, resumed["events"])
}

func TestSubscriptionsSelectIndependentRepositoriesAndResumeAfterRestart(t *testing.T) {
	isolatedConfig(t)
	repositories := []struct {
		selector, name, host, path string
	}{
		{"https://github.com/listenbox/client", "listenbox/client", "api.github.com", "repos/listenbox/client/actions/runs"},
		{"github.com/listenbox/listenbox2", "listenbox/listenbox2", "api.github.com", "repos/listenbox/listenbox2/actions/runs"},
		{"listenbox/client", "listenbox/client", "github.example", "api/v3/repos/listenbox/client/actions/runs"},
	}
	registry := func(status string) *httpmock.Registry {
		value := &httpmock.Registry{}
		for _, repository := range repositories {
			value.Register(httpmock.WithHost(httpmock.REST(http.MethodGet, repository.path), repository.host),
				httpmock.JSONResponse(map[string]any{"workflow_runs": []any{run(1, "main", status)}}))
		}
		return value
	}
	client := startPeer(t, registry("in_progress"))
	cursors := make([]any, len(repositories))
	for index, repository := range repositories {
		page := client.call(t, "events/poll", map[string]any{
			"name": "github.workflow_run", "arguments": map[string]any{"repository": repository.selector},
		})
		require.Empty(t, page["events"])
		require.Equal(t, false, page["truncated"])
		cursors[index] = page["cursor"]
		snapshot := client.call(t, "tools/call", map[string]any{
			"name": "list_workflow_runs", "arguments": map[string]any{"repository": repository.selector},
		})["structuredContent"].(map[string]any)
		require.Equal(t, repository.name, snapshot["repository"])
		require.Equal(t, "in_progress", snapshot["workflow_runs"].([]any)[0].(map[string]any)["status"])
	}
	restarted := startPeer(t, registry("completed"))
	ids := map[any]bool{}
	for index, repository := range repositories {
		page := restarted.call(t, "events/poll", map[string]any{
			"name": "github.workflow_run", "arguments": map[string]any{"repository": repository.selector}, "cursor": cursors[index],
		})
		require.Equal(t, false, page["truncated"], "each repository must resume its own saved cursor")
		events := page["events"].([]any)
		require.Len(t, events, 1)
		event := events[0].(map[string]any)
		require.Equal(t, repository.name, event["data"].(map[string]any)["repository"])
		require.Equal(t, "completed", event["data"].(map[string]any)["status"])
		require.False(t, ids[event["eventId"]], "identical run IDs on different repositories and hosts must not collide")
		ids[event["eventId"]] = true
		wrong := restarted.call(t, "events/poll", map[string]any{
			"name": "github.workflow_run", "arguments": map[string]any{"repository": repository.selector},
			"cursor": cursors[(index+1)%len(repositories)],
		})
		require.Equal(t, true, wrong["truncated"])
		require.Empty(t, wrong["events"], "another repository's cursor must not replay this repository's history")
	}
}

func TestRepositoryAccessFailureDoesNotDisableOtherSubscriptions(t *testing.T) {
	isolatedConfig(t)
	registry := &httpmock.Registry{}
	registry.Register(httpmock.WithHost(httpmock.REST(http.MethodGet, "repos/listenbox/missing/actions/runs"), "api.github.com"),
		httpmock.StatusJSONResponse(404, map[string]any{"message": "Not Found"}))
	registry.Register(actionsRequest(t), httpmock.JSONResponse(map[string]any{"workflow_runs": []any{}}))
	client := startPeer(t, registry)
	client.send(t, "request", "events/poll", map[string]any{
		"name": "github.workflow_run", "arguments": map[string]any{"repository": "github.com/listenbox/missing"},
	})
	require.Equal(t, float64(-32024), client.read(t)["error"].(map[string]any)["code"])
	require.Empty(t, client.call(t, "events/poll", map[string]any{"name": "github.workflow_run"})["events"])
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
	registry.Register(httpmock.WithHost(httpmock.REST(http.MethodGet, "repos/listenbox/client/actions/runs"), "api.github.com"),
		httpmock.JSONResponse(map[string]any{"workflow_runs": []any{}}))
	client := startPeer(t, registry)
	client.send(t, 1, "events/poll", map[string]any{"name": "github.workflow_run"})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("GitHub request did not start")
	}
	require.Empty(t, client.call(t, "events/poll", map[string]any{
		"name": "github.workflow_run", "arguments": map[string]any{"repository": "https://github.com/listenbox/client"},
	})["events"], "a blocked repository must not block another repository's subscription")
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
		{"events/poll", map[string]any{}, -32602},
		{"events/poll", map[string]any{"name": nil}, -32602},
		{"events/poll", map[string]any{"name": "github.workflow_run", "maxEvents": 0}, -32602},
		{"events/poll", map[string]any{"name": "github.workflow_run", "maxEvents": nil}, -32602},
		{"events/poll", map[string]any{"name": "github.workflow_run", "maxAgeMs": -1}, -32602},
		{"events/poll", map[string]any{"name": "github.workflow_run", "maxAgeMs": nil}, -32602},
		{"events/poll", map[string]any{"name": "github.workflow_run", "arguments": map[string]any{"typo": true}}, -32602},
		{"events/poll", map[string]any{"name": "github.workflow_run", "arguments": map[string]any{"branch": nil}}, -32602},
		{"events/poll", map[string]any{"name": "github.workflow_run", "arguments": map[string]any{"branch": 1}}, -32602},
		{"tools/call", map[string]any{"name": "list_workflow_runs", "arguments": nil}, -32602},
		{"events/poll", map[string]any{"name": "github.workflow_run", "arguments": map[string]any{"repository": "owner-without-repository"}}, -32602},
		{"events/poll", map[string]any{"name": "github.workflow_run", "arguments": map[string]any{"repository": 1}}, -32602},
		{"tools/call", map[string]any{"name": "list_workflow_runs", "arguments": map[string]any{"repository": 1}}, -32602},
		{"tools/call", map[string]any{"name": "list_workflow_runs", "arguments": map[string]any{"repository": "owner-without-repository"}}, -32602},
		{"tools/call", map[string]any{"name": "list_workflow_runs", "arguments": map[string]any{"branch": "main"}}, -32602},
		{"events/stream", map[string]any{"name": "github.workflow_run"}, -32026},
		{"events/poll", map[string]any{"name": "github.workflow_run"}, -32025},
		{"events/poll", map[string]any{"name": "github.workflow_run"}, -32024},
	} {
		if test.code == -32024 {
			time.Sleep(1020 * time.Millisecond)
		}
		client.send(t, "request", test.method, test.params)
		frame := client.read(t)
		require.Equal(t, test.code, frame["error"].(map[string]any)["code"])
	}
	require.NotEmpty(t, client.call(t, "events/list", map[string]any{})["events"], "a bad request must not kill the server")
}

func TestGitHubRateLimitCooldownIsSharedByPollers(t *testing.T) {
	isolatedConfig(t)
	registry := &httpmock.Registry{}
	registry.Register(actionsRequest(t), func(request *http.Request) (*http.Response, error) {
		response, err := httpmock.StatusJSONResponse(429, map[string]any{"message": "rate limited"})(request)
		if err != nil {
			return nil, err
		}
		response.Header.Set("Retry-After", "1")
		return response, nil
	})
	registry.Register(actionsRequest(t), httpmock.JSONResponse(map[string]any{"workflow_runs": []any{}}))
	client := startPeer(t, registry)
	for index := range 2 {
		client.send(t, "request", "events/poll", map[string]any{"name": "github.workflow_run", "arguments": map[string]any{"branch": fmt.Sprint(index)}})
		frame := client.read(t)
		failure, ok := frame["error"].(map[string]any)
		require.True(t, ok, "a second poll must retain the upstream cooldown: %v", frame)
		require.Equal(t, float64(-32025), failure["code"])
		require.Greater(t, failure["data"].(map[string]any)["retryAfterMs"].(float64), float64(0))
	}
	time.Sleep(1020 * time.Millisecond)
	require.Empty(t, client.call(t, "events/poll", map[string]any{"name": "github.workflow_run"})["events"])
}

func TestGitHubRateLimitResetAdvice(t *testing.T) {
	for _, test := range []struct {
		name, header string
		value        func() string
	}{
		{"seconds", "Retry-After", func() string { return "5" }},
		{"http_date", "Retry-After", func() string { return time.Now().Add(5 * time.Second).UTC().Format(http.TimeFormat) }},
		{"reset", "X-RateLimit-Reset", func() string { return fmt.Sprint(time.Now().Add(5 * time.Second).Unix()) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			isolatedConfig(t)
			registry := &httpmock.Registry{}
			registry.Register(actionsRequest(t), func(request *http.Request) (*http.Response, error) {
				response, err := httpmock.StatusJSONResponse(429, map[string]any{"message": "rate limited"})(request)
				if err != nil {
					return nil, err
				}
				response.Header.Set(test.header, test.value())
				return response, nil
			})
			client := startPeer(t, registry)
			client.send(t, "request", "events/poll", map[string]any{"name": "github.workflow_run"})
			failure := client.read(t)["error"].(map[string]any)
			require.Equal(t, float64(-32025), failure["code"])
			delay, ok := failure["data"].(map[string]any)["retryAfterMs"].(float64)
			require.True(t, ok, "the server must forward GitHub retry advice")
			require.Greater(t, delay, float64(3000))
			require.LessOrEqual(t, delay, float64(5000))
		})
	}
}

func TestCheckpointFailureRetainsReplayPosition(t *testing.T) {
	isolatedConfig(t)
	registry := &httpmock.Registry{}
	registry.Register(actionsRequest(t), httpmock.JSONResponse(map[string]any{"workflow_runs": []any{run(1, "main", "in_progress")}}))
	for range 2 {
		registry.Register(actionsRequest(t), httpmock.JSONResponse(map[string]any{"workflow_runs": []any{run(1, "main", "completed")}}))
	}
	client := startPeer(t, registry)
	first := client.call(t, "events/poll", map[string]any{"name": "github.workflow_run"})
	directory := filepath.Join(os.Getenv("XDG_CACHE_HOME"), "gh", "mcp-events")
	require.NoError(t, os.Rename(directory, directory+".held"))
	require.NoError(t, os.WriteFile(directory, []byte("blocked checkpoint directory"), 0600))
	time.Sleep(1020 * time.Millisecond)
	client.send(t, "request", "events/poll", map[string]any{"name": "github.workflow_run", "cursor": first["cursor"]})
	failure := client.read(t)
	require.Equal(t, float64(-32603), failure["error"].(map[string]any)["code"])
	require.NotContains(t, failure, "result", "uncommitted observations must not advance the cursor")
	require.NoError(t, os.Remove(directory))
	require.NoError(t, os.Rename(directory+".held", directory))
	time.Sleep(1020 * time.Millisecond)
	page := client.call(t, "events/poll", map[string]any{"name": "github.workflow_run", "cursor": first["cursor"]})
	require.Equal(t, false, page["truncated"])
	events := page["events"].([]any)
	require.Len(t, events, 1)
	require.Equal(t, "completed", events[0].(map[string]any)["data"].(map[string]any)["status"])
	require.Empty(t, client.call(t, "events/poll", map[string]any{"name": "github.workflow_run", "cursor": page["cursor"]})["events"])
}
