package serve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/cli/cli/v2/api"
	"github.com/cli/cli/v2/internal/ghrepo"
	"github.com/google/uuid"
)

const eventName = "github.workflow_run"

type workflowRun struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	Status     string    `json:"status"`
	Conclusion *string   `json:"conclusion"`
	Branch     string    `json:"head_branch"`
	SHA        string    `json:"head_sha"`
	URL        string    `json:"html_url"`
	Attempt    int       `json:"run_attempt"`
	Updated    time.Time `json:"updated_at"`
}

type filters struct {
	Branch     string `json:"branch,omitempty"`
	Workflow   string `json:"workflow,omitempty"`
	Status     string `json:"status,omitempty"`
	Conclusion string `json:"conclusion,omitempty"`
}

func (f *filters) UnmarshalJSON(data []byte) error {
	type fields filters
	if len(data) == 0 || data[0] != '{' {
		return fmt.Errorf("arguments must be an object")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode((*fields)(f))
}

type pollParams struct {
	Name      string  `json:"name"`
	Arguments filters `json:"arguments"`
	Cursor    *string `json:"cursor"`
	MaxAge    *int64  `json:"maxAgeMs"`
	MaxEvents *int    `json:"maxEvents"`
}

type position struct {
	Generation string `json:"g"`
	Sequence   uint64 `json:"s"`
	Filter     string `json:"f"`
}

type occurrence struct {
	EventID   string         `json:"eventId"`
	Name      string         `json:"name"`
	Timestamp time.Time      `json:"timestamp"`
	Data      map[string]any `json:"data"`
}

type bufferedEvent struct {
	sequence uint64
	run      workflowRun
	value    occurrence
}

// All observation state and checkpoint writes are owned by gate.
type eventSource struct {
	client     *api.Client
	repo       ghrepo.Interface
	interval   time.Duration
	gate       chan struct{}
	generation string
	sequence   uint64
	lastRead   time.Time
	previous   map[int64]string
	runs       []workflowRun
	buffer     []bufferedEvent
	checkpoint string
	retryAt    time.Time
}

func newSource(client *api.Client, repo ghrepo.Interface, interval time.Duration) *eventSource {
	return &eventSource{client: client, repo: repo, interval: interval,
		gate: make(chan struct{}, 1), generation: uuid.NewString()}
}

func (s *eventSource) handle(ctx context.Context, request rpcRequest) (any, *rpcError) {
	switch request.Method {
	case "server/discover":
		return map[string]any{
			"supportedVersions": []string{"2026-07-28"},
			"serverInfo":        map[string]any{"name": "gh-mcp-events", "version": "0.1.0"},
			"capabilities":      map[string]any{"events": map[string]any{}, "tools": map[string]any{}},
		}, nil
	case "ping":
		return map[string]any{}, nil
	case "events/list", "tools/list":
		var params struct {
			Cursor *string `json:"cursor"`
		}
		if failure := decodeParams(request.Params, &params); failure != nil {
			return nil, failure
		}
		if params.Cursor != nil {
			return nil, &rpcError{Code: -32602, Message: "cursor is invalid: this catalog has only one page"}
		}
		if request.Method == "events/list" {
			return map[string]any{"events": []any{eventDescriptor(s.repo)}}, nil
		}
		return map[string]any{"tools": []any{map[string]any{
			"name":        "list_workflow_runs",
			"description": "Read the latest 100 GitHub Actions runs for " + ghrepo.FullName(s.repo) + ". To watch future changes between turns, subscribe to the native github.workflow_run event through the client's MCP Events tools.",
			"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false},
			"annotations": map[string]any{"readOnlyHint": true, "openWorldHint": true},
		}}}, nil
	case "events/poll", "tools/call":
		select {
		case s.gate <- struct{}{}:
			defer func() { <-s.gate }()
		case <-ctx.Done():
			return nil, nil
		}
		if request.Method == "events/poll" {
			var params pollParams
			if failure := decodeParams(request.Params, &params); failure != nil {
				return nil, failure
			}
			if params.Name != eventName {
				return nil, &rpcError{Code: -32023, Message: "NotFound", Data: map[string]any{"kind": "event", "name": params.Name}}
			}
			if params.MaxAge != nil && *params.MaxAge < 0 {
				return nil, &rpcError{Code: -32602, Message: fmt.Sprintf("maxAgeMs must be non-negative: %d", *params.MaxAge)}
			}
			if params.MaxEvents != nil && (*params.MaxEvents < 1 || *params.MaxEvents > 100) {
				return nil, &rpcError{Code: -32602, Message: fmt.Sprintf("maxEvents must be between 1 and 100: %d", *params.MaxEvents)}
			}
			if failure := s.refresh(ctx); failure != nil {
				return nil, failure
			}
			return s.poll(params), nil
		}
		var params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if failure := decodeParams(request.Params, &params); failure != nil {
			return nil, failure
		}
		if params.Name != "list_workflow_runs" {
			return nil, &rpcError{Code: -32602, Message: fmt.Sprintf("Unknown tool name %q", params.Name)}
		}
		if len(params.Arguments) != 0 {
			return nil, &rpcError{Code: -32602, Message: "list_workflow_runs arguments must be empty"}
		}
		if failure := s.refresh(ctx); failure != nil {
			return nil, failure
		}
		data := map[string]any{"repository": ghrepo.FullName(s.repo), "workflow_runs": s.runs}
		encoded, err := json.Marshal(data)
		if err != nil {
			return nil, &rpcError{Code: -32603, Message: "Could not encode workflow runs"}
		}
		return map[string]any{"content": []any{map[string]any{"type": "text", "text": string(encoded)}}, "structuredContent": data, "isError": false}, nil
	case "events/stream", "events/subscribe", "events/unsubscribe":
		return nil, &rpcError{Code: -32026, Message: "Unsupported", Data: map[string]any{"feature": request.Method, "supported": []string{"poll"}}}
	default:
		return nil, &rpcError{Code: -32601, Message: "Method not found"}
	}
}

func (s *eventSource) refresh(ctx context.Context) *rpcError {
	if remaining := time.Until(s.retryAt); remaining > 0 {
		return &rpcError{Code: -32025, Message: "ResourceExhausted", Data: map[string]any{
			"limit": "github_api", "retryAfterMs": remaining.Milliseconds() + 1,
		}}
	}
	if !s.lastRead.IsZero() && time.Since(s.lastRead) < s.interval {
		return nil
	}
	previous := s.snapshot()
	if err := s.readRuns(ctx); err != nil {
		failure := upstreamError(err)
		if ctx.Err() == nil && failure.Code != -32024 {
			delay := s.interval.Milliseconds()
			if advised, ok := failure.Data["retryAfterMs"].(int64); ok {
				delay = max(delay, advised)
			}
			s.retryAt = time.Now().Add(time.Duration(delay) * time.Millisecond)
			if failure.Data == nil {
				failure.Data = map[string]any{}
			}
			failure.Data["retryAfterMs"] = delay
		}
		return failure
	}
	if err := s.save(); err != nil {
		s.restore(previous)
		s.lastRead = time.Time{}
		s.retryAt = time.Now().Add(s.interval)
		return &rpcError{Code: -32603, Message: "Could not checkpoint GitHub Actions observations",
			Data: map[string]any{"retryAfterMs": s.interval.Milliseconds()}}
	}
	return nil
}

func (s *eventSource) readRuns(ctx context.Context) error {
	fetch, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	path := fmt.Sprintf("repos/%s/%s/actions/runs?per_page=100", url.PathEscape(s.repo.RepoOwner()), url.PathEscape(s.repo.RepoName()))
	response, err := s.client.RequestWithContext(fetch, s.repo.RepoHost(), http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var payload struct {
		Runs []workflowRun `json:"workflow_runs"`
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 8*1024*1024+1))
	if err != nil {
		return err
	}
	if len(data) > 8*1024*1024 {
		return fmt.Errorf("workflow runs response exceeds 8 MiB")
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("decode workflow runs: %w", err)
	}
	if len(payload.Runs) > 100 {
		return fmt.Errorf("workflow runs response exceeds requested limit 100: %d", len(payload.Runs))
	}
	s.runs = payload.Runs
	// Preserve chronological order within each observed batch, with stable ties.
	runs := append([]workflowRun{}, payload.Runs...)
	sort.Slice(runs, func(i, j int) bool {
		if runs[i].Updated.Equal(runs[j].Updated) {
			return runs[i].ID < runs[j].ID
		}
		return runs[i].Updated.Before(runs[j].Updated)
	})
	next := make(map[int64]string, len(runs))
	for _, run := range runs {
		conclusion := ""
		if run.Conclusion != nil {
			conclusion = *run.Conclusion
		}
		fingerprint := fmt.Sprintf("%d:%s:%s:%s", run.Attempt, run.Status, conclusion, run.Updated.Format(time.RFC3339Nano))
		next[run.ID] = fingerprint
		if s.previous == nil || s.previous[run.ID] == fingerprint {
			continue
		}
		s.sequence++
		identity := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d/%s", s.repo.RepoHost(), ghrepo.FullName(s.repo), run.ID, fingerprint)))
		value := occurrence{EventID: fmt.Sprintf("gh_%x", identity), Name: eventName, Timestamp: run.Updated, Data: map[string]any{
			"repository": ghrepo.FullName(s.repo), "run_id": run.ID, "workflow": run.Name,
			"status": run.Status, "conclusion": run.Conclusion, "branch": run.Branch,
			"head_sha": run.SHA, "url": run.URL, "attempt": run.Attempt,
		}}
		s.buffer = append(s.buffer, bufferedEvent{sequence: s.sequence, run: run, value: value})
	}
	if len(s.buffer) > 512 {
		s.buffer = append([]bufferedEvent{}, s.buffer[len(s.buffer)-512:]...)
	}
	s.previous = next
	s.lastRead = time.Now()
	s.retryAt = time.Time{}
	return nil
}

func (s *eventSource) poll(params pollParams) map[string]any {
	arguments, _ := json.Marshal(params.Arguments)
	fingerprint := sha256.Sum256(arguments)
	filter := fmt.Sprintf("%x", fingerprint)
	current := position{Generation: s.generation, Sequence: s.sequence, Filter: filter}
	truncated := false
	if params.Cursor != nil {
		var prior position
		encoded, err := base64.RawURLEncoding.DecodeString(*params.Cursor)
		if err != nil || len(encoded) > 1024 || json.Unmarshal(encoded, &prior) != nil || prior.Generation != s.generation || prior.Filter != filter || prior.Sequence > s.sequence {
			truncated = true
		} else if len(s.buffer) > 0 && prior.Sequence < s.buffer[0].sequence-1 {
			truncated = true
		} else {
			current = prior
		}
	}
	limit := 50
	if params.MaxEvents != nil {
		limit = *params.MaxEvents
	}
	events := make([]occurrence, 0)
	for _, event := range s.buffer {
		if event.sequence <= current.Sequence {
			continue
		}
		current.Sequence = event.sequence
		if params.MaxAge != nil && time.Since(event.value.Timestamp).Milliseconds() > *params.MaxAge {
			truncated = true
			continue
		}
		run, match := event.run, params.Arguments
		if match.Branch != "" && run.Branch != match.Branch || match.Workflow != "" && run.Name != match.Workflow || match.Status != "" && run.Status != match.Status || match.Conclusion != "" && (run.Conclusion == nil || *run.Conclusion != match.Conclusion) {
			continue
		}
		events = append(events, event.value)
		if len(events) == limit {
			break
		}
	}
	encoded, _ := json.Marshal(current)
	return map[string]any{"events": events, "cursor": base64.RawURLEncoding.EncodeToString(encoded), "truncated": truncated, "hasMore": current.Sequence < s.sequence, "nextPollMs": s.interval.Milliseconds()}
}

func upstreamError(err error) *rpcError {
	if failure, ok := errors.AsType[api.HTTPError](err); ok {
		data := map[string]any{"status": failure.StatusCode}
		switch failure.StatusCode {
		case 401:
			return &rpcError{Code: -32024, Message: "Forbidden", Data: data}
		case 403, 429:
			if failure.StatusCode == 429 || failure.Headers.Get("X-RateLimit-Remaining") == "0" || failure.Headers.Get("Retry-After") != "" {
				data["limit"] = "github_api"
				if retry, parseErr := strconv.Atoi(failure.Headers.Get("Retry-After")); parseErr == nil {
					data["retryAfterMs"] = int64(retry) * 1000
				} else if retry, parseErr := http.ParseTime(failure.Headers.Get("Retry-After")); parseErr == nil {
					data["retryAfterMs"] = max(int64(0), time.Until(retry).Milliseconds())
				} else if reset, parseErr := strconv.ParseInt(failure.Headers.Get("X-RateLimit-Reset"), 10, 64); parseErr == nil {
					data["retryAfterMs"] = max(int64(0), time.Until(time.Unix(reset, 0)).Milliseconds())
				}
				return &rpcError{Code: -32025, Message: "ResourceExhausted", Data: data}
			}
			return &rpcError{Code: -32024, Message: "Forbidden", Data: data}
		case 404:
			return &rpcError{Code: -32024, Message: "Repository is missing or inaccessible", Data: data}
		}
	}
	return &rpcError{Code: -32603, Message: "GitHub Actions API read failed"}
}

func eventDescriptor(repo ghrepo.Interface) map[string]any {
	properties := map[string]any{}
	for _, name := range []string{"branch", "workflow", "status", "conclusion"} {
		properties[name] = map[string]any{"type": "string"}
	}
	payload := map[string]any{}
	for _, name := range []string{"repository", "workflow", "status", "branch", "head_sha", "url"} {
		payload[name] = map[string]any{"type": "string"}
	}
	payload["run_id"], payload["attempt"] = map[string]any{"type": "integer"}, map[string]any{"type": "integer"}
	payload["conclusion"] = map[string]any{"type": []string{"string", "null"}}
	return map[string]any{
		"name":          eventName,
		"description":   "Observed GitHub Actions run state changes in " + ghrepo.FullName(repo) + ". Filters use exact matches. Uses the current gh login. Intermediate transitions can coalesce between API reads; cursors survive only for this server process and up to 512 observed changes.",
		"delivery":      []string{"poll"},
		"inputSchema":   map[string]any{"type": "object", "properties": properties, "additionalProperties": false},
		"payloadSchema": map[string]any{"type": "object", "properties": payload, "required": []string{"repository", "run_id", "workflow", "status", "conclusion", "branch", "head_sha", "url", "attempt"}, "additionalProperties": false},
	}
}
