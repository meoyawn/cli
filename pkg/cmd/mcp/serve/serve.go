package serve

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/cli/cli/v2/api"
	"github.com/cli/cli/v2/internal/ghrepo"
	"github.com/cli/cli/v2/pkg/cmdutil"
	"github.com/cli/cli/v2/pkg/iostreams"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

type serveOptions struct {
	io         *iostreams.IOStreams
	httpClient func() (*http.Client, error)
	baseRepo   func() (ghrepo.Interface, error)
	interval   time.Duration
}

// NewCmdServe constructs the stdio MCP Events server command.
func NewCmdServe(f *cmdutil.Factory) *cobra.Command {
	opts := &serveOptions{io: f.IOStreams, httpClient: f.HttpClient}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve native MCP Events over stdio using the current gh login",
		Long:  "Serve GitHub Actions state changes using the experimental MCP Events draft.\nSupports events/list and events/poll. Polling continues in the MCP client between agent turns.\nOnly read-only GitHub API requests are made. Stdout contains JSON-RPC exclusively.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.baseRepo = f.BaseRepo
			if opts.interval < time.Second || opts.interval > 5*time.Minute {
				return cmdutil.FlagErrorf("invalid interval %s: expected 1s through 5m", opts.interval)
			}
			ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer cancel()
			return serveRun(ctx, opts)
		},
	}
	cmd.Flags().DurationVar(&opts.interval, "interval", 15*time.Second, "Minimum interval between GitHub API reads (1s through 5m)")
	return cmd
}

type rpcRequest struct {
	Version string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int            `json:"code"`
	Message string         `json:"message"`
	Data    map[string]any `json:"data,omitempty"`
}

func (e *rpcError) Error() string { return e.Message }

var inputClosed = errors.New("MCP input closed")

// The process owns stdio. Closing it on shutdown releases both a blocked read
// and a backpressured write, including when the parent disappears mid-request.
func serveRun(ctx context.Context, opts *serveOptions) error {
	repo, err := opts.baseRepo()
	if err != nil {
		return err
	}
	httpClient, err := opts.httpClient()
	if err != nil {
		return err
	}
	source := newSource(api.NewClientFromHTTP(httpClient), repo, opts.interval)
	group, lifetime := errgroup.WithContext(ctx)
	var output sync.Mutex
	send := func(id json.RawMessage, result any, failure *rpcError) error {
		frame := map[string]any{"jsonrpc": "2.0", "id": id}
		if failure != nil {
			frame["error"] = failure
		} else {
			frame["result"] = result
		}
		output.Lock()
		defer output.Unlock()
		if lifetime.Err() != nil {
			return lifetime.Err()
		}
		return json.NewEncoder(opts.io.Out).Encode(frame)
	}
	var requests sync.Mutex
	active := map[string]context.CancelFunc{}
	slots := make(chan struct{}, 32)
	group.Go(func() error {
		<-lifetime.Done()
		if closer, ok := opts.io.In.(io.Closer); ok {
			_ = closer.Close()
		}
		if closer, ok := opts.io.Out.(io.Closer); ok {
			_ = closer.Close()
		}
		return nil
	})
	group.Go(func() error {
		scanner := bufio.NewScanner(opts.io.In)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			var request rpcRequest
			if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
				code, message := -32700, "Parse error"
				if json.Valid(scanner.Bytes()) {
					code, message = -32600, "Invalid Request"
				}
				if err := send(nil, nil, &rpcError{Code: code, Message: message}); err != nil {
					return err
				}
				continue
			}
			if request.Version != "2.0" || request.Method == "" || !validID(request.ID) {
				if err := send(nil, nil, &rpcError{Code: -32600, Message: "Invalid Request"}); err != nil {
					return err
				}
				continue
			}
			if len(request.ID) == 0 {
				if request.Method == "notifications/cancelled" {
					var params struct {
						RequestID json.RawMessage `json:"requestId"`
					}
					if json.Unmarshal(request.Params, &params) == nil {
						requests.Lock()
						if cancel := active[string(params.RequestID)]; cancel != nil {
							cancel()
						}
						requests.Unlock()
					}
				}
				continue
			}
			key := string(request.ID)
			requests.Lock()
			_, duplicate := active[key]
			if duplicate {
				requests.Unlock()
				if err := send(request.ID, nil, &rpcError{Code: -32600, Message: "Request id is already active"}); err != nil {
					return err
				}
				continue
			}
			select {
			case slots <- struct{}{}:
			default:
				requests.Unlock()
				if err := send(request.ID, nil, &rpcError{Code: -32025, Message: "ResourceExhausted", Data: map[string]any{"limit": "requests", "max": 32}}); err != nil {
					return err
				}
				continue
			}
			operation, cancel := context.WithCancel(lifetime)
			active[key] = cancel
			requests.Unlock()
			group.Go(func() error {
				defer func() {
					cancel()
					<-slots
				}()
				result, failure := source.handle(operation, request)
				// Retire the id before publishing its response so the client
				// can reuse it as soon as that response reaches its pipe.
				requests.Lock()
				delete(active, key)
				requests.Unlock()
				if operation.Err() != nil {
					return nil
				}
				return send(request.ID, result, failure)
			})
		}
		if err := scanner.Err(); err != nil && lifetime.Err() == nil {
			return err
		}
		return inputClosed
	})
	err = group.Wait()
	// Closing stdio during shutdown may win the race to report the first error.
	if ctx.Err() == nil && !errors.Is(err, inputClosed) {
		return err
	}
	return nil
}

func validID(id json.RawMessage) bool {
	if len(id) == 0 {
		return true
	}
	if len(id) > 1024 {
		return false
	}
	var text string
	if json.Unmarshal(id, &text) == nil {
		return true
	}
	_, err := strconv.ParseInt(string(id), 10, 64)
	return err == nil
}

func decodeParams(raw json.RawMessage, target any) *rpcError {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if raw[0] != '{' {
		return &rpcError{Code: -32602, Message: "params must be an object"}
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return &rpcError{Code: -32602, Message: fmt.Sprintf("Invalid params: %s", err)}
	}
	return nil
}
