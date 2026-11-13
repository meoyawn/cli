package serve

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cli/cli/v2/internal/ghrepo"
	"github.com/cli/go-gh/v2/pkg/config"
)

type savedEvent struct {
	Sequence uint64      `json:"sequence"`
	Run      workflowRun `json:"run"`
	Value    occurrence  `json:"value"`
}

type savedSource struct {
	Generation string           `json:"generation"`
	Sequence   uint64           `json:"sequence"`
	Previous   map[int64]string `json:"previous"`
	Runs       []workflowRun    `json:"runs"`
	Events     []savedEvent     `json:"events"`
}

func (s *eventSource) snapshot() savedSource {
	value := savedSource{Generation: s.generation, Sequence: s.sequence, Previous: s.previous, Runs: s.runs}
	for _, event := range s.buffer {
		value.Events = append(value.Events, savedEvent{Sequence: event.sequence, Run: event.run, Value: event.value})
	}
	return value
}

func (s *eventSource) restore(value savedSource) {
	s.generation, s.sequence, s.previous, s.runs = value.Generation, value.Sequence, value.Previous, value.Runs
	s.buffer = nil
	for _, event := range value.Events {
		s.buffer = append(s.buffer, bufferedEvent{sequence: event.Sequence, run: event.Run, value: event.Value})
	}
}

func (s *eventSource) openCheckpoint() error {
	key := sha256.Sum256([]byte(s.repo.RepoHost() + "/" + ghrepo.FullName(s.repo)))
	s.checkpoint = filepath.Join(config.CacheDir(), "mcp-events", fmt.Sprintf("%x.json", key))
	data, err := os.ReadFile(s.checkpoint)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read MCP event checkpoint: %w", err)
	}
	var value savedSource
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("decode MCP event checkpoint: %w", err)
	}
	if value.Generation == "" || len(value.Events) > 512 {
		return fmt.Errorf("invalid MCP event checkpoint %q", s.checkpoint)
	}
	s.restore(value)
	return nil
}

func (s *eventSource) save() error {
	if err := os.MkdirAll(filepath.Dir(s.checkpoint), 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(s.checkpoint), ".checkpoint-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := json.NewEncoder(file).Encode(s.snapshot()); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), s.checkpoint)
}
