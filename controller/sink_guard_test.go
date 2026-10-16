package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/eventlog"
)

// Keep cancellation fixtures independent of approval tests in the external test package.
func cancellationLive(c *Controller) *liveSink {
	return &liveSink{c: c, executionID: "e", guard: &sinkGuard{}, desc: &api.Descriptor{Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, calls: make(map[string]bool)}
}

func cancellationMethods(ctx context.Context, s api.EventSink) []error {
	_, m := s.Model(ctx, api.ModelRequest{})
	_, c := s.ToolCall(ctx, api.ToolCall{ID: "c", Tool: "write", IdempotencyKey: "k", Mediation: api.MediationRequiresApproval})
	return []error{m, c, s.Output(ctx, "late"), s.Report(ctx, api.ToolResult{ID: "c"}), s.Usage(ctx, api.Usage{})}
}

func TestSinkQueuedCancellation(t *testing.T) {
	for _, resume := range []bool{false, true} {
		name := "live"
		if resume {
			name = "resume"
		}
		t.Run(name, func(t *testing.T) {
			started := make(chan struct{}, 1)
			log := eventlog.AsStore(eventlog.New())
			c, err := New(log, func(ctx context.Context, _ api.ModelRequest) (api.ModelResponse, error) {
				started <- struct{}{}
				<-ctx.Done()
				return api.ModelResponse{}, ctx.Err()
			})
			if err != nil {
				t.Fatal(err)
			}
			var sink api.EventSink = cancellationLive(c)
			if resume {
				sink = &resumeSink{live: *cancellationLive(c)}
			}
			firstCtx, stopFirst := context.WithCancel(t.Context())
			defer stopFirst()
			first := make(chan error, 1)
			go func() { _, err := sink.Model(firstCtx, api.ModelRequest{}); first <- err }()
			<-started // The first call holds the guard while the provider waits.
			ctx, cancel := context.WithCancel(t.Context())
			queued := make(chan []error, 1)
			go func() { queued <- cancellationMethods(ctx, sink) }()
			cancel()
			var errs []error
			select {
			case errs = <-queued:
			case <-time.After(time.Second):
				t.Error("canceled waiter blocked behind the provider")
			}
			stopFirst()
			if err := <-first; !errors.Is(err, context.Canceled) {
				t.Fatalf("first call = %v", err)
			}
			if errs == nil {
				errs = <-queued
			}
			for _, err := range errs {
				if !errors.Is(err, context.Canceled) {
					t.Errorf("queued effect = %v, want canceled", err)
				}
			}
			recs, err := log.Read(1)
			if err != nil || len(recs) != 1 || recs[0].Event.Kind != api.EventModelCall {
				t.Fatalf("canceled effects changed journal: %+v, %v", recs, err)
			}
		})
	}
}

func TestSinkCanceledContextAndTerminalPrecedence(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, terminal := range []error{nil, ErrApprovalReceiptReport, &api.ApprovalParkedError{Ref: api.ApprovalRef{ExecutionID: "e", ToolCallID: "c", RequestSeq: 2}}} {
		guard := &sinkGuard{stop: terminal}
		c, err := New(eventlog.AsStore(eventlog.New()), func(ctx context.Context, _ api.ModelRequest) (api.ModelResponse, error) {
			return api.ModelResponse{}, ctx.Err()
		})
		if err != nil {
			t.Fatal(err)
		}
		live := cancellationLive(c)
		live.guard = guard
		replay := &replaySink{guard: guard, stream: []api.Event{{Kind: api.EventUsage, Usage: &api.Usage{}}}}
		failedLive := *live
		failedLive.guard = &sinkGuard{}
		for _, sink := range []api.EventSink{live, &resumeSink{live: *live}, replay,
			&resumeSink{live: failedLive, failure: terminal}, &replaySink{guard: &sinkGuard{}, failure: terminal}} {
			want := terminal
			if want == nil {
				want = context.Canceled
			}
			for _, err := range cancellationMethods(ctx, sink) {
				if !errors.Is(err, want) {
					t.Errorf("%T: got %v, want %v", sink, err, want)
				}
			}
		}
		if replay.i != 0 || replay.failure != nil {
			t.Fatal("canceled replay consumed evidence or latched divergence")
		}
	}
}
