package placement_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aramase/agentsessions/api"
	"github.com/aramase/agentsessions/controller"
	"github.com/aramase/agentsessions/eventlog"
	"github.com/aramase/agentsessions/harness/echoagent"
	"github.com/aramase/agentsessions/placement"
	"github.com/aramase/agentsessions/runtime/local"
)

// Keep the local lifecycle real, but supply distinct, complete incarnations at a shared router.
// The custom dialer replaces only the transport boundary.
type dialBackend struct{ *local.Backend }

func newDialBackend(t *testing.T) dialBackend {
	t.Helper()
	b := local.New(echoagent.Harness{})
	t.Cleanup(func() { _ = b.Close() })
	return dialBackend{b}
}

func (b dialBackend) Create(ctx context.Context, spec *api.SessionSpec) (api.Incarnation, error) {
	inc, err := b.Backend.Create(ctx, spec)
	if err != nil {
		return inc, err
	}
	inc.Worker, inc.Address, inc.Runtime, inc.FenceToken = "created-worker", "router:443", "test", 41
	return inc, nil
}

func (b dialBackend) Restore(ctx context.Context, ref api.SnapshotRef) (api.Incarnation, error) {
	inc, err := b.Backend.Restore(ctx, ref)
	if err != nil {
		return inc, err
	}
	inc.Worker, inc.Address, inc.Runtime, inc.FenceToken = "restored-worker", "router:443", "test", 73
	return inc, nil
}

// Literal durable prefixes exercise modern and legacy crash recovery, not just an empty Resume.
func interruptedDialLog(t *testing.T, modern bool) eventlog.Store {
	t.Helper()
	log := eventlog.AsStore(eventlog.New())
	fence, err := log.NewFence()
	if err != nil {
		t.Fatal(err)
	}
	events := []api.Event{{ExecutionID: "interrupted", Kind: api.EventInput, Message: api.TextMessage("user", "hi")}}
	if modern {
		count := int64(1)
		events = append([]api.Event{{ExecutionID: "interrupted", Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{InputCount: &count}}}, events...)
	}
	var head int64
	for _, ev := range events {
		rec, err := log.Append(head, fence, ev)
		if err != nil {
			t.Fatal(err)
		}
		head = rec.Seq
	}
	return log
}

func TestIncarnationDialerLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name            string
		resume, suspend bool
		markerless      bool
	}{
		{name: "exec"},
		{name: "exec_after_suspend", suspend: true},
		{name: "resume_suspended", resume: true, suspend: true},
		{name: "resume_crash_modern", resume: true},
		{name: "resume_crash_markerless", resume: true, markerless: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := eventlog.AsStore(eventlog.New())
			want := api.Incarnation{ID: "s", Worker: "created-worker", Address: "router:443", Runtime: "test", FenceToken: 41}
			if tc.resume {
				log = interruptedDialLog(t, !tc.markerless)
				want = api.Incarnation{ID: "s", Worker: "restored-worker", Address: "router:443", Runtime: "test", FenceToken: 73}
			}
			calls, closes, wantCalls := 0, 0, 1
			p := placement.New(newDialBackend(t), echoagent.Model, placement.WithIncarnationDialer(func(got api.Incarnation) (api.Harness, func() error, error) {
				calls++
				if got != want {
					t.Errorf("dial incarnation = %+v, want %+v", got, want)
				}
				return echoagent.Harness{}, func() error { closes++; return nil }, nil
			}))
			if tc.suspend {
				if !tc.resume {
					if _, err := p.Exec(t.Context(), log, "s", nil, 0); err != nil {
						t.Fatal(err)
					}
					wantCalls++
				}
				if _, err := p.Suspend(t.Context(), log, "s"); err != nil {
					t.Fatal(err)
				}
			}
			if tc.resume {
				if err := p.Resume(t.Context(), log, "s"); err != nil {
					t.Fatal(err)
				}
			} else {
				head, err := log.Head()
				if err != nil {
					t.Fatal(err)
				}
				inc, err := p.Exec(t.Context(), log, "s", []api.Message{*api.TextMessage("user", "hi")}, head)
				if err != nil {
					t.Fatal(err)
				}
				if inc.FenceToken <= 0 || inc.FenceToken == want.FenceToken {
					t.Fatalf("Exec did not stamp a log-minted fence: %+v", inc)
				}
				want.FenceToken = inc.FenceToken
				if inc != want {
					t.Fatalf("Exec incarnation = %+v, want %+v", inc, want)
				}
			}
			if calls != wantCalls || closes != wantCalls {
				t.Fatalf("dials/closes = %d/%d, want %d/%d", calls, closes, wantCalls, wantCalls)
			}
			assertDialOutput(t, log, "echo:hi")
		})
	}
}

func TestIncarnationDialerForkChildren(t *testing.T) {
	parent := eventlog.AsStore(eventlog.New())
	c, err := controller.New(parent, echoagent.Model)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(t.Context(), echoagent.Harness{}, nil, 0); err != nil {
		t.Fatal(err)
	}
	var dialed []api.Incarnation
	p := placement.New(newDialBackend(t), echoagent.Model, placement.WithIncarnationDialer(func(inc api.Incarnation) (api.Harness, func() error, error) {
		dialed = append(dialed, inc)
		return echoagent.Harness{}, func() error { return nil }, nil
	}))
	if _, err := p.Suspend(t.Context(), parent, "parent"); err != nil {
		t.Fatal(err)
	}
	head, err := parent.Head()
	if err != nil {
		t.Fatal(err)
	}
	children := []placement.ForkChild{
		{UID: "child-a", Log: eventlog.AsStore(eventlog.New())},
		{UID: "child-b", Log: eventlog.AsStore(eventlog.New())},
	}
	if err := p.Fork(t.Context(), parent, "parent", children, head); err != nil {
		t.Fatal(err)
	}
	if len(dialed) != 0 {
		t.Fatal("Fork must not dial")
	}
	for _, child := range children {
		recs, err := child.Log.Read(1)
		if err != nil {
			t.Fatal(err)
		}
		inherited := false
		for _, rec := range recs {
			lc := rec.Event.Lifecycle
			if rec.Event.Kind == api.EventLifecycle && lc != nil && lc.Kind == api.LifecycleSuspend && lc.Snapshot != nil && lc.Snapshot.Local == "parent" {
				inherited = true
			}
		}
		if !inherited {
			t.Fatal("child must inherit the parent's snapshot history")
		}
		head, err := child.Log.Head()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Exec(t.Context(), child.Log, child.UID, nil, head); err != nil {
			t.Fatal(err)
		}
		if err := p.Resume(t.Context(), child.Log, child.UID); err != nil {
			t.Fatal(err)
		}
	}
	want := []api.Incarnation{
		{ID: "child-a", Worker: "created-worker", Address: "router:443", Runtime: "test", FenceToken: 41},
		{ID: "child-a", Worker: "restored-worker", Address: "router:443", Runtime: "test", FenceToken: 73},
		{ID: "child-b", Worker: "created-worker", Address: "router:443", Runtime: "test", FenceToken: 41},
		{ID: "child-b", Worker: "restored-worker", Address: "router:443", Runtime: "test", FenceToken: 73},
	}
	if !reflect.DeepEqual(dialed, want) {
		t.Fatalf("child dial incarnations = %+v, want %+v", dialed, want)
	}
}

func TestIncarnationDialerPrecedence(t *testing.T) {
	for _, operation := range []string{"exec", "resume"} {
		for _, name := range []string{"legacy_only", "incarnation_first", "incarnation_last", "nil_first", "nil_last"} {
			t.Run(operation+"/"+name, func(t *testing.T) {
				errLegacy, errIncarnation := errors.New("legacy dial"), errors.New("incarnation dial")
				legacyCalls, incarnationCalls := 0, 0
				legacy := placement.WithDialer(func(address string) (api.Harness, func() error, error) {
					legacyCalls++
					if address != "router:443" {
						t.Errorf("legacy address = %q, want router:443", address)
					}
					return nil, nil, errLegacy
				})
				var dial placement.IncarnationDialer = func(api.Incarnation) (api.Harness, func() error, error) {
					incarnationCalls++
					return nil, nil, errIncarnation
				}
				if name == "nil_first" || name == "nil_last" {
					dial = nil
				}
				opts := []placement.Option{legacy}
				switch name {
				case "incarnation_first", "nil_first":
					opts = []placement.Option{placement.WithIncarnationDialer(dial), legacy}
				case "incarnation_last", "nil_last":
					opts = append(opts, placement.WithIncarnationDialer(dial))
				}
				p := placement.New(newDialBackend(t), echoagent.Model, opts...)
				log := eventlog.AsStore(eventlog.New())
				var err error
				if operation == "exec" {
					_, err = p.Exec(t.Context(), log, "s", nil, 0)
				} else {
					err = p.Resume(t.Context(), log, "s")
				}
				wantErr, wantLegacy, wantIncarnation := errLegacy, 1, 0
				if name == "incarnation_first" || name == "incarnation_last" {
					wantErr, wantLegacy, wantIncarnation = errIncarnation, 0, 1
				}
				if !errors.Is(err, wantErr) || legacyCalls != wantLegacy || incarnationCalls != wantIncarnation {
					t.Fatalf("error/calls = %v/%d/%d, want %v/%d/%d", err, legacyCalls, incarnationCalls, wantErr, wantLegacy, wantIncarnation)
				}
			})
		}
	}
}

func TestIncarnationDialerErrorsAndClose(t *testing.T) {
	errDial, errClose := errors.New("dial failed"), errors.New("close failed")
	for _, operation := range []string{"exec", "resume"} {
		for _, outcome := range []string{"dial_failure", "success", "controller_failure"} {
			t.Run(operation+"/"+outcome, func(t *testing.T) {
				log := eventlog.AsStore(eventlog.New())
				if outcome == "controller_failure" && operation == "resume" {
					fence, err := log.NewFence()
					if err != nil {
						t.Fatal(err)
					}
					if _, err := log.Append(0, fence, api.Event{ExecutionID: "bad", Kind: api.EventExecutionStart}); err != nil {
						t.Fatal(err)
					}
				}
				closes := 0
				p := placement.New(newDialBackend(t), echoagent.Model, placement.WithIncarnationDialer(func(api.Incarnation) (api.Harness, func() error, error) {
					close := func() error { closes++; return errClose }
					if outcome == "dial_failure" {
						return nil, close, errDial
					}
					return echoagent.Harness{}, close, nil
				}))
				var err error
				if operation == "exec" {
					expected := int64(0)
					if outcome == "controller_failure" {
						expected = 1
					}
					var inc api.Incarnation
					inc, err = p.Exec(t.Context(), log, "s", nil, expected)
					if outcome == "dial_failure" {
						want := api.Incarnation{ID: "s", Worker: "created-worker", Address: "router:443", Runtime: "test", FenceToken: 41}
						if inc != want {
							t.Fatalf("dial failure lost incarnation: %+v, want %+v", inc, want)
						}
					}
				} else {
					err = p.Resume(t.Context(), log, "s")
				}
				var wantErr error
				wantCloses := 1
				switch outcome {
				case "dial_failure":
					wantErr, wantCloses = errDial, 0
					if fence, fenceErr := log.NewFence(); fenceErr != nil || fence != 1 {
						t.Fatalf("dial failure minted a fence: next fence = %d, error = %v", fence, fenceErr)
					}
				case "controller_failure":
					wantErr = eventlog.ErrConflict
					if operation == "resume" {
						wantErr = controller.ErrInvalidExecutionLog
					}
				}
				if !errors.Is(err, wantErr) || closes != wantCloses {
					t.Fatalf("error/closes = %v/%d, want %v/%d", err, closes, wantErr, wantCloses)
				}
			})
		}
	}
}

type routedDialHarness struct {
	echoagent.Harness
	target string
}

func (h routedDialHarness) Run(ctx context.Context, _ *api.Start, sink api.EventSink) error {
	return sink.Output(ctx, h.target)
}

func TestIncarnationDialerOverlappingSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	arrived := make(chan api.Incarnation, 2)
	release := make(chan struct{})
	p := placement.New(newDialBackend(t), echoagent.Model, placement.WithIncarnationDialer(func(inc api.Incarnation) (api.Harness, func() error, error) {
		arrived <- inc
		select {
		case <-release:
			return routedDialHarness{target: inc.ID}, func() error { return nil }, nil
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}))
	logs := map[string]eventlog.Store{"a": eventlog.AsStore(eventlog.New()), "b": eventlog.AsStore(eventlog.New())}
	done := make(chan error, 2)
	for uid, log := range logs {
		go func() {
			_, err := p.Exec(ctx, log, uid, nil, 0)
			done <- err
		}()
	}
	seen := make(map[string]api.Incarnation)
	for range 2 {
		select {
		case inc := <-arrived:
			seen[inc.ID] = inc
		case <-ctx.Done():
			t.Fatal("two sessions did not overlap in the dial hook")
		}
	}
	close(release)
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("overlapping executions did not finish")
		}
	}
	for uid, log := range logs {
		want := api.Incarnation{ID: uid, Worker: "created-worker", Address: "router:443", Runtime: "test", FenceToken: 41}
		if seen[uid] != want {
			t.Fatalf("session %s dial incarnation = %+v, want %+v", uid, seen[uid], want)
		}
		assertDialOutput(t, log, uid)
	}
}

func assertDialOutput(t *testing.T, log eventlog.Store, want string) {
	t.Helper()
	recs, err := log.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range recs {
		if rec.Event.Kind == api.EventOutput && rec.Event.Message != nil && rec.Event.Message.Text() == want {
			if err := log.Verify(); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatalf("journal has no output %q", want)
}
