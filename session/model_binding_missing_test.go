package session_test

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/agent-primitives/sessions/api"
	v1 "github.com/agent-primitives/sessions/api/genpb"
	"github.com/agent-primitives/sessions/controller"
	"github.com/agent-primitives/sessions/eventlog"
	"github.com/agent-primitives/sessions/placement"
	"github.com/agent-primitives/sessions/runtime/local"
	"github.com/agent-primitives/sessions/session"
)

// Losing policy must not silently turn a saved opt-in into legacy execution, even when a
// provider fallback is available. Check the fence separately: an unchanged head is not enough.
func TestMissingModelPolicyAfterEmptySessionReopen(t *testing.T) {
	for _, operation := range []string{"Exec", "Resume"} {
		t.Run(operation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal.db")
			store := openStore(t, path)
			created, err := session.NewService(store, sessionPolicyRegistry(t, true)).CreateSession(t.Context(), &v1.CreateSessionRequest{})
			if err != nil {
				t.Fatal(err)
			}
			uid := created.GetMetadata().GetUid()
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store = openStore(t, path)
			info, err := store.SessionInfo(uid)
			if err != nil || !info.ModelPolicy || info.LastSeq != 0 {
				t.Fatalf("reopened metadata=%+v, %v", info, err)
			}
			var providers atomic.Int32
			r, backends := missingPolicyRegistry(t, false, true, &providers, false)
			client := serveRegistry(t, store, r)
			log := store.Session(uid)
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			// NewFence is setup and updates metadata; snapshot after it, not before it.
			info, err = store.SessionInfo(uid)
			if err != nil {
				t.Fatal(err)
			}
			if operation == "Exec" {
				_, err = collectPublicExec(client, &v1.ExecRequest{Session: uid})
			} else {
				_, err = session.NewService(store, r).Resume(t.Context(), &v1.ResumeRequest{Session: uid})
				if !errors.Is(err, session.ErrModelPolicyMissing) {
					t.Errorf("direct Resume lost missing-policy cause: %v", err)
				}
			}
			if status.Code(err) != codes.FailedPrecondition {
				t.Errorf("missing policy %s=%v, want FailedPrecondition", operation, err)
			}
			after, lookupErr := store.SessionInfo(uid)
			if lookupErr != nil || !reflect.DeepEqual(info, after) {
				t.Errorf("refusal changed metadata: %+v, %v", after, lookupErr)
			}
			assertMissingPolicyNoEffects(t, log, nil, fence, backends, &providers)
			if _, err := client.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid}); err != nil {
				t.Fatal(err)
			}
			replay, err := client.Replay(t.Context(), &v1.ReplayRequest{Session: uid})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := replay.Recv(); err != io.EOF {
				t.Fatalf("empty replay=%v", err)
			}
		})
	}
}

// Only live opted-in work is refused. Modern unconfigured fallback, a configured override, and another
// configured session remain available; missing policy must not become a global route block.
func TestMissingModelPolicyCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, override string
		enabled        bool
	}{
		{"modern unconfigured fallback", "", false},
		{"configured override", "other", true},
		{"unaffected route", "other", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openStore(t, ":memory:")
			sess, err := session.NewService(store, sessionPolicyRegistry(t, tc.enabled)).CreateSession(t.Context(), &v1.CreateSessionRequest{})
			if err != nil {
				t.Fatal(err)
			}
			var providers atomic.Int32
			r, _ := missingPolicyRegistry(t, false, true, &providers, false)
			client := serveRegistry(t, store, r)
			uid := sess.GetMetadata().GetUid()
			if tc.name == "unaffected route" {
				sess, err = client.CreateSession(t.Context(), &v1.CreateSessionRequest{Session: &v1.Session{Harness: "other"}})
				if err != nil {
					t.Fatal(err)
				}
				uid = sess.GetMetadata().GetUid()
			}
			if _, err := collectPublicExec(client, &v1.ExecRequest{Session: uid, Harness: tc.override}); err != nil {
				t.Fatal(err)
			}
			if providers.Load() != 1 {
				t.Fatalf("provider calls=%d, want 1", providers.Load())
			}
		})
	}
}

// Recovery must check the recorded B route, not metadata A. Cover both repairing a missing
// approval request and continuing a decided call through the real local Connect/Restore path.
func TestMissingModelPolicyRecoveryUsesRecordedRoute(t *testing.T) {
	for _, operation := range []string{"Resume", "Exec"} {
		for _, decided := range []bool{false, true} {
			for _, recordedPolicy := range []bool{false, true} {
				name := operation + "/" + map[bool]string{false: "repair", true: "decided"}[decided] + "/" + map[bool]string{false: "recorded missing", true: "metadata missing"}[recordedPolicy]
				t.Run(name, func(t *testing.T) {
					store := openStore(t, ":memory:")
					sess, err := session.NewService(store, sessionPolicyRegistry(t, true)).CreateSession(t.Context(), &v1.CreateSessionRequest{Session: &v1.Session{Model: "chosen"}})
					if err != nil {
						t.Fatal(err)
					}
					uid := sess.GetMetadata().GetUid()
					var effects atomic.Int32
					r, backends := missingPolicyRegistry(t, !recordedPolicy, recordedPolicy, &effects, true)
					client := serveRegistry(t, store, r)
					log := store.Session(uid)
					// Today allows only current. Recovery must retain the historical chosen
					// default rather than re-admitting it as a new turn.
					call := publicGateCall("call")
					events := []api.Event{
						{Kind: api.EventExecutionStart, ExecutionStart: &api.ExecutionStart{Harness: "other", HarnessVersion: "v1", InputCount: proto.Int64(0), Model: "chosen"}},
						{Kind: api.EventToolCall, ToolCall: &call},
					}
					if decided {
						events = append(events, api.Event{Kind: api.EventApprovalRequest, Approval: &api.ApprovalRequest{ToolCallID: "call"}})
					}
					seedFence, err := log.NewFence()
					if err != nil {
						t.Fatal(err)
					}
					for i, ev := range events {
						ev.ExecutionID = "execution"
						if _, err := log.Append(int64(i), seedFence, ev); err != nil {
							t.Fatal(err)
						}
					}
					if decided {
						decision := publicDecision(uid)
						decision.Approved = proto.Bool(true)
						if _, err := client.Approve(t.Context(), decision); err != nil {
							t.Fatal(err)
						}
					}
					before := routingRecords(t, log)
					info, err := client.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
					if err != nil {
						t.Fatal(err)
					}
					fence := before[len(before)-1].Fence
					if operation == "Resume" {
						_, err = client.Resume(t.Context(), &v1.ResumeRequest{Session: uid})
					} else {
						_, err = collectPublicExec(client, &v1.ExecRequest{Session: uid})
					}
					if !recordedPolicy {
						if status.Code(err) != codes.FailedPrecondition {
							t.Errorf("recovery=%v, want FailedPrecondition", err)
						}
						after, lookupErr := client.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
						if lookupErr != nil || !proto.Equal(info, after) {
							t.Errorf("refusal changed projection: %v, %v", after, lookupErr)
						}
						assertMissingPolicyNoEffects(t, log, before, fence, backends, &effects)
						return
					}
					if err != nil {
						t.Fatalf("configured recorded route refused due to metadata: %v", err)
					}
					if backends["echo"].creates.Load() != 0 || backends["echo"].restores.Load() != 0 {
						t.Fatal("recovery provisioned metadata route")
					}
					if decided {
						if effects.Load() != 1 || backends["other"].restores.Load() != 1 {
							t.Fatalf("recovery effects=%d restores=%d", effects.Load(), backends["other"].restores.Load())
						}
					} else {
						after, err := client.GetSession(t.Context(), &v1.GetSessionRequest{Uid: uid})
						if err != nil || after.GetPendingApproval().GetRequestSeq() != 3 {
							t.Fatalf("request not repaired: %v, %v", after, err)
						}
					}
				})
			}
		}
	}
}

func TestMissingModelPolicyAllowsSuspendAndFork(t *testing.T) {
	store := openStore(t, ":memory:")
	client := serveRegistry(t, store, sessionPolicyRegistry(t, true))
	uid := mustCreate(t, client)
	if _, err := collectPublicExec(client, &v1.ExecRequest{Session: uid}); err != nil {
		t.Fatal(err)
	}
	var providers atomic.Int32
	r, _ := missingPolicyRegistry(t, false, true, &providers, false)
	client = serveRegistry(t, store, r)
	if _, err := client.Suspend(t.Context(), &v1.SuspendRequest{Session: uid}); err != nil {
		t.Fatalf("Suspend missing policy: %v", err)
	}
	forked, err := client.Fork(t.Context(), &v1.ForkRequest{Session: uid, Count: 1})
	if err != nil || len(forked.GetChildren()) != 1 {
		t.Fatalf("Fork missing policy=%v, %v", forked, err)
	}
	child, err := store.SessionInfo(forked.GetChildren()[0].GetMetadata().GetUid())
	if err != nil || !child.ModelPolicy {
		t.Fatalf("fork lost saved opt-in: %+v, %v", child, err)
	}
	replay, err := client.Replay(t.Context(), &v1.ReplayRequest{Session: uid})
	if err != nil {
		t.Fatal(err)
	}
	for {
		_, err := replay.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if providers.Load() != 0 {
		t.Fatal("Suspend/Fork/replay called provider")
	}
}

// A removed chat-only binding can also remove the route itself when no global model is
// configured. Preserve that existing unknown-route classification rather than changing legacy
// routing, while proving the saved session still cannot execute on a fallback.
func TestMissingModelPolicyUnservedRoute(t *testing.T) {
	for _, operation := range []string{"Exec", "Resume"} {
		t.Run(operation, func(t *testing.T) {
			store := openStore(t, ":memory:")
			b := local.New(sessionPolicyHarness{})
			t.Cleanup(func() { _ = b.Close() })
			initial, err := placement.NewRegistry("chat", map[string]*placement.Placer{
				"chat": placement.New(b, nil, placement.WithModelPolicy(api.ModelPolicy{Default: "chosen", Allowed: []string{"chosen"}})),
			})
			if err != nil {
				t.Fatal(err)
			}
			sess, err := session.NewService(store, initial).CreateSession(t.Context(), &v1.CreateSessionRequest{})
			if err != nil {
				t.Fatal(err)
			}
			uid := sess.GetMetadata().GetUid()
			var effects atomic.Int32
			r, backends := missingPolicyRegistry(t, false, true, &effects, false)
			log := store.Session(uid)
			fence, err := log.NewFence()
			if err != nil {
				t.Fatal(err)
			}
			before, err := store.SessionInfo(uid)
			if err != nil {
				t.Fatal(err)
			}
			if operation == "Exec" {
				_, err = collectPublicExec(serveRegistry(t, store, r), &v1.ExecRequest{Session: uid})
			} else {
				_, err = session.NewService(store, r).Resume(t.Context(), &v1.ResumeRequest{Session: uid})
			}
			if status.Code(err) != codes.InvalidArgument || errors.Is(err, session.ErrModelPolicyMissing) {
				t.Fatalf("unserved route lost existing unknown-route classification: %v", err)
			}
			after, lookupErr := store.SessionInfo(uid)
			if lookupErr != nil || !reflect.DeepEqual(before, after) {
				t.Errorf("unserved route changed metadata: %+v, %v", after, lookupErr)
			}
			assertMissingPolicyNoEffects(t, log, nil, fence, backends, &effects)
		})
	}
}

func missingPolicyRegistry(t *testing.T, aPolicy, bPolicy bool, effects *atomic.Int32, gated bool) (*placement.Registry, map[string]*publicGateBackend) {
	t.Helper()
	entries := make(map[string]*placement.Placer)
	backends := make(map[string]*publicGateBackend)
	for _, route := range []struct {
		name   string
		policy bool
	}{{"echo", aPolicy}, {"other", bPolicy}} {
		var h api.Harness = sessionPolicyHarness{}
		if gated {
			h = &publicGateHarness{}
		}
		b := &publicGateBackend{Backend: local.New(h)}
		t.Cleanup(func() { _ = b.Close() })
		backends[route.name] = b
		var opts []placement.Option
		if route.policy {
			opts = append(opts, placement.WithModelPolicy(api.ModelPolicy{Default: "current", Allowed: []string{"current"}}))
		}
		if gated {
			opts = append(opts, dedupAllTools, placement.WithToolExecutor(func(context.Context, controller.ToolCallContext, api.ToolCall) (api.ToolResult, error) {
				effects.Add(1)
				return api.ToolResult{Output: map[string]any{"ok": true}}, nil
			}))
		}
		entries[route.name] = placement.New(b, func(_ context.Context, req api.ModelRequest) (api.ModelResponse, error) {
			effects.Add(1)
			return api.ModelResponse{Message: *api.TextMessage("assistant", req.Model)}, nil
		}, opts...)
	}
	r, err := placement.NewRegistry("echo", entries)
	if err != nil {
		t.Fatal(err)
	}
	return r, backends
}

func assertMissingPolicyNoEffects(t *testing.T, log eventlog.Store, before []eventlog.Record, fence int64, backends map[string]*publicGateBackend, effects *atomic.Int32) {
	t.Helper()
	after := routingRecords(t, log)
	if len(before) != len(after) || (len(before) != 0 && !reflect.DeepEqual(before, after)) {
		t.Errorf("refusal changed journal: before=%v after=%v", before, after)
	}
	if effects.Load() != 0 {
		t.Errorf("refusal called provider/executor %d times", effects.Load())
	}
	for name, b := range backends {
		if b.creates.Load() != 0 || b.restores.Load() != 0 || b.snapshots.Load() != 0 {
			t.Errorf("refusal changed %s compute: %v", name, b.ioCounts())
		}
	}
	if next, err := log.NewFence(); err != nil || next != fence+1 {
		t.Errorf("refusal advanced fence: next=%d previous=%d, %v", next, fence, err)
	}
}
