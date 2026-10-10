package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	atepb "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aramase/agentsessions/api"
	ateadapter "github.com/aramase/agentsessions/integrations/substrate"
	"github.com/aramase/agentsessions/runtime/substrate"
)

func TestSessionDeletePendingOnGVisor(t *testing.T) {
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-delete-gvisor"))
	for _, cold := range []bool{false, true} {
		name := "running"
		if cold {
			name = "suspended"
		}
		t.Run(name, func(t *testing.T) {
			runDeleteHierarchy(t, f, echoTemplate, api.Descriptor{ID: "echo", Capabilities: api.Capabilities{Resumability: api.ResumabilityStatelessReplay}}, cold)
		})
	}
}

func TestSessionDeletePendingOnMicroVM(t *testing.T) {
	f := newFixture(t, env("SUBSTRATE_ATESPACE", "e2e-delete-microvm"))
	for _, cold := range []bool{false, true} {
		name := "running"
		if cold {
			name = "suspended"
		}
		t.Run(name, func(t *testing.T) { runDeleteHierarchy(t, f, counterTemplate, counterDescriptor, cold) })
	}
}

// Retain the actual tag RPC response from the unmodified adapter. The pinned Control API has no
// read-only GetTag RPC; GetActorSnapshot(tag) below proves the same pin still resolves, without
// re-tagging or mutating scope just to observe metadata.
type deleteControl struct {
	grpc.ClientConnInterface
	mu   sync.Mutex
	tags map[string]*atepb.ActorSnapshotTag
}

func (c *deleteControl) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	err := c.ClientConnInterface.Invoke(ctx, method, args, reply, opts...)
	if err == nil && method == atepb.Control_TagActorSnapshot_FullMethodName {
		tag := reply.(*atepb.ActorSnapshotTag)
		c.mu.Lock()
		c.tags[tag.GetMetadata().GetName()] = tag
		c.mu.Unlock()
	}
	return err
}
func (c *deleteControl) tag(name string) *atepb.ActorSnapshotTag {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tags[name]
}

func runDeleteHierarchy(t *testing.T, f *fixture, template string, desc api.Descriptor, cold bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	observed := &deleteControl{ClientConnInterface: f.conn, tags: map[string]*atepb.ActorSnapshotTag{}}
	backend := substrate.New(ateadapter.New(observed, ""), f.atespace, substrate.ObjectRef{Namespace: f.tmplNS, Name: template}, desc)
	b := &deleteRuntime{Backend: backend}
	a := newDeleteAPI(t, b)
	ctl := atepb.NewControlClient(f.conn)
	owned := []string{}
	// Cleanup is test-owned Control work AFTER assertions, never product Delete or a change to Stop.
	// Track only this case's UIDs; cold actors are deleted directly, running ones are first suspended.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cleanupCancel()
		for i := len(owned) - 1; i >= 0; i-- {
			ref := &atepb.ObjectRef{Atespace: f.atespace, Name: owned[i]}
			actor, err := ctl.GetActor(cleanupCtx, &atepb.GetActorRequest{Actor: ref})
			if status.Code(err) == codes.NotFound {
				continue
			}
			if err != nil {
				t.Errorf("owned cleanup GetActor %s: %v", owned[i], err)
				continue
			}
			if actor.GetStatus() == atepb.Actor_STATUS_RUNNING {
				if _, err := ctl.SuspendActor(cleanupCtx, &atepb.SuspendActorRequest{Actor: ref}); err != nil {
					t.Errorf("owned cleanup Suspend %s: %v", owned[i], err)
					continue
				}
			}
			if _, err := ctl.DeleteActor(cleanupCtx, &atepb.DeleteActorRequest{Actor: ref}); err != nil {
				t.Errorf("owned cleanup DeleteActor %s: %v", owned[i], err)
			}
			if desc.Capabilities.Resumability == api.ResumabilityRequiresMemorySnapshot && observed.tag("fork-"+owned[i]) != nil {
				if _, err := ctl.DeleteActorSnapshotTag(cleanupCtx, &atepb.DeleteActorSnapshotTagRequest{Tag: &atepb.ObjectRef{Atespace: f.atespace, Name: "fork-" + owned[i]}}); err != nil {
					t.Errorf("owned cleanup pin %s: %v", owned[i], err)
				}
			}
		}
	}()
	parent := a.create(t, ctx, "delete-parent")
	owned = append(owned, parent)
	if got := a.exec(t, ctx, parent, "inc"); template == counterTemplate && got != "1" {
		t.Fatalf("initial RAM count=%q want 1", got)
	}
	// The existing echo pool has only two workers. Park setup actors through the product Suspend
	// operation while ACTIVE, without deleting them or changing the template's resource budget.
	if template == echoTemplate {
		if _, err := a.client.Suspend(ctx, parent); err != nil {
			t.Fatal(err)
		}
	}
	children := a.fork(t, ctx, parent, 2)
	owned = append(owned, children...)
	middle, sibling := children[0], children[1]
	if template == echoTemplate {
		if _, err := a.client.Suspend(ctx, middle); err != nil {
			t.Fatal(err)
		}
	}
	grandchild := a.fork(t, ctx, middle, 1)[0]
	owned = append(owned, grandchild)
	prefixSibling, prefixGrandchild := a.records(t, sibling), a.records(t, grandchild)
	if template == echoTemplate {
		for _, uid := range []string{sibling, grandchild} {
			if _, err := a.client.Suspend(ctx, uid); err != nil {
				t.Fatal(err)
			}
		}
	}
	deletionStatus(t, ctx, a, parent, cold)
	deletionStatus(t, ctx, a, middle, cold)
	actors := map[string]*atepb.Actor{}
	pins := map[string]*atepb.ActorSnapshot{}
	for _, uid := range owned {
		actor, err := ctl.GetActor(ctx, &atepb.GetActorRequest{Actor: &atepb.ObjectRef{Atespace: f.atespace, Name: uid}})
		if err != nil {
			t.Fatalf("control actor %s: %v", uid, err)
		}
		if actor.GetMetadata().GetUid() == "" || actor.GetActorTemplateName() != template || actor.GetActorTemplateNamespace() != f.tmplNS {
			t.Fatalf("actor identity/template %s: %v", uid, actor)
		}
		if uid == parent || uid == middle {
			want := atepb.Actor_STATUS_RUNNING
			if cold {
				want = atepb.Actor_STATUS_SUSPENDED
			}
			if actor.GetStatus() != want || (!cold && (actor.GetAteomPodUid() == "" || actor.GetAteomPodIp() == "")) || (cold && (actor.GetLatestSnapshot().GetName() == "" || actor.GetAteomPodIp() != "")) {
				t.Fatalf("control state %s: %v", uid, actor)
			}
		}
		actors[uid] = actor
		if template == counterTemplate && uid != parent {
			pins[uid] = readDeletePin(t, ctx, ctl, observed, f.atespace, uid)
		}
	}
	if template == counterTemplate {
		for child, source := range map[string]string{middle: parent, sibling: parent, grandchild: middle} {
			if pins[child].GetSourceActorUid() != actors[source].GetMetadata().GetUid() || pins[child].GetSourceActor().GetName() != source {
				t.Fatalf("pin %s does not retain actual source actor %s", child, source)
			}
		}
	}
	a.pending(t, ctx, b, parent)
	assertDeleteResources(t, ctx, ctl, observed, f.atespace, actors, pins)
	// Delete an intermediate ancestor too: its incoming pin AND its child's pin stay retained.
	a.pending(t, ctx, b, middle)
	assertDeleteResources(t, ctx, ctl, observed, f.atespace, actors, pins)
	listed, err := a.client.ListSessions(ctx, "delete-fixture")
	if err != nil {
		t.Fatal(err)
	}
	visible := map[string]bool{}
	for _, s := range listed {
		visible[s.GetMetadata().GetUid()] = true
	}
	if visible[parent] || visible[middle] || !visible[sibling] || !visible[grandchild] {
		t.Fatalf("noncascade List=%v", visible)
	}
	// After proving running actors unchanged by Delete/retry/refusals, test-owned Control parking
	// releases the echo pool's two workers for descendants. This is NOT an effect of product Delete;
	// the actors and pins remain, and the journal's compute projection is deliberately not repaired.
	if template == echoTemplate && !cold {
		for _, uid := range []string{parent, middle} {
			ref := &atepb.ObjectRef{Atespace: f.atespace, Name: uid}
			if _, err := ctl.SuspendActor(ctx, &atepb.SuspendActorRequest{Actor: ref}); err != nil {
				t.Fatalf("fixture park retained actor %s: %v", uid, err)
			}
			actor, err := ctl.GetActor(ctx, &atepb.GetActorRequest{Actor: ref})
			if err != nil || actor.GetStatus() != atepb.Actor_STATUS_SUSPENDED || actor.GetMetadata().GetUid() != actors[uid].GetMetadata().GetUid() {
				t.Fatalf("fixture parking lost actor %s: %v %v", uid, actor, err)
			}
			actors[uid] = actor
		}
	}
	for _, uid := range []string{sibling, grandchild} {
		if template == counterTemplate {
			// These clones are already running; snapshot before Resume to test actual RAM recovery,
			// rather than relying on ResumeActor accepting an already-running actor.
			if _, err := a.client.Suspend(ctx, uid); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := a.client.Resume(ctx, uid, false); err != nil {
			t.Fatalf("surviving Resume %s: %v", uid, err)
		}
		got := a.exec(t, ctx, uid, "inc")
		want := "echo:inc"
		if template == counterTemplate {
			want = "2"
		}
		if got != want {
			t.Fatalf("surviving descendant %s output=%q want %q", uid, got, want)
		}
	}
	assertDeletePrefix(t, prefixSibling, a.records(t, sibling))
	assertDeletePrefix(t, prefixGrandchild, a.records(t, grandchild))
	if template == echoTemplate {
		if _, err := a.client.Suspend(ctx, sibling); err != nil {
			t.Fatal(err)
		}
	}
	ownFork := a.fork(t, ctx, grandchild, 1)[0]
	owned = append(owned, ownFork)
	wantOwn := "echo:inc"
	if template == counterTemplate {
		wantOwn = "3"
	}
	if got := a.exec(t, ctx, ownFork, "inc"); got != wantOwn {
		t.Fatalf("descendant own fork output=%q want %q", got, wantOwn)
	}
	if template == counterTemplate {
		pins[ownFork] = readDeletePin(t, ctx, ctl, observed, f.atespace, ownFork)
		if pins[ownFork].GetSourceActorUid() != actors[grandchild].GetMetadata().GetUid() {
			t.Fatal("own fork pin lost grandchild source identity")
		}
		for uid, before := range pins {
			after := readDeletePin(t, ctx, ctl, observed, f.atespace, uid)
			if !sameDeleteMessage(t, before, after) {
				t.Fatalf("descendant activity released/replaced pin %s", uid)
			}
		}
	}
	for _, uid := range []string{parent, middle} {
		actor, err := ctl.GetActor(ctx, &atepb.GetActorRequest{Actor: &atepb.ObjectRef{Atespace: f.atespace, Name: uid}})
		if err != nil || !sameDeleteMessage(t, actors[uid], actor) {
			t.Fatalf("ancestor actor changed during descendant execution: %v %v", actor, err)
		}
	}
	t.Logf("retained parent/intermediate actors, %d clone pins, independent sibling/grandchild/own-fork; owned actors=%v", len(pins), owned)
}

func readDeletePin(t *testing.T, ctx context.Context, ctl atepb.ControlClient, observed *deleteControl, space, uid string) *atepb.ActorSnapshot {
	t.Helper()
	tag := observed.tag("fork-" + uid)
	if tag == nil || tag.GetMetadata().GetAtespace() != space || tag.GetMetadata().GetUid() == "" || tag.GetSnapshot().GetName() == "" || tag.GetScope() != atepb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE {
		t.Fatalf("actual clone tag metadata %s: %v", uid, tag)
	}
	byTag, err := ctl.GetActorSnapshot(ctx, &atepb.GetActorSnapshotRequest{Snapshot: &atepb.ActorSnapshotRef{Reference: &atepb.ActorSnapshotRef_Tag{Tag: &atepb.ObjectRef{Atespace: space, Name: "fork-" + uid}}}})
	if err != nil {
		t.Fatalf("retained pin lookup %s: %v", uid, err)
	}
	canonical, err := ctl.GetActorSnapshot(ctx, &atepb.GetActorSnapshotRequest{Snapshot: &atepb.ActorSnapshotRef{Reference: &atepb.ActorSnapshotRef_Snapshot{Snapshot: tag.GetSnapshot()}}})
	if err != nil || !sameDeleteMessage(t, byTag, canonical) || byTag.GetSourceActorUid() == "" || byTag.GetSourceActorVersion() <= 0 {
		t.Fatalf("pin no longer resolves exact source snapshot %s: %v %v", uid, byTag, err)
	}
	t.Logf("retained pin %s tagUID=%s snapshotUID=%s sourceActorUID=%s sourceVersion=%d", uid, tag.GetMetadata().GetUid(), byTag.GetMetadata().GetUid(), byTag.GetSourceActorUid(), byTag.GetSourceActorVersion())
	return byTag
}
func assertDeleteResources(t *testing.T, ctx context.Context, ctl atepb.ControlClient, observed *deleteControl, space string, actors map[string]*atepb.Actor, pins map[string]*atepb.ActorSnapshot) {
	t.Helper()
	for uid, before := range actors {
		after, err := ctl.GetActor(ctx, &atepb.GetActorRequest{Actor: &atepb.ObjectRef{Atespace: space, Name: uid}})
		if err != nil || !sameDeleteMessage(t, before, after) {
			t.Fatalf("Delete changed/removed actor %s: %v %v", uid, after, err)
		}
	}
	for uid, before := range pins {
		if after := readDeletePin(t, ctx, ctl, observed, space, uid); !sameDeleteMessage(t, before, after) {
			t.Fatalf("Delete released/changed pin %s", uid)
		}
	}
}
