package wire_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/aramase/agentsessions/api/genpb"
)

func TestSessionDeletionFieldsRoundTrip(t *testing.T) {
	requested := timestamppb.New(time.Unix(1_700_000_000, 123456789).UTC())
	completed := timestamppb.New(time.Unix(1_700_000_001, 987654321).UTC())
	// COMPLETE is serialization vocabulary only; no host cleanup transition is exercised here.
	for _, tc := range []struct {
		name                 string
		state                v1.DeletionState
		requested, completed *timestamppb.Timestamp
	}{
		{"unspecified", 0, nil, nil},
		{"active", 1, nil, nil},
		{"pending", 2, requested, nil},
		{"complete", 3, requested, completed},
		{"unknown", 99, requested, nil},
		{"present_epoch", 2, timestamppb.New(time.Unix(0, 0).UTC()), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := &v1.Session{
				Metadata:  &v1.ResourceMetadata{Uid: "session-audit", CreateTime: requested, UpdateTime: requested},
				ExecState: v1.ExecState_EXEC_AWAITING, ComputeState: v1.ComputeState_COMPUTE_COLD,
				LastSeq: 7, ParentUid: "parent", ForkSeq: 3,
				DeletionState: tc.state, DeletionRequestedTime: tc.requested, DeletionCompletedTime: tc.completed,
			}
			blob, err := proto.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			var got v1.Session
			if err := proto.Unmarshal(blob, &got); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(&got, want) {
				t.Fatalf("Session wire round-trip = %v, want %v", &got, want)
			}
		})
	}
}

func TestSessionDeletionWireTags(t *testing.T) {
	// These are the public wire slots, not Go struct indexes. A collision or wrong type would
	// make a non-Go client misread deletion status even if a Go self-roundtrip still passed.
	desc := (&v1.Session{}).ProtoReflect().Descriptor()
	for _, tc := range []struct {
		name   protoreflect.Name
		tag    protoreflect.FieldNumber
		kind   protoreflect.Kind
		typeID protoreflect.FullName
	}{
		{"deletion_state", 16, protoreflect.EnumKind, "agentsessions.v1.DeletionState"},
		{"deletion_requested_time", 17, protoreflect.MessageKind, "google.protobuf.Timestamp"},
		{"deletion_completed_time", 18, protoreflect.MessageKind, "google.protobuf.Timestamp"},
	} {
		field := desc.Fields().ByName(tc.name)
		if field == nil || field.Number() != tc.tag || field.Kind() != tc.kind {
			t.Fatalf("Session field %s = %v, want tag %d kind %s", tc.name, field, tc.tag, tc.kind)
		}
		var typeID protoreflect.FullName
		if field.Enum() != nil {
			typeID = field.Enum().FullName()
		} else {
			typeID = field.Message().FullName()
		}
		if typeID != tc.typeID {
			t.Errorf("Session field %s type = %s, want %s", tc.name, typeID, tc.typeID)
		}
	}
	if field := desc.Fields().ByNumber(15); field != nil {
		t.Fatalf("tag 15 must remain available for pending_approval, got %s", field.Name())
	}
	for _, tc := range []struct {
		number protoreflect.EnumNumber
		name   protoreflect.Name
	}{
		{0, "DELETION_STATE_UNSPECIFIED"}, {1, "DELETION_ACTIVE"},
		{2, "DELETION_PENDING"}, {3, "DELETION_COMPLETE"},
	} {
		value := v1.DeletionState(0).Descriptor().Values().ByNumber(tc.number)
		if value == nil || value.Name() != tc.name {
			t.Errorf("deletion enum %d = %v, want %s", tc.number, value, tc.name)
		}
	}
}
