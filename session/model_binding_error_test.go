package session

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/placement"
)

func TestMissingModelPolicyErrorMapping(t *testing.T) {
	for _, mapping := range []struct {
		name     string
		mapError func(error) error
	}{{"Exec", execError}, {"Resume", resumeError}} {
		t.Run(mapping.name, func(t *testing.T) {
			err := mapping.mapError(fmt.Errorf("selected route: %w", ErrModelPolicyMissing))
			if status.Code(err) != codes.FailedPrecondition || !errors.Is(err, ErrModelPolicyMissing) {
				t.Fatalf("missing policy lost code/cause: %v", err)
			}
			if errors.Is(err, api.ErrModelNotAllowed) || errors.Is(err, api.ErrInvalidModelPolicy) || errors.Is(err, placement.ErrRecordedHarnessNotServed) {
				t.Fatalf("missing policy conflated with model or harness failure: %v", err)
			}
		})
	}
}
