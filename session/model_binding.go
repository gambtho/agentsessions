package session

import (
	"errors"
	"fmt"
)

// ErrModelPolicyMissing means a session created with host-owned model admission can no
// longer execute on the selected route because that route has no model policy.
var ErrModelPolicyMissing = errors.New("session model policy missing")

// checkSessionModelPolicy checks only policy presence. Current turn/default admission remains
// the Placer's responsibility; recovery must not revalidate a historical recorded default.
// Callers pass the saved opt-in bit and, for recovery, the registry's resolved pending route.
func (s *Service) checkSessionModelPolicy(enabled bool, name string) error {
	if !enabled {
		return nil
	}
	p, err := s.registry.For(name)
	if err != nil {
		return err
	}
	if _, configured, _ := p.ResolveModel(""); !configured {
		return fmt.Errorf("%w: harness %q has no model policy", ErrModelPolicyMissing, name)
	}
	return nil
}
