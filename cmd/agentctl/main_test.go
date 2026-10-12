package main

import (
	"bytes"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/aramase/agentsessions/api/genpb"
	"github.com/aramase/agentsessions/sqlitelog"
	"github.com/aramase/agentsessions/wire"
)

func TestExecHarnessFlag(t *testing.T) {
	for _, tt := range []struct {
		name    string
		session string
		harness string
	}{
		{name: "host default"},
		{name: "create chat", harness: "chat"},
		{name: "session default", session: "existing"},
		{name: "turn override", session: "existing", harness: "chat"},
		{name: "unknown harness", harness: "unknown"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requests := make(chan *v1.ExecRequest, 1)
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := grpc.NewServer()
			v1.RegisterSessionsServer(srv, &execRecorder{requests: requests})
			go func() { _ = srv.Serve(lis) }()
			t.Cleanup(srv.Stop)

			args := []string{"--server", lis.Addr().String(), "--input", "hello"}
			if tt.session != "" {
				args = append(args, "--session", tt.session)
			}
			if tt.harness != "" {
				args = append(args, "--harness", tt.harness)
			}
			err = cmdExec(args)
			if tt.harness == "unknown" {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("unknown harness error = %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			select {
			case req := <-requests:
				if req.GetHarness() != tt.harness || req.GetSession() != tt.session {
					t.Fatalf("exec request = %v", req)
				}
				if len(req.GetInputs()) != 1 || wire.MessageFromProto(req.Inputs[0]).Text() != "hello" {
					t.Fatalf("inputs = %v", req.GetInputs())
				}
				if req.ExpectedLastSeq != nil {
					t.Fatal("default exec unexpectedly enabled CAS")
				}
			default:
				t.Fatal("exec sent no request")
			}
		})
	}
}

func TestExecConfigFile(t *testing.T) {
	for _, tt := range []struct {
		name    string
		file    bool
		config  []byte
		missing bool
	}{
		{name: "set", file: true, config: []byte{0, 255, ' ', '\n', '\t'}},
		{name: "unset"},
		{name: "empty", file: true, config: []byte{}},
		{name: "missing file", file: true, missing: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requests := make(chan *v1.ExecRequest, 1)
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := grpc.NewServer()
			v1.RegisterSessionsServer(srv, &execRecorder{requests: requests})
			go func() { _ = srv.Serve(lis) }()
			t.Cleanup(srv.Stop)

			args := []string{"--server", lis.Addr().String(), "--input", "hello"}
			path := filepath.Join(t.TempDir(), "config.bin")
			if tt.file {
				args = append(args, "--config-file", path)
				if !tt.missing {
					if err := os.WriteFile(path, tt.config, 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			err = cmdExec(args)
			if tt.missing {
				if !errors.Is(err, os.ErrNotExist) || !strings.Contains(err.Error(), path) {
					t.Fatalf("missing config file: got %v, want path-qualified ErrNotExist", err)
				}
				select {
				case <-requests:
					t.Fatal("missing config file sent an Exec request")
				default:
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case req := <-requests:
				if got := req.GetConfig(); !bytes.Equal(got, tt.config) {
					t.Fatalf("config = %x, want %x", got, tt.config)
				}
			default:
				t.Fatal("exec sent no request")
			}
		})
	}
}

func TestExecMissingConfigFileLeavesNoJournal(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, "journal.db")
	path := filepath.Join(dir, "missing.bin")
	err := cmdExec([]string{"--journal", journal, "--input", "hello", "--config-file", path})
	if !errors.Is(err, os.ErrNotExist) || !strings.Contains(err.Error(), path) {
		t.Fatalf("missing config file: got %v, want path-qualified ErrNotExist", err)
	}
	if _, err := os.Stat(journal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal stat = %v, want ErrNotExist", err)
	}
}

type execRecorder struct {
	v1.UnimplementedSessionsServer
	requests chan *v1.ExecRequest
}

func (s *execRecorder) Exec(req *v1.ExecRequest, stream v1.Sessions_ExecServer) error {
	s.requests <- req
	if req.GetHarness() == "unknown" {
		return status.Error(codes.InvalidArgument, "unknown harness")
	}
	return stream.Send(&v1.ExecUpdate{Update: &v1.ExecUpdate_Session{Session: &v1.Session{
		Metadata: &v1.ResourceMetadata{Uid: "session-uid"},
		Harness:  req.GetHarness(),
	}}})
}

// Embedded mode refuses a journal in which another host registered the harness name it serves, and
// opens one whose registrations use other names.
func TestEmbeddedRefusesRegistrationCollision(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wantErr bool
	}{
		{name: "echo", wantErr: true},
		{name: "suite-agent"},
	} {
		journal := filepath.Join(t.TempDir(), "journal.db")
		store, err := sqlitelog.Open(journal)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.RegisterHarness(sqlitelog.HarnessRecord{Name: tc.name, UID: "u", Spec: "{}", SpecDigest: "d"}); err != nil {
			t.Fatal(err)
		}
		_ = store.Close()
		err = cmdList([]string{"--journal", journal})
		if tc.wantErr {
			if err == nil || !strings.Contains(err.Error(), "collides with a static harness name") {
				t.Errorf("registration %q: got %v, want a collision error", tc.name, err)
			}
		} else if err != nil {
			t.Errorf("registration %q: %v", tc.name, err)
		}
	}
}
