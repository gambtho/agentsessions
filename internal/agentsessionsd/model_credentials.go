package agentsessionsd

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/net/http/httpguts"
)

// CredentialRef is a host-owned reference, never a credential value. Sources may define their
// own types; the daemon's default source implements only env.
type CredentialRef struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

// CredentialFunc obtains a raw credential for one HTTP attempt. Implementations must be safe
// for concurrent use and honor the request context, including cancellation and deadlines.
type CredentialFunc func(context.Context) (string, error)

// CredentialSource validates a reference and returns its per-attempt lookup without requiring
// a token or network probe at startup. Hosts can integrate identity SDKs through this seam.
type CredentialSource func(CredentialRef) (CredentialFunc, error)

// WithCredentialSource replaces the default env source. A nil source uses EnvCredentialSource.
func WithCredentialSource(source CredentialSource) Option {
	return func(o *options) { o.credentialSource = source }
}

// EnvCredentialSource checks env reference syntax and presence at startup, then rereads the
// variable for every request so rotation does not require rebuilding bindings.
func EnvCredentialSource(ref CredentialRef) (CredentialFunc, error) {
	value := os.Getenv(ref.Name)
	if ref.Type != "env" || !validEnvName(ref.Name) || value == "" || !httpguts.ValidHeaderFieldValue(value) {
		return nil, errors.New("model credential: invalid or unavailable environment reference")
	}
	return func(ctx context.Context) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		value := os.Getenv(ref.Name)
		if value == "" {
			return "", errors.New("model credential: environment value unavailable")
		}
		return value, nil
	}, nil
}

func validEnvName(name string) bool {
	if name == "" {
		return false
	}
	for i, c := range []byte(name) {
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' || (i > 0 && c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return true
}

func validCredentialHeader(header string) bool {
	if !httpguts.ValidHeaderFieldName(header) {
		return false
	}
	switch strings.ToLower(header) {
	case "content-type", "content-length", "host", "transfer-encoding", "connection", "te", "trailer", "upgrade", "proxy-authorization", "proxy-connection", "accept", "accept-encoding":
		return false
	}
	return true
}

type credentialTransport struct {
	base       http.RoundTripper
	origin     string
	header     string
	credential CredentialFunc
}

func credentialHTTPClient(endpoint *url.URL, header string, credential CredentialFunc) *http.Client {
	return &http.Client{
		Timeout:   2 * time.Minute,
		Transport: &credentialTransport{base: http.DefaultTransport, origin: credentialOrigin(endpoint), header: header, credential: credential},
	}
}

func (t *credentialTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// RoundTripper owns the body even when refusing before delegation. Once dispatched, the
	// base transport owns the same body through the clone and must close it instead.
	body := req.Body
	defer func() {
		if body != nil {
			_ = body.Close()
		}
	}()
	// Validate the destination before obtaining a token, even if redirect policy is replaced.
	if credentialOrigin(req.URL) != t.origin {
		return nil, errors.New("model credential: request origin refused")
	}
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	token, err := t.credential(req.Context())
	if err != nil {
		return nil, safeBindingError(err)
	}
	if token == "" || !httpguts.ValidHeaderFieldValue(token) {
		return nil, errors.New("model credential: invalid credential value")
	}
	clone := req.Clone(req.Context())
	if strings.EqualFold(t.header, "Authorization") {
		token = "Bearer " + token
	}
	clone.Header.Set(t.header, token)
	body = nil
	return t.base.RoundTrip(clone)
}

func credentialOrigin(u *url.URL) string {
	port := u.Port()
	if port == "" {
		if strings.EqualFold(u.Scheme, "https") {
			port = "443"
		} else if strings.EqualFold(u.Scheme, "http") {
			port = "80"
		}
	}
	return strings.ToLower(u.Scheme) + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port)
}
