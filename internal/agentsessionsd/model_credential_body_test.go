package agentsessionsd

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

type bindingRequestBody struct {
	io.Reader
	closes int
}

func (b *bindingRequestBody) Close() error { b.closes++; return nil }

func TestCredentialTransportRequestBodyOwnership(t *testing.T) {
	endpoint, _ := url.Parse("https://example.test/model")
	for _, name := range []string{"origin", "canceled", "source", "invalid", "delegated"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			destination := endpoint.String()
			if name == "origin" {
				destination = "https://other.test/model"
			}
			if name == "canceled" {
				cancel()
			}
			transport := credentialHTTPClient(endpoint, "Authorization", func(context.Context) (string, error) {
				if name == "source" {
					return "", errors.New("private source error")
				}
				if name == "delegated" {
					return "valid", nil
				}
				return "", nil
			}).Transport.(*credentialTransport)
			transport.base = credentialRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if name != "delegated" {
					t.Fatal("refused request reached base transport")
				}
				body := r.Body.(*bindingRequestBody)
				if body.closes != 0 {
					t.Fatal("body closed before delegation")
				}
				_ = body.Close()
				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			})
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, destination, nil)
			if err != nil {
				t.Fatal(err)
			}
			body := &bindingRequestBody{Reader: strings.NewReader("request")}
			req.Body = body
			_, err = transport.RoundTrip(req)
			if (err == nil) != (name == "delegated") {
				t.Fatalf("unexpected dispatch result: %v", err)
			}
			if body.closes != 1 {
				t.Fatalf("body closed %d times, want exactly once", body.closes)
			}
		})
	}
}
