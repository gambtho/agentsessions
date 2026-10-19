package agentsessionsd

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/controller"
	"github.com/agent-primitives/sessions/internal/modelconfig"
	"github.com/agent-primitives/sessions/model/openai"
)

type modelBinding struct {
	policy api.ModelPolicy
	model  controller.ModelFunc
	stream controller.StreamFunc
}

type modelBindings map[string]modelBinding

type modelBindingConfig struct {
	Version  int                 `json:"version"`
	Bindings []modelBindingEntry `json:"bindings"`
}
type modelBindingEntry struct {
	Name          string         `json:"name"`
	Endpoint      string         `json:"endpoint"`
	Protocol      string         `json:"protocol"`
	DefaultModel  string         `json:"default_model"`
	AllowedModels []string       `json:"allowed_models"`
	Credential    *CredentialRef `json:"credential"`
	AuthHeader    string         `json:"auth_header"`
}

func loadModelBindings(path string, source CredentialSource) (modelBindings, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("model bindings: cannot open configuration")
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return nil, errors.New("model bindings: cannot read configuration")
	}
	if len(data) > 1<<20 {
		return nil, errors.New("model bindings: configuration exceeds 1 MiB")
	}
	var config modelBindingConfig
	if err := strictJSON(data, &config); err != nil {
		return nil, errors.New("model bindings: invalid JSON configuration")
	}
	if config.Version != 1 {
		return nil, errors.New("model bindings: version 1 required")
	}
	if source == nil {
		source = EnvCredentialSource
	}
	result := make(modelBindings, len(config.Bindings))
	for _, entry := range config.Bindings {
		if entry.Name == "" {
			return nil, errors.New("model bindings: nonempty unique names required")
		}
		if _, ok := result[entry.Name]; ok {
			return nil, errors.New("model bindings: nonempty unique names required")
		}
		binding, err := compileModelBinding(entry, source)
		if err != nil {
			return nil, err
		}
		result[entry.Name] = binding
	}
	return result, nil
}

func compileModelBinding(entry modelBindingEntry, source CredentialSource) (modelBinding, error) {
	policy := api.ModelPolicy{Default: entry.DefaultModel, Allowed: entry.AllowedModels}
	if _, err := policy.Resolve(""); err != nil {
		return modelBinding{}, errors.New("model bindings: invalid model policy")
	}
	endpoint, err := url.Parse(entry.Endpoint)
	if err != nil || endpoint.Host == "" || endpoint.Hostname() == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
		endpoint.User != nil || strings.Contains(entry.Endpoint, "#") || endpoint.Path == "" || !strings.HasPrefix(endpoint.Path, "/") || strings.HasPrefix(endpoint.Path, "//") {
		return modelBinding{}, errors.New("model bindings: invalid endpoint")
	}
	if entry.Protocol != "chat-completions" && entry.Protocol != "responses" {
		return modelBinding{}, errors.New("model bindings: unsupported protocol")
	}
	header := entry.AuthHeader
	if header == "" {
		header = "Authorization"
	}
	if !validCredentialHeader(header) || (entry.AuthHeader != "" && entry.Credential == nil) {
		return modelBinding{}, errors.New("model bindings: invalid authentication header")
	}
	cfg := modelconfig.Config{Model: policy.Default, BaseURL: endpoint.Scheme + "://" + endpoint.Host, Path: endpoint.EscapedPath()}
	if endpoint.RawQuery != "" {
		cfg.Path += "?" + endpoint.RawQuery
	}
	if entry.Credential != nil {
		if entry.Credential.Type == "" || entry.Credential.Name == "" {
			return modelBinding{}, errors.New("model bindings: invalid credential reference")
		}
		credential, err := source(*entry.Credential)
		if err != nil || credential == nil {
			return modelBinding{}, errors.New("model bindings: credential source unavailable")
		}
		cfg.Credentialed = true
		cfg.HTTPClient = credentialHTTPClient(endpoint, header, credential)
	}
	var model controller.ModelFunc
	var stream controller.StreamFunc
	var builtEndpoint string
	if entry.Protocol == "responses" {
		client, err := modelconfig.NewResponses(cfg)
		if err != nil {
			return modelBinding{}, errors.New("model bindings: cannot construct provider")
		}
		model, stream, builtEndpoint = client.Model, client.StreamModel, client.Endpoint()
	} else {
		client, err := modelconfig.New(cfg)
		if err != nil {
			return modelBinding{}, errors.New("model bindings: cannot construct provider")
		}
		model, stream, builtEndpoint = client.Model, client.StreamModel, client.Endpoint()
	}
	if builtEndpoint != endpoint.String() {
		return modelBinding{}, errors.New("model bindings: endpoint normalization refused")
	}
	return modelBinding{
		policy: policy,
		model: func(ctx context.Context, req api.ModelRequest) (api.ModelResponse, error) {
			response, err := model(ctx, req)
			if err != nil {
				return api.ModelResponse{}, safeBindingError(err)
			}
			return response, nil
		},
		stream: func(ctx context.Context, req api.ModelRequest, chunk func(string)) (api.ModelResponse, error) {
			response, err := stream(ctx, req, chunk)
			if err != nil {
				return api.ModelResponse{}, safeBindingError(err)
			}
			return response, nil
		},
	}, nil
}

// Return only fixed categories: even wrapped context/unsupported errors can contain credentials
// or private endpoints in their outer message. Never preserve those messages or wrapped causes.
func safeBindingError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(err, openai.ErrUnsupportedPart):
		return openai.ErrUnsupportedPart
	default:
		return errors.New("model binding: provider request failed")
	}
}
