// Package headers Middleware based on https://github.com/unrolled/secure.
package headers

import (
	"context"
	"errors"
	"net/http"

	"github.com/containous/traefik/v2/pkg/config/dynamic"
	"github.com/containous/traefik/v2/pkg/log"
	"github.com/containous/traefik/v2/pkg/middlewares"
	"github.com/containous/traefik/v2/pkg/tracing"
	"github.com/opentracing/opentracing-go/ext"
)

const (
	typeName = "Headers"
)

func handleDeprecation(ctx context.Context, cfg *dynamic.Headers) {
	if cfg.AccessControlAllowOrigin != "" {
		log.FromContext(ctx).Warn("accessControlAllowOrigin is deprecated, please use accessControlAllowOriginList instead.")
		cfg.AccessControlAllowOriginList = append(cfg.AccessControlAllowOriginList, cfg.AccessControlAllowOrigin)
		cfg.AccessControlAllowOrigin = ""
	}
}

type headers struct {
	name    string
	handler http.Handler
}

// New creates a Headers middleware.
func New(ctx context.Context, next http.Handler, cfg dynamic.Headers, name string) (http.Handler, error) {
	// HeaderMiddleware -> SecureMiddleWare -> next
	mCtx := middlewares.GetLoggerCtx(ctx, name, typeName)
	logger := log.FromContext(mCtx)
	logger.Debug("Creating middleware")

	handleDeprecation(mCtx, &cfg)

	hasSecureHeaders := cfg.HasSecureHeadersDefined()
	hasCustomHeaders := cfg.HasCustomHeadersDefined()
	hasCorsHeaders := cfg.HasCorsHeadersDefined()

	if !hasSecureHeaders && !hasCustomHeaders && !hasCorsHeaders {
		return nil, errors.New("headers configuration not valid")
	}

	var handler http.Handler
	nextHandler := next

	if hasSecureHeaders {
		logger.Debugf("Setting up secureHeaders from %v", cfg)
		handler = newSecure(next, cfg, name)
		nextHandler = handler
	}

	if hasCustomHeaders || hasCorsHeaders {
		logger.Debugf("Setting up customHeaders/Cors from %v", cfg)
		handler = NewHeader(nextHandler, cfg)
	}

	return &headers{
		handler: handler,
		name:    name,
	}, nil
}

func (h *headers) GetTracingInformation() (string, ext.SpanKindEnum) {
	return h.name, tracing.SpanKindNoneEnum
}

func (h *headers) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	h.handler.ServeHTTP(rw, req)
}