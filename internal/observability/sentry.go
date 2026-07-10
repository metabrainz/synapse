// Package observability wires up Sentry error tracking for Synapse services.
package observability

import (
	"fmt"
	"time"

	"github.com/getsentry/sentry-go"
)

// InitSentry initialises the Sentry SDK. Returns a flush function that must be
// deferred by the caller to ensure buffered events are sent before process exit.
// If dsn is empty the call is a no-op and the flush function does nothing.
func InitSentry(dsn, environment, release string, tracesSampleRate float64) (func(), error) {
	if dsn == "" {
		return func() {}, nil
	}
	if err := sentry.Init(sentry.ClientOptions{
		Dsn:              dsn,
		Environment:      environment,
		Release:          release,
		AttachStacktrace: true,
		TracesSampleRate: tracesSampleRate,
		EnableLogs:       true,
	}); err != nil {
		return func() {}, fmt.Errorf("sentry init: %w", err)
	}
	return func() { sentry.Flush(2 * time.Second) }, nil
}
