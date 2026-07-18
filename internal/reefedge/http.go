// Package reefedge owns Coral-specific integration with Reef's production edge
// APIs. It keeps transport lifecycle and warning handling consistent across
// every signal exporter.
package reefedge

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/yaop-labs/reef/edge"
	"github.com/yaop-labs/reef/reefclient"
)

// NewHTTPClient returns a target-bound, managed HTTP client. The caller owns
// the returned EdgeClient and must close it with the exporter.
func NewHTTPClient(
	timeout time.Duration,
	cfg edge.ClientConfig,
	logger *slog.Logger,
) (*http.Client, *reefclient.EdgeClient, error) {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	managed, warnings, err := reefclient.NewEdgeTransport(cfg, nil)
	if err != nil {
		return nil, nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	for _, warning := range warnings {
		logger.Warn("reef edge configuration warning", "warning", string(warning))
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: managed,
		// A redirect can otherwise escape the target origin before RoundTrip
		// rejects it. Returning the original response is explicit and avoids
		// forwarding credentials or converting OTLP POST to GET.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}, managed, nil
}
