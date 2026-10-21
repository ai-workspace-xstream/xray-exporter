package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Pusher delivers normalized exporter snapshots to Vector. It deliberately
// knows only the Vector URL, never a Billing URL, so Billing remains behind
// Vector's routing and retry policy.
type Pusher struct {
	url       string
	token     string
	env       string
	transport string
	metrics   *Metrics
	client    *http.Client
}

func NewPusher(url, token string, timeout time.Duration) *Pusher {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	p := &Pusher{
		url:     strings.TrimSpace(url),
		token:   strings.TrimSpace(token),
		metrics: DefaultMetrics(),
		client:  &http.Client{Timeout: timeout},
	}
	p.metrics.SetPushEnabled(p.env, p.transport, p.Enabled())
	return p
}

func (p *Pusher) WithTelemetry(env, transport string, metrics *Metrics) *Pusher {
	if p == nil {
		return &Pusher{
			env:       strings.TrimSpace(env),
			transport: strings.TrimSpace(transport),
			metrics:   metrics,
		}
	}
	p.env = strings.TrimSpace(env)
	p.transport = strings.TrimSpace(transport)
	if metrics != nil {
		p.metrics = metrics
	}
	if p.metrics == nil {
		p.metrics = DefaultMetrics()
	}
	p.metrics.SetPushEnabled(p.env, p.transport, p.Enabled())
	return p
}

func (p *Pusher) Enabled() bool {
	return p != nil && p.url != "" && p.token != ""
}

func (p *Pusher) Push(ctx context.Context, value Snapshot) error {
	var env, transport string
	var metrics *Metrics
	if p != nil {
		env = p.env
		transport = p.transport
		metrics = p.metrics
	}
	if env == "" {
		env = value.Env
	}
	if transport == "" {
		transport = DeriveTransport(value.NodeID, env)
	}
	if metrics == nil {
		metrics = DefaultMetrics()
	}
	metrics.SetPushEnabled(env, transport, p.Enabled())
	if !p.Enabled() {
		metrics.IncPushStatus(env, transport, "disabled")
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}
	pushStart := time.Now()
	defer func() {
		metrics.ObserveCollectStage(env, transport, "vector_push", time.Since(pushStart))
	}()

	body, err := json.Marshal(value)
	if err != nil {
		metrics.IncPushStatus(env, transport, "network_error")
		return fmt.Errorf("encode snapshot: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		metrics.IncPushStatus(env, transport, "network_error")
		return fmt.Errorf("create snapshot push request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "application/json")
	tp := value.TraceParent
	if tp == "" {
		tp = TraceParentFromContext(ctx)
	}
	if tp == "" {
		tp = GenerateTraceParent()
	}
	req.Header.Set("traceparent", tp)

	req, tracker := attachHTTPTrace(req)
	resp, err := p.client.Do(req)
	tracker.record(metrics, env, transport, "vector")
	if err != nil {
		metrics.IncPushStatus(env, transport, classifyHTTPError(err))
		return fmt.Errorf("push snapshot: %w", err)
	}
	defer resp.Body.Close()

	statusClass := classifyHTTPStatus(resp.StatusCode)
	metrics.IncPushStatus(env, transport, statusClass)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("push snapshot: HTTP %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	return nil
}
