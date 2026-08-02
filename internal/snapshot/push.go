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
	url    string
	token  string
	client *http.Client
}

func NewPusher(url, token string, timeout time.Duration) *Pusher {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Pusher{
		url:    strings.TrimSpace(url),
		token:  strings.TrimSpace(token),
		client: &http.Client{Timeout: timeout},
	}
}

func (p *Pusher) Enabled() bool {
	return p != nil && p.url != "" && p.token != ""
}

func (p *Pusher) Push(ctx context.Context, value Snapshot) error {
	if !p.Enabled() {
		return nil
	}
	body, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create snapshot push request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("push snapshot: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("push snapshot: HTTP %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	return nil
}
