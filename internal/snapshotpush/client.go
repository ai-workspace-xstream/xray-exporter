package snapshotpush

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"xray-exporter/internal/model"
)

// Client posts normalized snapshots to the local Vector HTTP source. Vector
// owns buffering and fan-out to Billing; the exporter never calls Billing.
type Client struct {
	endpoint   string
	token      string
	httpClient *http.Client
}

func New(endpoint, token string) *Client {
	return &Client{
		endpoint:   strings.TrimSpace(endpoint),
		token:      strings.TrimSpace(token),
		httpClient: &http.Client{},
	}
}

func (c *Client) PushSnapshot(ctx context.Context, snapshot model.Snapshot) error {
	if c == nil || c.endpoint == "" {
		return nil
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build snapshot request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post snapshot: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("post snapshot: unexpected status %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}
