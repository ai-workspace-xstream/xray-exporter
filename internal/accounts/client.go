package accounts

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"xray-exporter/internal/model"
)

type Client struct {
	baseURL      string
	serviceToken string
	httpClient   *http.Client
}

func NewClient(baseURL, serviceToken string) *Client {
	return &Client{
		baseURL:      strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		serviceToken: strings.TrimSpace(serviceToken),
		httpClient:   &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *Client) FetchIdentities(ctx context.Context) (map[string]model.Identity, error) {
	endpoint, err := url.JoinPath(c.baseURL, "/api/internal/network/identities")
	if err != nil {
		return nil, fmt.Errorf("build identities endpoint: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build identities request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Service-Token", c.serviceToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch identities: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch identities: unexpected status %s", resp.Status)
	}

	var payload struct {
		Identities []model.Identity `json:"identities"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode identities: %w", err)
	}

	result := make(map[string]model.Identity, len(payload.Identities))
	for _, identity := range payload.Identities {
		proxyUUID := strings.TrimSpace(identity.UUID)
		accountUUID := strings.TrimSpace(identity.AccountUUID)
		email := strings.ToLower(strings.TrimSpace(identity.Email))
		if proxyUUID == "" && accountUUID == "" && email == "" {
			continue
		}

		canonical := model.Identity{
			UUID:        proxyUUID,
			Email:       email,
			AccountUUID: accountUUID,
		}
		// Xray may label a counter with the proxy UUID, the account UUID, or
		// the user's email depending on which config generation produced it.
		// Keep all three lookup keys pointing at the same canonical identity;
		// the exporter will emit AccountUUID to Billing.
		for _, key := range []string{proxyUUID, accountUUID, email} {
			if key != "" {
				result[strings.ToLower(key)] = canonical
			}
		}
	}
	return result, nil
}
