package snapshot

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type AccountsClient struct {
	baseURL string
	token   string
	client  *http.Client
}

func NewAccountsClient(baseURL, token string) *AccountsClient {
	return &AccountsClient{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		token:   strings.TrimSpace(token),
		client:  &http.Client{},
	}
}

func (c *AccountsClient) FetchIdentities(timeout time.Duration) (map[string]Identity, error) {
	if c == nil || c.baseURL == "" || c.token == "" {
		return nil, fmt.Errorf("accounts identity source is not configured")
	}
	endpoint, err := url.JoinPath(c.baseURL, "/api/internal/network/identities")
	if err != nil {
		return nil, fmt.Errorf("build identities endpoint: %w", err)
	}
	request, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build identities request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-Service-Token", c.token)

	client := *c.client
	client.Timeout = timeout
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch identities: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch identities: unexpected status %s", response.Status)
	}

	var payload struct {
		Identities []Identity `json:"identities"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode identities: %w", err)
	}

	result := make(map[string]Identity, len(payload.Identities)*3)
	for _, identity := range payload.Identities {
		identity.ProxyUUID = strings.TrimSpace(identity.ProxyUUID)
		identity.AccountUUID = strings.TrimSpace(identity.AccountUUID)
		identity.Email = strings.ToLower(strings.TrimSpace(identity.Email))
		for _, key := range []string{identity.ProxyUUID, identity.AccountUUID, identity.Email} {
			if key != "" {
				result[strings.ToLower(key)] = identity
			}
		}
	}
	return result, nil
}
