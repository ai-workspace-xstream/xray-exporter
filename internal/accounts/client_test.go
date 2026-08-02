package accounts

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchIdentitiesIndexesProxyAccountAndEmail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/internal/network/identities" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("X-Service-Token") != "secret" {
			t.Fatalf("expected service token")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"identities":[{"uuid":"proxy-1","email":"User@Example.com","accountUuid":"account-1"}]}`))
	}))
	defer server.Close()

	identities, err := NewClient(server.URL, "secret").FetchIdentities(context.Background())
	if err != nil {
		t.Fatalf("fetch identities: %v", err)
	}
	for _, key := range []string{"proxy-1", "account-1", "user@example.com"} {
		identity, ok := identities[key]
		if !ok {
			t.Fatalf("expected lookup key %q in %#v", key, identities)
		}
		if identity.AccountUUID != "account-1" || identity.Email != "user@example.com" {
			t.Fatalf("unexpected canonical identity for %q: %#v", key, identity)
		}
	}
}
