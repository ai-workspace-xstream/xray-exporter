package snapshot

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAccountsClientIndexesProxyAccountAndEmail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/internal/network/identities" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("X-Service-Token") != "secret" {
			t.Fatalf("missing service token")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"identities":[{"uuid":"proxy-1","email":"User@Example.com","accountUuid":"account-1"}]}`))
	}))
	defer server.Close()

	identities, err := NewAccountsClient(server.URL, "secret").FetchIdentities(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"proxy-1", "account-1", "user@example.com"} {
		identity, ok := identities[key]
		if !ok {
			t.Fatalf("missing lookup key %q", key)
		}
		if identity.AccountUUID != "account-1" || identity.Email != "user@example.com" {
			t.Fatalf("unexpected identity %#v", identity)
		}
	}
}
