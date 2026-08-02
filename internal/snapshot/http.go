package snapshot

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func Handler(store *Store, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/snapshots/") {
			http.NotFound(w, r)
			return
		}
		if !authorized(r, token) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/snapshots/latest":
			snapshot, ok := store.Latest()
			if !ok {
				http.Error(w, "no snapshot available", http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(snapshot)
		case "/v1/snapshots/window":
			page := window(store, r)
			_ = json.NewEncoder(w).Encode(page)
		default:
			http.NotFound(w, r)
		}
	})
}

func authorized(r *http.Request, token string) bool {
	if token == "" {
		return false
	}
	return r.Header.Get("Authorization") == "Bearer "+token
}

func window(store *Store, r *http.Request) WindowPage {
	now := time.Now().UTC()
	since := parseTime(r.URL.Query().Get("since"), now.Add(-time.Hour))
	until := parseTime(r.URL.Query().Get("until"), now)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	var cursor *time.Time
	if value := r.URL.Query().Get("cursor"); value != "" {
		parsed := parseTime(value, time.Time{})
		if !parsed.IsZero() {
			cursor = &parsed
		}
	}
	page, _ := store.Window(since, until, limit, cursor)
	return page
}

func parseTime(value string, fallback time.Time) time.Time {
	if value == "" {
		return fallback
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return fallback
	}
	return parsed.UTC()
}
