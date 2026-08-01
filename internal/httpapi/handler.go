package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"xray-exporter/internal/service"
)

type Handler struct {
	service *service.Service
	token   string
}

func New(svc *service.Service, token string) *Handler {
	return &Handler{
		service: svc,
		token:   strings.TrimSpace(token),
	}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.healthz)
	mux.HandleFunc("/metrics", h.metrics)
	mux.HandleFunc("/v1/snapshots/latest", h.snapshot)
	mux.HandleFunc("/v1/snapshots/window", h.window)
	return mux
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	ok, message := h.service.Health()
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":  map[bool]string{true: "ok", false: "degraded"}[ok],
		"message": message,
	})
}

func (h *Handler) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(h.service.MetricsText()))
}

func (h *Handler) snapshot(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	snapshot, err := h.service.LatestSnapshot(r.Context())
	if err != nil {
		http.Error(w, "snapshot unavailable", http.StatusInternalServerError)
		return
	}

	payload, err := json.Marshal(snapshot)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(payload)
}

func (h *Handler) window(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	since, err := time.Parse(time.RFC3339, strings.TrimSpace(r.URL.Query().Get("since")))
	if err != nil {
		http.Error(w, "invalid since", http.StatusBadRequest)
		return
	}

	until := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
	if raw := strings.TrimSpace(r.URL.Query().Get("until")); raw != "" {
		until, err = time.Parse(time.RFC3339, raw)
		if err != nil {
			http.Error(w, "invalid until", http.StatusBadRequest)
			return
		}
	}
	if until.Before(since) {
		http.Error(w, "until must not be before since", http.StatusBadRequest)
		return
	}

	limit := 120
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		limit, err = parseLimit(raw)
		if err != nil {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
	}

	var cursor *time.Time
	if raw := strings.TrimSpace(r.URL.Query().Get("cursor")); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		cursor = &parsed
	}

	page, err := h.service.SnapshotWindow(r.Context(), since, until, limit, cursor)
	if err != nil {
		http.Error(w, "snapshot window unavailable", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(page)
}

func (h *Handler) authorize(r *http.Request) bool {
	expected := "Bearer " + strings.TrimSpace(h.token)
	if h.token == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(strings.TrimSpace(r.Header.Get("Authorization"))), []byte(expected)) == 1
}

func parseLimit(raw string) (int, error) {
	limit, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, err
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be positive")
	}
	if limit > 1440 {
		limit = 1440
	}
	return limit, nil
}
