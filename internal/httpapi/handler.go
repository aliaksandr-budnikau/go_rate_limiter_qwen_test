package httpapi

import (
	"net/http"
	"strings"
)

const userIDHeader = "X-User-ID"

// Limiter is the transport layer's only dependency on rate-limiting behavior.
type Limiter interface {
	Allow(userID string) bool
}

type Handler struct {
	limiter Limiter
}

func NewHandler(limiter Limiter) *Handler {
	return &Handler{limiter: limiter}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(r.Header.Get(userIDHeader))
	if userID == "" {
		http.Error(w, "missing X-User-ID header", http.StatusBadRequest)
		return
	}

	if !h.limiter.Allow(userID) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
