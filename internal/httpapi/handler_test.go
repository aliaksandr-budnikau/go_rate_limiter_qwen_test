package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeLimiter struct {
	allow bool
	calls []string
}

func (f *fakeLimiter) Allow(userID string) bool {
	f.calls = append(f.calls, userID)
	return f.allow
}

func TestHandlerStatusCodes(t *testing.T) {
	tests := []struct {
		name       string
		userID     string
		allow      bool
		wantStatus int
		wantCalls  int
	}{
		{name: "accepted", userID: "alice", allow: true, wantStatus: http.StatusOK, wantCalls: 1},
		{name: "limited", userID: "alice", allow: false, wantStatus: http.StatusTooManyRequests, wantCalls: 1},
		{name: "missing user", userID: "", allow: true, wantStatus: http.StatusBadRequest, wantCalls: 0},
		{name: "blank user", userID: "   ", allow: true, wantStatus: http.StatusBadRequest, wantCalls: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lim := &fakeLimiter{allow: tt.allow}
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.userID != "" {
				req.Header.Set(userIDHeader, tt.userID)
			}
			rec := httptest.NewRecorder()

			NewHandler(lim).ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if len(lim.calls) != tt.wantCalls {
				t.Fatalf("limiter calls = %d, want %d", len(lim.calls), tt.wantCalls)
			}
		})
	}
}
