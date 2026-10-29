package fetch

import (
	"net/http"
	"testing"
	"time"
)

func TestRetryAfter(t *testing.T) {
	if got := retryAfter("10", 5*time.Second); got != 5*time.Second {
		t.Fatalf("got %s", got)
	}
	if got := retryAfter("bad", time.Minute); got != 0 {
		t.Fatalf("got %s", got)
	}
}

func TestSafeHeaders(t *testing.T) {
	h := http.Header{"Content-Type": {"application/x-protobuf"}, "Set-Cookie": {"secret"}}
	got := safeHeaders(h)
	if got["Content-Type"] == "" || got["Set-Cookie"] != "" {
		t.Fatalf("unsafe headers: %#v", got)
	}
}
