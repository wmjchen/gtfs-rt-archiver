package fetch

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/rawstore"
	"gtfs-rt-archiver/internal/state"
)

func TestFetchStoresDecodedGzipEntityAndCountsWireBytes(t *testing.T) {
	version := "2.0"
	body, err := proto.Marshal(&gtfs.FeedMessage{Header: &gtfs.FeedHeader{GtfsRealtimeVersion: &version}})
	if err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	zipper := gzip.NewWriter(&encoded)
	if _, err := zipper.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := zipper.Close(); err != nil {
		t.Fatal(err)
	}
	fetcher, cfg, raw, db, _ := testFetcher(t, 1024)
	defer db.Close()
	fetcher.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		resp := response(r, encoded.Bytes(), int64(encoded.Len()))
		resp.Header.Set("Content-Encoding", "gzip")
		return resp, nil
	})
	result, err := fetcher.Fetch(context.Background(), cfg.Sources[0], cfg.Sources[0].Streams[0], time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if result.Capture.EncodedLength != int64(encoded.Len()) || result.Capture.DecodedLength != int64(len(body)) {
		t.Fatalf("lengths = encoded %d decoded %d", result.Capture.EncodedLength, result.Capture.DecodedLength)
	}
	stored, err := raw.Read(result.Capture.RawPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, body) {
		t.Fatal("stored entity was not content-decoded")
	}
}

func TestFetchPersistsValidResponseWithoutLeakingSecret(t *testing.T) {
	secret := "canary-api-secret"
	t.Setenv("TEST_FEED_KEY", secret)
	version := "2.0"
	timestamp := uint64(time.Now().Unix())
	body, err := proto.Marshal(&gtfs.FeedMessage{Header: &gtfs.FeedHeader{GtfsRealtimeVersion: &version, Timestamp: &timestamp}})
	if err != nil {
		t.Fatal(err)
	}
	fetcher, cfg, raw, db, logs := testFetcher(t, int64(len(body)+10))
	fetcher.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("apikey") != secret {
			t.Error("secret query parameter was not resolved")
		}
		return response(r, body, int64(len(body))), nil
	})
	defer db.Close()
	result, err := fetcher.Fetch(context.Background(), cfg.Sources[0], cfg.Sources[0].Streams[0], time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if result.Capture.ParseStatus != "valid" || !result.Capture.TransportComplete {
		t.Fatalf("capture = %+v", result.Capture)
	}
	if strings.Contains(result.Capture.SanitizedURL, secret) {
		t.Fatal("secret leaked into sanitized URL")
	}
	sidecar, err := raw.Read(result.Capture.SidecarPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sidecar, []byte(secret)) || strings.Contains(logs.String(), secret) {
		t.Fatal("secret leaked into durable metadata or logs")
	}
}

func TestFetchArchivesMalformedAndTruncated2xx(t *testing.T) {
	fetcher, cfg, _, db, _ := testFetcher(t, 100)
	truncated := bytes.Repeat([]byte{0xff}, 4096)
	cfg.Sources[0].Streams[0].MaxResponseBytes = 8192
	fetcher.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		resp := response(r, nil, 8192)
		resp.Body = &truncatedReader{data: truncated}
		return resp, nil
	})
	defer db.Close()
	result, err := fetcher.Fetch(context.Background(), cfg.Sources[0], cfg.Sources[0].Streams[0], time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if result.Capture.TransportComplete {
		t.Fatal("truncated response was marked complete")
	}
	if result.Capture.ParseStatus == "valid" {
		t.Fatal("malformed response was marked valid")
	}
	if result.Capture.DecodedLength != 4096 {
		t.Fatalf("partial body length = %d", result.Capture.DecodedLength)
	}
}

func TestFetchArchivesUnexpectedContentTypeWithWarning(t *testing.T) {
	version := "2.0"
	body, err := proto.Marshal(&gtfs.FeedMessage{Header: &gtfs.FeedHeader{GtfsRealtimeVersion: &version}})
	if err != nil {
		t.Fatal(err)
	}
	fetcher, cfg, _, db, _ := testFetcher(t, 1024)
	defer db.Close()
	fetcher.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		resp := response(r, body, int64(len(body)))
		resp.Header.Set("Content-Type", "text/plain; charset=utf-8")
		return resp, nil
	})
	result, err := fetcher.Fetch(context.Background(), cfg.Sources[0], cfg.Sources[0].Streams[0], time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if result.Capture.ParseStatus != "valid" || !contains(result.Capture.ValidationFlags, "unexpected_content_type") {
		t.Fatalf("unexpected capture metadata: %+v", result.Capture)
	}
}

func TestFetchRejectsOversizedBodyWithoutPartialCapture(t *testing.T) {
	fetcher, cfg, _, db, _ := testFetcher(t, 4)
	fetcher.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) { return response(r, []byte("123456"), 6), nil })
	defer db.Close()
	_, err := fetcher.Fetch(context.Background(), cfg.Sources[0], cfg.Sources[0].Streams[0], time.Now())
	if err == nil || !strings.Contains(err.Error(), "body_too_large") {
		t.Fatalf("expected body limit error, got %v", err)
	}
	status, err := db.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Captures != 0 || status.Ticks != 1 {
		t.Fatalf("status = %+v", status)
	}
}

func TestFetchRetriesEligibleStatusThenCapturesOnce(t *testing.T) {
	version := "2.0"
	body, err := proto.Marshal(&gtfs.FeedMessage{Header: &gtfs.FeedHeader{GtfsRealtimeVersion: &version}})
	if err != nil {
		t.Fatal(err)
	}
	fetcher, cfg, _, db, _ := testFetcher(t, 1024)
	defer db.Close()
	cfg.HTTP.Retry.Attempts = 2
	cfg.HTTP.Retry.InitialBackoff = config.Duration{}
	cfg.HTTP.Retry.MaxBackoff = config.Duration{}
	attempts := 0
	fetcher.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		resp := response(r, body, int64(len(body)))
		if attempts == 1 {
			resp.StatusCode = http.StatusServiceUnavailable
		}
		return resp, nil
	})
	result, err := fetcher.Fetch(context.Background(), cfg.Sources[0], cfg.Sources[0].Streams[0], time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || result.Tick.Attempts != 2 || result.Capture == nil {
		t.Fatalf("attempts=%d result=%+v", attempts, result)
	}
	status, err := db.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Ticks != 1 || status.Captures != 1 {
		t.Fatalf("retry created duplicate state: %+v", status)
	}
}

func TestFetchHonorsStreamAcceptHeaderOverride(t *testing.T) {
	version := "2.0"
	body, err := proto.Marshal(&gtfs.FeedMessage{Header: &gtfs.FeedHeader{GtfsRealtimeVersion: &version}})
	if err != nil {
		t.Fatal(err)
	}
	fetcher, cfg, _, db, _ := testFetcher(t, 1024)
	defer db.Close()
	cfg.Sources[0].Streams[0].Headers = map[string]string{"Accept": "*/*"}
	var gotAccept string
	fetcher.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotAccept = r.Header.Get("Accept")
		return response(r, body, int64(len(body))), nil
	})
	if _, err := fetcher.Fetch(context.Background(), cfg.Sources[0], cfg.Sources[0].Streams[0], time.Now()); err != nil {
		t.Fatal(err)
	}
	if gotAccept != "*/*" {
		t.Fatalf("stream Accept override was clobbered: got %q", gotAccept)
	}
}

func TestFetchDoesNotFollowRedirect(t *testing.T) {
	targetHits := 0
	fetcher, cfg, _, db, _ := testFetcher(t, 1024)
	defer db.Close()
	fetcher.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host == "redirect-target.example" {
			targetHits++
			return response(r, nil, 0), nil
		}
		resp := response(r, nil, 0)
		resp.StatusCode = http.StatusFound
		resp.Header.Set("Location", "https://redirect-target.example/feed")
		return resp, nil
	})
	_, err := fetcher.Fetch(context.Background(), cfg.Sources[0], cfg.Sources[0].Streams[0], time.Now())
	if err == nil || !strings.Contains(err.Error(), "http_status") {
		t.Fatalf("expected redirect status failure, got %v", err)
	}
	if targetHits != 0 {
		t.Fatal("redirect target was contacted")
	}
}

func testFetcher(t *testing.T, maxBytes int64) (*Fetcher, *config.Config, *rawstore.Store, *state.Store, *bytes.Buffer) {
	t.Helper()
	root := t.TempDir()
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.HTTP.Retry.Attempts = 1
	cfg.Sources = []config.Source{{ID: "demo", Timezone: "UTC", Location: time.UTC, Streams: []config.Stream{{
		ID: "feed", ExpectedKind: "mixed", URL: "https://example.test/feed", Interval: config.Duration{Duration: 30 * time.Second},
		MaxResponseBytes: maxBytes, Auth: config.Auth{Query: map[string]config.SecretRef{"apikey": {Env: "TEST_FEED_KEY"}}},
	}}}}
	if _, ok := os.LookupEnv("TEST_FEED_KEY"); !ok {
		t.Setenv("TEST_FEED_KEY", "test")
	}
	raw, err := rawstore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(context.Background(), cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	return New(&cfg, raw, db, logger, nil), &cfg, raw, db, logs
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func response(request *http.Request, body []byte, contentLength int64) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/x-protobuf"}}, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: contentLength, Request: request}
}

type truncatedReader struct {
	data []byte
	sent bool
}

func (r *truncatedReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, io.ErrUnexpectedEOF
	}
	r.sent = true
	return copy(p, r.data), nil
}
func (*truncatedReader) Close() error { return nil }

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
