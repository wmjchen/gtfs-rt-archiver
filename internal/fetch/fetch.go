package fetch

import (
	"compress/gzip"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/gtfsrt"
	"gtfs-rt-archiver/internal/model"
	"gtfs-rt-archiver/internal/rawstore"
	"gtfs-rt-archiver/internal/state"
	"gtfs-rt-archiver/internal/version"
)

type Observer interface {
	ObserveFetch(source, stream, result string, duration time.Duration, bytes int64)
	ObserveValidation(source, stream, flag string)
	ObserveFeedMetadata(source, stream string, observed time.Time, feedTimestamp *uint64, valid bool)
}

type noopObserver struct{}

func (noopObserver) ObserveFetch(string, string, string, time.Duration, int64) {}
func (noopObserver) ObserveValidation(string, string, string)                  {}
func (noopObserver) ObserveFeedMetadata(string, string, time.Time, *uint64, bool) {
}

type Fetcher struct {
	cfg         *config.Config
	client      *http.Client
	raw         *rawstore.Store
	state       *state.Store
	log         *slog.Logger
	observer    Observer
	fingerprint string
}

type Result struct {
	Tick    model.Tick
	Capture *model.Capture
}

var ErrBodyTooLarge = errors.New("decoded body exceeds configured limit")

func New(cfg *config.Config, raw *rawstore.Store, stateStore *state.Store, log *slog.Logger, observer Observer) *Fetcher {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	transport.MaxIdleConns = 200
	transport.MaxIdleConnsPerHost = max(4, cfg.Runtime.PerHostConcurrency)
	transport.MaxConnsPerHost = max(4, cfg.Runtime.PerHostConcurrency)
	client := &http.Client{
		Transport:     transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}
	if observer == nil {
		observer = noopObserver{}
	}
	return &Fetcher{cfg: cfg, client: client, raw: raw, state: stateStore, log: log, observer: observer, fingerprint: cfg.Fingerprint()}
}

func (f *Fetcher) Fetch(ctx context.Context, source config.Source, stream config.Stream, scheduledAt time.Time) (Result, error) {
	now := time.Now()
	tickID, err := model.NewID(now)
	if err != nil {
		return Result{}, err
	}
	started := now.UTC()
	tick := model.Tick{ID: tickID, SourceID: source.ID, StreamID: stream.ID, ScheduledAt: scheduledAt.UTC(), StartedAt: &started, Result: "running", ConfigFingerprint: f.fingerprint}
	if err := f.state.RecordTick(ctx, tick); err != nil {
		return Result{}, err
	}
	result := Result{Tick: tick}

	requestURL, headers, err := resolveRequest(stream)
	if err != nil {
		return f.finishFailure(ctx, result, "configuration", err.Error(), 0, 0)
	}
	timeout := stream.EffectiveTimeout(f.cfg.HTTP)
	attempts := f.cfg.HTTP.Retry.Attempts
	var lastCategory string
	var lastStatus int
	for attempt := 1; attempt <= attempts; attempt++ {
		result.Tick.Attempts = attempt
		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		req, reqErr := http.NewRequestWithContext(attemptCtx, http.MethodGet, requestURL.String(), nil)
		if reqErr != nil {
			cancel()
			return f.finishFailure(ctx, result, "configuration", "invalid request", 0, attempt)
		}
		for name, value := range stream.Headers {
			req.Header.Set(name, value)
		}
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		if req.Header.Get("Accept") == "" {
			req.Header.Set("Accept", "application/x-protobuf, application/octet-stream")
		}
		req.Header.Set("Accept-Encoding", "gzip")
		req.Header.Set("User-Agent", f.cfg.HTTP.UserAgent)
		attemptStart := time.Now()
		resp, doErr := f.client.Do(req)
		if doErr != nil {
			cancel()
			lastCategory = classifyNetwork(doErr)
			if lastCategory == "cancelled" {
				return f.finishFailure(ctx, result, lastCategory, "request cancelled", 0, attempt)
			}
			if attempt < attempts && retryableNetwork(doErr) {
				if err := waitBackoff(ctx, f.cfg.HTTP.Retry, attempt, 0); err != nil {
					return f.finishFailure(ctx, result, "cancelled", "request cancelled", 0, attempt)
				}
				continue
			}
			return f.finishFailure(ctx, result, lastCategory, "request failed", 0, attempt)
		}
		lastStatus = resp.StatusCode
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
			cancel()
			if attempt < attempts && retryableStatus(resp.StatusCode) {
				if err := waitBackoff(ctx, f.cfg.HTTP.Retry, attempt, retryAfter(resp.Header.Get("Retry-After"), stream.Interval.Duration)); err != nil {
					return f.finishFailure(ctx, result, "cancelled", "request cancelled", resp.StatusCode, attempt)
				}
				continue
			}
			return f.finishFailure(ctx, result, "http_status", fmt.Sprintf("HTTP %d", resp.StatusCode), resp.StatusCode, attempt)
		}

		capture, captureErr := f.captureResponse(attemptCtx, source, stream, result.Tick, resp, attemptStart, attempt)
		resp.Body.Close()
		cancel()
		if captureErr != nil {
			category := "local_storage"
			detail := "capture commit failed"
			if errors.Is(captureErr, ErrBodyTooLarge) {
				category, detail = "body_too_large", "response exceeded decoded body limit"
			}
			return f.finishFailure(ctx, result, category, detail, resp.StatusCode, attempt)
		}
		finished := capture.CompletedAt
		result.Tick.FinishedAt = &finished
		result.Tick.HTTPStatus = resp.StatusCode
		result.Tick.Result = "captured"
		if capture.ParseStatus != "valid" || !capture.TransportComplete {
			result.Tick.Result = "captured_invalid"
		}
		result.Capture = capture
		if err := f.state.SaveCapture(ctx, result.Tick, *capture); err != nil {
			return result, fmt.Errorf("capture files committed but state update failed: %w", err)
		}
		f.observer.ObserveFetch(source.ID, stream.ID, result.Tick.Result, time.Since(started), capture.DecodedLength)
		f.observer.ObserveFeedMetadata(source.ID, stream.ID, capture.CompletedAt, capture.FeedTimestamp,
			capture.ParseStatus == "valid" && capture.TransportComplete)
		for _, flag := range capture.ValidationFlags {
			f.observer.ObserveValidation(source.ID, stream.ID, flag)
		}
		f.log.Info("fetch captured", "source", source.ID, "stream", stream.ID, "capture_id", capture.ID,
			"parse_status", capture.ParseStatus, "transport_complete", capture.TransportComplete,
			"bytes", capture.DecodedLength, "duration_ms", capture.DurationMS)
		return result, nil
	}
	return f.finishFailure(ctx, result, lastCategory, "request failed", lastStatus, attempts)
}

func (f *Fetcher) captureResponse(ctx context.Context, source config.Source, stream config.Stream, tick model.Tick, resp *http.Response, started time.Time, attempt int) (*model.Capture, error) {
	counted := &countingReader{reader: resp.Body}
	var decoded io.Reader = counted
	var decoder io.Closer
	encoding := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))
	var setupErr error
	switch encoding {
	case "", "identity":
	case "gzip", "x-gzip":
		gz, err := gzip.NewReader(counted)
		if err != nil {
			setupErr = err
			decoded = errorReader{err: err}
		} else {
			decoded, decoder = gz, gz
		}
	default:
		setupErr = fmt.Errorf("unsupported content encoding")
		decoded = errorReader{err: setupErr}
	}
	staged, err := f.raw.Stage(decoded, stream.EffectiveMaxBytes(f.cfg.HTTP))
	if decoder != nil {
		_ = decoder.Close()
	}
	if err != nil {
		return nil, err
	}
	if staged.TooLarge {
		return nil, ErrBodyTooLarge
	}
	defer f.raw.Abort(staged)
	body, err := os.ReadFile(staged.Path)
	if err != nil {
		return nil, fmt.Errorf("read staged response: %w", err)
	}
	completedWall := time.Now()
	duration := completedWall.Sub(started)
	completed := completedWall.UTC()
	meta := gtfsrt.Decode(body, stream.ExpectedKind, completed)
	if !protobufContentType(resp.Header.Get("Content-Type")) {
		meta.Flags = appendUnique(meta.Flags, "unexpected_content_type")
	}
	transportComplete := setupErr == nil && staged.ReadErr == nil
	if resp.ContentLength >= 0 && counted.n != resp.ContentLength {
		transportComplete = false
	}
	if !transportComplete {
		meta.Flags = appendUnique(meta.Flags, "transport_incomplete")
	}
	loc, err := stream.EffectiveLocation(source)
	if err != nil {
		return nil, err
	}
	id, err := model.NewID(completed)
	if err != nil {
		return nil, err
	}
	u := resp.Request.URL
	sanitized := url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}
	info := version.Current()
	capture := &model.Capture{
		FormatVersion: model.SidecarFormatVersion, ID: id, TickID: tick.ID,
		SourceID: source.ID, StreamID: stream.ID, ExpectedKind: stream.ExpectedKind,
		ScheduledAt: tick.ScheduledAt, StartedAt: started.UTC(), CompletedAt: completed,
		ArchiveDate: tick.ScheduledAt.In(loc).Format(time.DateOnly), Timezone: loc.String(),
		SanitizedURL: sanitized.String(), HTTPStatus: resp.StatusCode,
		ResponseHeaders: safeHeaders(resp.Header), DurationMS: duration.Milliseconds(),
		AttemptCount: attempt, AdvertisedLength: resp.ContentLength, EncodedLength: counted.n,
		ContentEncoding: encoding, TransportComplete: transportComplete,
		ParseStatus: meta.ParseStatus, ParseError: meta.ParseError, FeedVersion: meta.FeedVersion,
		Incrementality: meta.Incrementality, FeedTimestamp: meta.FeedTimestamp,
		EntityCount: meta.EntityCount, ValidationFlags: meta.Flags,
		ConfigFingerprint: f.fingerprint, ApplicationVersion: info.Version,
		ProtobufRevision: info.ProtobufRevision,
	}
	if err := f.raw.Commit(staged, capture, loc); err != nil {
		return nil, err
	}
	return capture, nil
}

func (f *Fetcher) finishFailure(ctx context.Context, result Result, category, detail string, status, attempts int) (Result, error) {
	now := time.Now().UTC()
	result.Tick.FinishedAt = &now
	result.Tick.Result = "failed"
	if category == "cancelled" {
		result.Tick.Result = "cancelled"
	}
	result.Tick.ErrorCategory = category
	result.Tick.ErrorDetail = detail
	result.Tick.HTTPStatus = status
	result.Tick.Attempts = attempts
	err := f.state.RecordTick(context.WithoutCancel(ctx), result.Tick)
	f.observer.ObserveFetch(result.Tick.SourceID, result.Tick.StreamID, result.Tick.Result, now.Sub(*result.Tick.StartedAt), 0)
	f.log.Warn("fetch failed", "source", result.Tick.SourceID, "stream", result.Tick.StreamID, "category", category, "status", status)
	if err != nil {
		return result, err
	}
	return result, fmt.Errorf("%s: %s", category, detail)
}

func resolveRequest(stream config.Stream) (*url.URL, map[string]string, error) {
	u, err := url.Parse(stream.URL)
	if err != nil {
		return nil, nil, err
	}
	query := u.Query()
	for name, ref := range stream.Auth.Query {
		value, err := ref.Resolve()
		if err != nil {
			return nil, nil, err
		}
		query.Set(name, value)
	}
	u.RawQuery = query.Encode()
	headers := map[string]string{}
	for name, ref := range stream.Auth.Headers {
		value, err := ref.Resolve()
		if err != nil {
			return nil, nil, err
		}
		if strings.ContainsAny(value, "\r\n") {
			return nil, nil, errors.New("secret header value contains a newline")
		}
		headers[name] = value
	}
	return u, headers, nil
}

func retryableStatus(status int) bool {
	return status == 408 || status == 425 || status == 429 || status >= 500
}
func retryableNetwork(err error) bool { return !errors.Is(err, context.Canceled) }

func classifyNetwork(err error) string {
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "dns"
	}
	var cert x509.UnknownAuthorityError
	if errors.As(err, &cert) {
		return "tls"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	var op *net.OpError
	if errors.As(err, &op) {
		return "connect"
	}
	return "network"
}

func waitBackoff(ctx context.Context, retry config.Retry, attempt int, override time.Duration) error {
	d := override
	if d <= 0 {
		d = retry.InitialBackoff.Duration << (attempt - 1)
		if d > retry.MaxBackoff.Duration {
			d = retry.MaxBackoff.Duration
		}
		if d > 0 {
			d = time.Duration(rand.Int64N(int64(d) + 1))
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func retryAfter(value string, maximum time.Duration) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		d := time.Duration(seconds) * time.Second
		if d <= maximum {
			return d
		}
		return maximum
	}
	if parsed, err := http.ParseTime(value); err == nil {
		d := time.Until(parsed)
		if d < 0 {
			return 0
		}
		if d > maximum {
			return maximum
		}
		return d
	}
	return 0
}

func safeHeaders(headers http.Header) map[string]string {
	out := map[string]string{}
	for _, name := range []string{"Content-Type", "Content-Length", "ETag", "Last-Modified", "Date"} {
		if value := headers.Get(name); value != "" {
			out[name] = value
		}
	}
	return out
}

func protobufContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return false
	}
	switch strings.ToLower(mediaType) {
	case "application/x-protobuf", "application/protobuf", "application/octet-stream", "application/vnd.google.protobuf":
		return true
	default:
		return false
	}
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

type countingReader struct {
	reader io.Reader
	n      int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.n += int64(n)
	return n, err
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }
