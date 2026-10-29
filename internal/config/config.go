package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const CurrentVersion = 1

var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

func ValidID(value string) bool { return idPattern.MatchString(value) }

// SanitizedStreamURL is the single URL-sanitization rule of the archive: it
// returns scheme, host, and path with userinfo, query, and fragment removed.
// Capture metadata, the Parquet feed_url column, and the publication
// base64url partition key all derive from this function.
func SanitizedStreamURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse stream URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("stream URL %q lacks scheme or host", rawURL)
	}
	sanitized := url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}
	return sanitized.String(), nil
}

type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration must be a string")
	}
	v, err := parseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", node.Value, err)
	}
	d.Duration = v
	return nil
}

var wholeDaysPattern = regexp.MustCompile(`^([0-9]+)d$`)

func parseDuration(value string) (time.Duration, error) {
	if match := wholeDaysPattern.FindStringSubmatch(value); match != nil {
		days, err := strconv.ParseUint(match[1], 10, 64)
		const day = uint64(24 * time.Hour)
		if err != nil || days > uint64(1<<63-1)/day {
			return 0, errors.New("day duration overflows time.Duration")
		}
		return time.Duration(days * day), nil
	}
	return time.ParseDuration(value)
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

type Config struct {
	Version      int           `yaml:"version" json:"version"`
	Storage      Storage       `yaml:"storage" json:"storage"`
	Runtime      Runtime       `yaml:"runtime" json:"runtime"`
	HTTP         HTTP          `yaml:"http" json:"http"`
	Parquet      Parquet       `yaml:"parquet" json:"parquet"`
	Rclone       Rclone        `yaml:"rclone" json:"rclone"`
	Sources      []Source      `yaml:"sources" json:"sources"`
	Destinations []Destination `yaml:"destinations" json:"destinations"`
}

type Storage struct {
	Root                        string   `yaml:"root" json:"root"`
	StateDB                     string   `yaml:"state_db" json:"state_db"`
	CloseDelay                  Duration `yaml:"close_delay" json:"close_delay"`
	RawRetentionAfterUpload     Duration `yaml:"raw_retention_after_upload" json:"raw_retention_after_upload"`
	ParquetRetentionAfterUpload Duration `yaml:"parquet_retention_after_upload" json:"parquet_retention_after_upload"`
	MetadataRetention           Duration `yaml:"metadata_retention" json:"metadata_retention"`
	AllowLocalOnlyCleanup       bool     `yaml:"allow_local_only_cleanup" json:"allow_local_only_cleanup"`
	PauseFetchingAtPercent      float64  `yaml:"pause_fetching_at_percent" json:"pause_fetching_at_percent"`
	ResumeFetchingAtPercent     float64  `yaml:"resume_fetching_at_percent" json:"resume_fetching_at_percent"`
}

type Runtime struct {
	HTTPAddress           string   `yaml:"http_address" json:"http_address"`
	ShutdownTimeout       Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`
	FetchConcurrency      int      `yaml:"fetch_concurrency" json:"fetch_concurrency"`
	PerHostConcurrency    int      `yaml:"per_host_concurrency" json:"per_host_concurrency"`
	CompactionConcurrency int      `yaml:"compaction_concurrency" json:"compaction_concurrency"`
	UploadConcurrency     int      `yaml:"upload_concurrency" json:"upload_concurrency"`
}

type HTTP struct {
	UserAgent        string   `yaml:"user_agent" json:"user_agent"`
	RequestTimeout   Duration `yaml:"request_timeout" json:"request_timeout"`
	MaxResponseBytes int64    `yaml:"max_response_bytes" json:"max_response_bytes"`
	MaxStartLateness Duration `yaml:"max_start_lateness" json:"max_start_lateness"`
	AllowHTTP        bool     `yaml:"allow_http" json:"allow_http,omitempty"`
	Retry            Retry    `yaml:"retry" json:"retry"`
}

type Retry struct {
	Attempts       int      `yaml:"attempts" json:"attempts"`
	InitialBackoff Duration `yaml:"initial_backoff" json:"initial_backoff"`
	MaxBackoff     Duration `yaml:"max_backoff" json:"max_backoff"`
}

type Parquet struct {
	Compression         string `yaml:"compression" json:"compression"`
	TargetRowGroupBytes int64  `yaml:"target_row_group_bytes" json:"target_row_group_bytes"`
}

type Rclone struct {
	Binary         string   `yaml:"binary" json:"binary"`
	ConfigFile     string   `yaml:"config_file" json:"config_file"`
	ProcessTimeout Duration `yaml:"process_timeout" json:"process_timeout"`
}

type Source struct {
	ID                  string         `yaml:"id" json:"id"`
	Timezone            string         `yaml:"timezone" json:"timezone"`
	RegistryID          string         `yaml:"registry_id" json:"registry_id,omitempty"`
	LicenseURL          string         `yaml:"license_url" json:"license_url,omitempty"`
	AttributionTextFile string         `yaml:"attribution_text_file" json:"attribution_text_file,omitempty"`
	Streams             []Stream       `yaml:"streams" json:"streams"`
	Location            *time.Location `yaml:"-" json:"-"`
}

type Stream struct {
	ID               string            `yaml:"id" json:"id"`
	ExpectedKind     string            `yaml:"expected_kind" json:"expected_kind"`
	URL              string            `yaml:"url" json:"url"`
	Interval         Duration          `yaml:"interval" json:"interval"`
	Timezone         string            `yaml:"timezone" json:"timezone,omitempty"`
	Headers          map[string]string `yaml:"headers" json:"headers,omitempty"`
	Auth             Auth              `yaml:"auth" json:"auth"`
	Enabled          *bool             `yaml:"enabled" json:"enabled,omitempty"`
	RequestTimeout   Duration          `yaml:"request_timeout" json:"request_timeout,omitempty"`
	MaxResponseBytes int64             `yaml:"max_response_bytes" json:"max_response_bytes,omitempty"`
	MaxStartLateness Duration          `yaml:"max_start_lateness" json:"max_start_lateness,omitempty"`
}

type Auth struct {
	Query   map[string]SecretRef `yaml:"query" json:"query,omitempty"`
	Headers map[string]SecretRef `yaml:"headers" json:"headers,omitempty"`
}

type SecretRef struct {
	Env  string `yaml:"env" json:"env,omitempty"`
	File string `yaml:"file" json:"file,omitempty"`
}

type Destination struct {
	ID       string `yaml:"id" json:"id"`
	Remote   string `yaml:"remote" json:"remote"`
	Required *bool  `yaml:"required" json:"required,omitempty"`
}

func Defaults() Config {
	return Config{
		Version: CurrentVersion,
		Storage: Storage{
			Root:                        "/data",
			CloseDelay:                  Duration{15 * time.Minute},
			RawRetentionAfterUpload:     Duration{7 * 24 * time.Hour},
			ParquetRetentionAfterUpload: Duration{7 * 24 * time.Hour},
			MetadataRetention:           Duration{30 * 24 * time.Hour},
			PauseFetchingAtPercent:      90,
			ResumeFetchingAtPercent:     80,
		},
		Runtime: Runtime{
			HTTPAddress:           ":8080",
			ShutdownTimeout:       Duration{60 * time.Second},
			FetchConcurrency:      16,
			PerHostConcurrency:    4,
			CompactionConcurrency: 1,
			UploadConcurrency:     2,
		},
		HTTP: HTTP{
			UserAgent:        "gtfs-rt-archiver/dev",
			RequestTimeout:   Duration{20 * time.Second},
			MaxResponseBytes: 32 << 20,
			MaxStartLateness: Duration{5 * time.Second},
			Retry: Retry{
				Attempts:       2,
				InitialBackoff: Duration{time.Second},
				MaxBackoff:     Duration{5 * time.Second},
			},
		},
		Parquet: Parquet{Compression: "zstd", TargetRowGroupBytes: 128 << 20},
		Rclone:  Rclone{Binary: "rclone", ProcessTimeout: Duration{2 * time.Hour}},
	}
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := Defaults()
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("decode config: multiple YAML documents are not allowed")
		}
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if cfg.Storage.StateDB == "" {
		cfg.Storage.StateDB = filepath.Join(cfg.Storage.Root, "state.sqlite")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) Validate() error {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	if c.Version != CurrentVersion {
		add("version must be %d", CurrentVersion)
	}
	if !filepath.IsAbs(c.Storage.Root) || !filepath.IsAbs(c.Storage.StateDB) {
		add("storage.root and storage.state_db must be absolute paths")
	}
	cleanRoot := filepath.Clean(c.Storage.Root)
	if cleanRoot == string(filepath.Separator) {
		add("storage.root cannot be the filesystem root")
	}
	if rel, err := filepath.Rel(cleanRoot, filepath.Clean(c.Storage.StateDB)); err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		add("storage.state_db must be beneath storage.root")
	}
	if c.Storage.CloseDelay.Duration < 0 || c.Storage.RawRetentionAfterUpload.Duration < 0 || c.Storage.ParquetRetentionAfterUpload.Duration < 0 || c.Storage.MetadataRetention.Duration < 0 {
		add("storage durations cannot be negative")
	}
	if c.Storage.ResumeFetchingAtPercent <= 0 || c.Storage.PauseFetchingAtPercent > 100 || c.Storage.ResumeFetchingAtPercent >= c.Storage.PauseFetchingAtPercent {
		add("storage watermarks must satisfy 0 < resume < pause <= 100")
	}
	if c.Runtime.FetchConcurrency < 1 || c.Runtime.PerHostConcurrency < 1 || c.Runtime.CompactionConcurrency < 1 || c.Runtime.UploadConcurrency < 1 {
		add("runtime concurrency values must be positive")
	}
	if c.Runtime.ShutdownTimeout.Duration <= 0 {
		add("runtime.shutdown_timeout must be positive")
	}
	if c.Runtime.HTTPAddress == "" {
		add("runtime.http_address is required")
	}
	if c.HTTP.RequestTimeout.Duration <= 0 || c.HTTP.MaxResponseBytes <= 0 || c.HTTP.MaxStartLateness.Duration <= 0 {
		add("http timeout, size, and max_start_lateness must be positive")
	}
	if c.HTTP.UserAgent == "" || strings.ContainsAny(c.HTTP.UserAgent, "\r\n") {
		add("http.user_agent is required and cannot contain newlines")
	}
	if c.HTTP.Retry.Attempts < 1 || c.HTTP.Retry.InitialBackoff.Duration < 0 || c.HTTP.Retry.MaxBackoff.Duration < c.HTTP.Retry.InitialBackoff.Duration {
		add("invalid http.retry settings")
	}
	if c.Parquet.Compression != "zstd" && c.Parquet.Compression != "snappy" && c.Parquet.Compression != "uncompressed" {
		add("parquet.compression must be zstd, snappy, or uncompressed")
	}
	if c.Parquet.TargetRowGroupBytes <= 0 {
		add("parquet.target_row_group_bytes must be positive")
	}
	if c.Rclone.Binary == "" || c.Rclone.ProcessTimeout.Duration <= 0 {
		add("rclone.binary and a positive rclone.process_timeout are required")
	}
	if c.Rclone.ConfigFile != "" && !filepath.IsAbs(c.Rclone.ConfigFile) {
		add("rclone.config_file must be an absolute path")
	}

	seenSources := map[string]bool{}
	seenStreams := map[string]bool{}
	for si := range c.Sources {
		source := &c.Sources[si]
		if !ValidID(source.ID) {
			add("source %q has an unsafe id", source.ID)
		}
		if seenSources[source.ID] {
			add("duplicate source id %q", source.ID)
		}
		seenSources[source.ID] = true
		loc, err := time.LoadLocation(source.Timezone)
		if err != nil {
			add("source %q timezone: %v", source.ID, err)
		} else {
			source.Location = loc
		}
		if len(source.Streams) == 0 {
			add("source %q must contain at least one stream", source.ID)
		}
		if source.AttributionTextFile != "" && !filepath.IsAbs(source.AttributionTextFile) {
			add("source %q attribution_text_file must be absolute", source.ID)
		}
		if source.LicenseURL != "" {
			u, err := url.Parse(source.LicenseURL)
			if err != nil || u.Scheme != "https" || u.Host == "" {
				add("source %q license_url must be an absolute HTTPS URL", source.ID)
			}
		}
		for i := range source.Streams {
			stream := &source.Streams[i]
			key := source.ID + "/" + stream.ID
			if !ValidID(stream.ID) {
				add("stream %q has an unsafe id", key)
			}
			if seenStreams[key] {
				add("duplicate stream id %q", key)
			}
			seenStreams[key] = true
			if stream.Interval.Duration < time.Second {
				add("stream %q interval must be at least 1s", key)
			}
			if stream.RequestTimeout.Duration < 0 || stream.MaxResponseBytes < 0 || stream.MaxStartLateness.Duration < 0 {
				add("stream %q timeout, size, and lateness overrides cannot be negative", key)
			}
			if !validKind(stream.ExpectedKind) {
				add("stream %q has invalid expected_kind %q", key, stream.ExpectedKind)
			}
			u, err := url.Parse(stream.URL)
			schemeOK := u.Scheme == "https" || (c.HTTP.AllowHTTP && u.Scheme == "http")
			if err != nil || !schemeOK || u.Host == "" || u.User != nil || u.Fragment != "" {
				if c.HTTP.AllowHTTP {
					add("stream %q must have an HTTPS or HTTP URL without userinfo or fragment", key)
				} else {
					add("stream %q must have an HTTPS URL without userinfo or fragment", key)
				}
			}
			if u != nil {
				for name := range u.Query() {
					if sensitiveName(name) {
						add("stream %q places a likely secret query parameter %q directly in its URL", key, name)
					}
				}
			}
			for h := range stream.Headers {
				if !validHeader(h) {
					add("stream %q has forbidden header %q", key, h)
				}
				if sensitiveName(h) {
					add("stream %q must put sensitive header %q under auth.headers", key, h)
				}
				if strings.ContainsAny(stream.Headers[h], "\r\n") {
					add("stream %q header %q contains a newline", key, h)
				}
			}
			for h, ref := range stream.Auth.Headers {
				if !validHeader(h) {
					add("stream %q has forbidden secret header %q", key, h)
				}
				if err := ref.Validate(); err != nil {
					add("stream %q secret header %q: %v", key, h, err)
				}
			}
			for name, ref := range stream.Auth.Query {
				if name == "" || strings.ContainsAny(name, "\r\n\x00") {
					add("stream %q has an invalid auth query key", key)
				}
				if err := ref.Validate(); err != nil {
					add("stream %q auth query %q: %v", key, name, err)
				}
			}
			if stream.Timezone != "" {
				if _, err := time.LoadLocation(stream.Timezone); err != nil {
					add("stream %q timezone: %v", key, err)
				}
			}
		}
	}
	if len(c.Sources) == 0 {
		add("at least one source is required")
	}
	seenDest := map[string]bool{}
	for _, dest := range c.Destinations {
		if !ValidID(dest.ID) || seenDest[dest.ID] {
			add("destination %q has an unsafe or duplicate id", dest.ID)
		}
		seenDest[dest.ID] = true
		if dest.Remote == "" || strings.ContainsAny(dest.Remote, "\r\n\x00") {
			add("destination %q has an invalid remote", dest.ID)
		}
		if !regexp.MustCompile(`^[A-Za-z0-9_-]+:`).MatchString(dest.Remote) {
			add("destination %q must use a named rclone remote", dest.ID)
		}
	}
	if len(c.Destinations) > 0 && c.Rclone.ConfigFile == "" {
		add("rclone.config_file is required when destinations are configured")
	}
	if len(problems) != 0 {
		sort.Strings(problems)
		return errors.New("invalid configuration:\n - " + strings.Join(problems, "\n - "))
	}
	return nil
}

func validKind(v string) bool {
	switch v {
	case "vehicle_position", "trip_update", "alert", "mixed", "auto":
		return true
	default:
		return false
	}
}

func validHeader(name string) bool {
	if !httpgutsValidHeaderName(name) {
		return false
	}
	switch strings.ToLower(name) {
	case "connection", "proxy-connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "host", "content-length":
		return false
	default:
		return true
	}
}

func sensitiveName(name string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(name, "-", "_"), ".", "_"))
	switch normalized {
	case "authorization", "proxy_authorization", "cookie", "x_api_key", "apikey", "api_key", "access_token", "token", "key":
		return true
	default:
		return strings.Contains(normalized, "secret") || strings.Contains(normalized, "password")
	}
}

func httpgutsValidHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c))) {
			return false
		}
	}
	return true
}

func (s Stream) IsEnabled() bool { return s.Enabled == nil || *s.Enabled }

func (s Stream) EffectiveTimeout(global HTTP) time.Duration {
	if s.RequestTimeout.Duration > 0 {
		return s.RequestTimeout.Duration
	}
	return global.RequestTimeout.Duration
}

func (s Stream) EffectiveMaxBytes(global HTTP) int64 {
	if s.MaxResponseBytes > 0 {
		return s.MaxResponseBytes
	}
	return global.MaxResponseBytes
}

func (s Stream) EffectiveLateness(global HTTP) time.Duration {
	if s.MaxStartLateness.Duration > 0 {
		return s.MaxStartLateness.Duration
	}
	return global.MaxStartLateness.Duration
}

func (s Stream) EffectiveLocation(source Source) (*time.Location, error) {
	if s.Timezone == "" {
		return source.Location, nil
	}
	return time.LoadLocation(s.Timezone)
}

func (d Destination) IsRequired() bool { return d.Required == nil || *d.Required }

func (r SecretRef) Validate() error {
	if (r.Env == "") == (r.File == "") {
		return errors.New("exactly one of env or file is required")
	}
	if r.Env != "" && !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(r.Env) {
		return errors.New("invalid environment variable name")
	}
	if r.File != "" && !filepath.IsAbs(r.File) {
		return errors.New("secret file path must be absolute")
	}
	return nil
}

func (r SecretRef) Resolve() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	if r.Env != "" {
		v, ok := os.LookupEnv(r.Env)
		if !ok || v == "" {
			return "", fmt.Errorf("environment variable %s is empty or unset", r.Env)
		}
		return v, nil
	}
	b, err := os.ReadFile(r.File)
	if err != nil {
		return "", fmt.Errorf("read secret file: %w", err)
	}
	v := strings.TrimRight(string(b), "\r\n")
	if v == "" {
		return "", errors.New("secret file is empty")
	}
	return v, nil
}

func (c *Config) ResolveSecrets() error {
	for _, source := range c.Sources {
		if source.AttributionTextFile != "" {
			b, err := os.ReadFile(source.AttributionTextFile)
			if err != nil {
				return fmt.Errorf("source %s attribution text: %w", source.ID, err)
			}
			if strings.TrimSpace(string(b)) == "" {
				return fmt.Errorf("source %s attribution text is empty", source.ID)
			}
		}
		for _, stream := range source.Streams {
			for name, ref := range stream.Auth.Query {
				if _, err := ref.Resolve(); err != nil {
					return fmt.Errorf("source %s stream %s query secret %s: %w", source.ID, stream.ID, name, err)
				}
			}
			for name, ref := range stream.Auth.Headers {
				value, err := ref.Resolve()
				if err != nil {
					return fmt.Errorf("source %s stream %s header secret %s: %w", source.ID, stream.ID, name, err)
				}
				if strings.ContainsAny(value, "\r\n") {
					return fmt.Errorf("source %s stream %s header secret %s contains a newline", source.ID, stream.ID, name)
				}
			}
		}
	}
	return nil
}

func (c *Config) Fingerprint() string {
	// Config contains secret references, never resolved secret values. Hash the
	// complete effective configuration so operational and request overrides are
	// reflected without exposing their contents.
	b, _ := json.Marshal(c)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (c *Config) FindSourceStream(sourceID, streamID string) (*Source, *Stream, error) {
	var matches [][2]int
	for si := range c.Sources {
		for ti := range c.Sources[si].Streams {
			if (sourceID == "" || c.Sources[si].ID == sourceID) && (streamID == "" || c.Sources[si].Streams[ti].ID == streamID) {
				matches = append(matches, [2]int{si, ti})
			}
		}
	}
	if len(matches) == 0 {
		return nil, nil, errors.New("no matching stream")
	}
	if len(matches) > 1 {
		return nil, nil, errors.New("selection matches multiple streams; specify --source and --stream")
	}
	m := matches[0]
	return &c.Sources[m[0]], &c.Sources[m[0]].Streams[m[1]], nil
}
