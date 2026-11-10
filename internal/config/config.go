// Package config reads service settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
)

// LogFormat selects how log records are written.
type LogFormat string

const (
	// LogFormatJSON writes one JSON object per line; used in containers.
	LogFormatJSON LogFormat = "json"
	// LogFormatText writes key=value pairs; easier to read in a terminal.
	LogFormatText LogFormat = "text"
)

// Rates providers.
const (
	// ProviderFrankfurter is the real external API.
	ProviderFrankfurter = "frankfurter"
	// ProviderFake generates rates locally: for demos without internet.
	ProviderFake = "fake"
)

// Config holds the settings of every mode of the service. Defaults suit
// running on the host against `make infra`; compose.yaml overrides the
// addresses for containers.
type Config struct {
	// HTTPAddr is the listen address of the HTTP server (HTTP_ADDR).
	HTTPAddr string
	// ShutdownTimeout bounds graceful shutdown (SHUTDOWN_TIMEOUT).
	ShutdownTimeout time.Duration
	// LogLevel is the minimum level written to the log (LOG_LEVEL).
	LogLevel slog.Level
	// LogFormat is json or text (LOG_FORMAT).
	LogFormat LogFormat

	// DatabaseURL is the PostgreSQL connection string (DATABASE_URL).
	DatabaseURL string
	// KafkaBrokers are the bootstrap brokers (KAFKA_BROKERS, comma-separated).
	KafkaBrokers []string
	// KafkaGroup is the consumer group of the workers (KAFKA_GROUP).
	KafkaGroup string

	// Currencies are the supported currencies (SUPPORTED_CURRENCIES).
	Currencies quotes.Currencies
	// RatesProvider is frankfurter or fake (RATES_PROVIDER).
	RatesProvider string
	// FrankfurterURL is the Frankfurter instance (FRANKFURTER_URL).
	FrankfurterURL string
	// ProviderTimeout bounds one call to the provider (PROVIDER_TIMEOUT).
	ProviderTimeout time.Duration
	// FakeDelay is the latency of the fake provider (FAKE_RATES_DELAY).
	FakeDelay time.Duration
	// FakeFailureRate is the share of failing fake calls, 0..1 (FAKE_RATES_FAILURE_RATE).
	FakeFailureRate float64

	// MaxAttempts is how many times the provider is asked (MAX_ATTEMPTS).
	MaxAttempts int
	// RetryBaseDelay is the pause before the second attempt (RETRY_BASE_DELAY).
	RetryBaseDelay time.Duration
	// RetryMaxDelay caps any pause between attempts (RETRY_MAX_DELAY).
	RetryMaxDelay time.Duration

	// RelayInterval is how often the outbox is polled (RELAY_INTERVAL).
	RelayInterval time.Duration
	// RelayBatchSize is how many outbox rows one poll publishes (RELAY_BATCH_SIZE).
	RelayBatchSize int
	// IdempotencyKeyTTL is how long Idempotency-Key values are kept (IDEMPOTENCY_KEY_TTL).
	IdempotencyKeyTTL time.Duration
	// RequeueAfter is how long an update may stay unfinished without a
	// message in the outbox before the worker sends it again (REQUEUE_AFTER).
	RequeueAfter time.Duration
}

// localDatabaseURL points at the PostgreSQL that `make infra` starts.
const localDatabaseURL = "postgres://quotes:quotes@localhost:55432/quotes?sslmode=disable" //nolint:gosec // G101: the local compose database, not a secret

// Default returns the configuration used when no variables are set.
func Default() Config {
	return Config{
		HTTPAddr:        ":8080",
		ShutdownTimeout: 15 * time.Second,
		LogLevel:        slog.LevelInfo,
		LogFormat:       LogFormatJSON,

		DatabaseURL:  localDatabaseURL,
		KafkaBrokers: []string{"localhost:59094"},
		KafkaGroup:   "currency-quotes-worker",

		Currencies:      quotes.MustCurrencies("USD", "EUR", "MXN"),
		RatesProvider:   ProviderFrankfurter,
		FrankfurterURL:  "https://api.frankfurter.dev",
		ProviderTimeout: 5 * time.Second,
		FakeDelay:       500 * time.Millisecond,

		MaxAttempts:    3,
		RetryBaseDelay: 500 * time.Millisecond,
		RetryMaxDelay:  5 * time.Second,

		RelayInterval:     200 * time.Millisecond,
		RelayBatchSize:    100,
		IdempotencyKeyTTL: 24 * time.Hour,
		// Well above the longest normal processing: 3 attempts of up to 5s
		// each plus up to 5s between them.
		RequeueAfter: 2 * time.Minute,
	}
}

// Load builds a Config from environment variables, falling back to defaults
// for unset ones. getenv is usually os.Getenv; tests pass a map lookup.
// All invalid values are reported at once, not just the first one.
func Load(getenv func(string) string) (Config, error) {
	cfg := Default()
	l := loader{getenv: getenv}

	l.parse("HTTP_ADDR", func(v string) error {
		if _, _, err := net.SplitHostPort(v); err != nil {
			return err
		}
		cfg.HTTPAddr = v
		return nil
	})
	l.duration("SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout)
	l.parse("LOG_LEVEL", func(v string) error { return cfg.LogLevel.UnmarshalText([]byte(v)) })
	l.parse("LOG_FORMAT", func(v string) error {
		switch f := LogFormat(strings.ToLower(v)); f {
		case LogFormatJSON, LogFormatText:
			cfg.LogFormat = f
			return nil
		default:
			return fmt.Errorf("want json or text, got %q", v)
		}
	})

	l.parse("DATABASE_URL", func(v string) error {
		u, err := url.Parse(v)
		if err != nil {
			return errors.New("not a URL") // the error text would contain the password
		}
		if u.Scheme != "postgres" && u.Scheme != "postgresql" {
			return fmt.Errorf("want a postgres:// URL, got scheme %q", u.Scheme)
		}
		cfg.DatabaseURL = v
		return nil
	})
	l.parse("KAFKA_BROKERS", func(v string) error {
		brokers := splitList(v)
		for _, b := range brokers {
			if _, _, err := net.SplitHostPort(b); err != nil {
				return fmt.Errorf("broker %q: %w", b, err)
			}
		}
		cfg.KafkaBrokers = brokers
		return nil
	})
	l.parse("KAFKA_GROUP", func(v string) error { cfg.KafkaGroup = v; return nil })

	l.parse("SUPPORTED_CURRENCIES", func(v string) error {
		cs, err := quotes.NewCurrencies(splitList(v)...)
		if err != nil {
			return err
		}
		cfg.Currencies = cs
		return nil
	})
	l.parse("RATES_PROVIDER", func(v string) error {
		switch p := strings.ToLower(v); p {
		case ProviderFrankfurter, ProviderFake:
			cfg.RatesProvider = p
			return nil
		default:
			return fmt.Errorf("want %s or %s, got %q", ProviderFrankfurter, ProviderFake, v)
		}
	})
	l.parse("FRANKFURTER_URL", func(v string) error {
		u, err := url.Parse(v)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("want an http(s) URL, got %q", v)
		}
		cfg.FrankfurterURL = v
		return nil
	})
	l.duration("PROVIDER_TIMEOUT", &cfg.ProviderTimeout)
	l.parse("FAKE_RATES_DELAY", func(v string) error {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return fmt.Errorf("want a duration of zero or more, got %q", v)
		}
		cfg.FakeDelay = d
		return nil
	})
	l.parse("FAKE_RATES_FAILURE_RATE", func(v string) error {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 || f > 1 {
			return fmt.Errorf("want a number from 0 to 1, got %q", v)
		}
		cfg.FakeFailureRate = f
		return nil
	})

	l.positiveInt("MAX_ATTEMPTS", &cfg.MaxAttempts)
	l.duration("RETRY_BASE_DELAY", &cfg.RetryBaseDelay)
	l.duration("RETRY_MAX_DELAY", &cfg.RetryMaxDelay)
	if cfg.RetryBaseDelay > cfg.RetryMaxDelay {
		l.errs = append(l.errs, fmt.Errorf("RETRY_BASE_DELAY (%s) must not exceed RETRY_MAX_DELAY (%s)", cfg.RetryBaseDelay, cfg.RetryMaxDelay))
	}

	l.duration("RELAY_INTERVAL", &cfg.RelayInterval)
	l.positiveInt("RELAY_BATCH_SIZE", &cfg.RelayBatchSize)
	l.duration("IDEMPOTENCY_KEY_TTL", &cfg.IdempotencyKeyTTL)
	l.duration("REQUEUE_AFTER", &cfg.RequeueAfter)

	if err := errors.Join(l.errs...); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// loader parses variables and collects every error.
type loader struct {
	getenv func(string) string
	errs   []error
}

// parse calls set with the value of name if it is set, and records its error.
func (l *loader) parse(name string, set func(string) error) {
	v := l.getenv(name)
	if v == "" {
		return
	}
	if err := set(v); err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", name, err))
	}
}

// duration parses a positive duration.
func (l *loader) duration(name string, dst *time.Duration) {
	l.parse(name, func(v string) error {
		d, err := time.ParseDuration(v)
		if err != nil {
			return err
		}
		if d <= 0 {
			return fmt.Errorf("must be positive, got %s", d)
		}
		*dst = d
		return nil
	})
}

// positiveInt parses an integer greater than zero.
func (l *loader) positiveInt(name string, dst *int) {
	l.parse(name, func(v string) error {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return fmt.Errorf("want a positive integer, got %q", v)
		}
		*dst = n
		return nil
	})
}

// splitList splits "a, b,,c" into [a b c].
func splitList(v string) []string {
	var out []string
	for part := range strings.SplitSeq(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
