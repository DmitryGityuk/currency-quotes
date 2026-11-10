package config

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
)

// env returns a getenv function backed by a map.
func env(vars map[string]string) func(string) string {
	return func(key string) string { return vars[key] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(nil))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := Default(); !reflect.DeepEqual(cfg, want) {
		t.Errorf("Load() = %+v, want %+v", cfg, want)
	}
	if cfg.Currencies.String() != "EUR, MXN, USD" {
		t.Errorf("default currencies = %s", cfg.Currencies)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"HTTP_ADDR":               "127.0.0.1:9000",
		"SHUTDOWN_TIMEOUT":        "3s",
		"LOG_LEVEL":               "debug",
		"LOG_FORMAT":              "TEXT",
		"DATABASE_URL":            "postgresql://u:p@db:5432/q",
		"KAFKA_BROKERS":           "kafka-1:9092, kafka-2:9092,",
		"KAFKA_GROUP":             "g",
		"SUPPORTED_CURRENCIES":    "usd,eur",
		"RATES_PROVIDER":          "Fake",
		"FRANKFURTER_URL":         "http://rates.local:8080",
		"PROVIDER_TIMEOUT":        "2s",
		"FAKE_RATES_DELAY":        "0s",
		"FAKE_RATES_FAILURE_RATE": "0.3",
		"MAX_ATTEMPTS":            "5",
		"RETRY_BASE_DELAY":        "100ms",
		"RETRY_MAX_DELAY":         "1s",
		"RELAY_INTERVAL":          "50ms",
		"RELAY_BATCH_SIZE":        "10",
		"IDEMPOTENCY_KEY_TTL":     "1h",
	}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	want := Config{
		HTTPAddr:          "127.0.0.1:9000",
		ShutdownTimeout:   3 * time.Second,
		LogLevel:          slog.LevelDebug,
		LogFormat:         LogFormatText,
		DatabaseURL:       "postgresql://u:p@db:5432/q",
		KafkaBrokers:      []string{"kafka-1:9092", "kafka-2:9092"},
		KafkaGroup:        "g",
		Currencies:        quotes.MustCurrencies("EUR", "USD"),
		RatesProvider:     ProviderFake,
		FrankfurterURL:    "http://rates.local:8080",
		ProviderTimeout:   2 * time.Second,
		FakeDelay:         0,
		FakeFailureRate:   0.3,
		MaxAttempts:       5,
		RetryBaseDelay:    100 * time.Millisecond,
		RetryMaxDelay:     time.Second,
		RelayInterval:     50 * time.Millisecond,
		RelayBatchSize:    10,
		IdempotencyKeyTTL: time.Hour,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("Load() = %+v\nwant     %+v", cfg, want)
	}
}

func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name    string
		vars    map[string]string
		wantErr []string // every substring must appear in the error
		notIn   string   // must not appear in the error
	}{
		{name: "address without port", vars: map[string]string{"HTTP_ADDR": "8080"}, wantErr: []string{"HTTP_ADDR"}},
		{name: "unparsable timeout", vars: map[string]string{"SHUTDOWN_TIMEOUT": "soon"}, wantErr: []string{"SHUTDOWN_TIMEOUT"}},
		{name: "negative timeout", vars: map[string]string{"SHUTDOWN_TIMEOUT": "-1s"}, wantErr: []string{"SHUTDOWN_TIMEOUT", "must be positive"}},
		{name: "unknown log level", vars: map[string]string{"LOG_LEVEL": "loud"}, wantErr: []string{"LOG_LEVEL"}},
		{name: "unknown log format", vars: map[string]string{"LOG_FORMAT": "xml"}, wantErr: []string{"LOG_FORMAT", `"xml"`}},
		{name: "database url with another scheme", vars: map[string]string{"DATABASE_URL": "mysql://u@h/db"}, wantErr: []string{"DATABASE_URL", "mysql"}},
		{
			name:    "broken database url does not leak the password",
			vars:    map[string]string{"DATABASE_URL": "postgres://u:s3cret@host:port/db"},
			wantErr: []string{"DATABASE_URL"}, notIn: "s3cret",
		},
		{name: "broker without port", vars: map[string]string{"KAFKA_BROKERS": "kafka"}, wantErr: []string{"KAFKA_BROKERS", "kafka"}},
		{name: "bad currency", vars: map[string]string{"SUPPORTED_CURRENCIES": "USD,EURO"}, wantErr: []string{"SUPPORTED_CURRENCIES", "EURO"}},
		{name: "only separators", vars: map[string]string{"SUPPORTED_CURRENCIES": " , "}, wantErr: []string{"SUPPORTED_CURRENCIES"}},
		{name: "unknown provider", vars: map[string]string{"RATES_PROVIDER": "ecb"}, wantErr: []string{"RATES_PROVIDER", `"ecb"`}},
		{name: "frankfurter url without scheme", vars: map[string]string{"FRANKFURTER_URL": "api.frankfurter.dev"}, wantErr: []string{"FRANKFURTER_URL"}},
		{name: "negative fake delay", vars: map[string]string{"FAKE_RATES_DELAY": "-1s"}, wantErr: []string{"FAKE_RATES_DELAY"}},
		{name: "failure rate above 1", vars: map[string]string{"FAKE_RATES_FAILURE_RATE": "1.5"}, wantErr: []string{"FAKE_RATES_FAILURE_RATE"}},
		{name: "zero attempts", vars: map[string]string{"MAX_ATTEMPTS": "0"}, wantErr: []string{"MAX_ATTEMPTS"}},
		{name: "attempts not a number", vars: map[string]string{"MAX_ATTEMPTS": "many"}, wantErr: []string{"MAX_ATTEMPTS"}},
		{
			name:    "base delay above max delay",
			vars:    map[string]string{"RETRY_BASE_DELAY": "10s", "RETRY_MAX_DELAY": "1s"},
			wantErr: []string{"RETRY_BASE_DELAY", "RETRY_MAX_DELAY"},
		},
		{name: "zero batch", vars: map[string]string{"RELAY_BATCH_SIZE": "0"}, wantErr: []string{"RELAY_BATCH_SIZE"}},
		{
			name:    "all errors are reported together",
			vars:    map[string]string{"SHUTDOWN_TIMEOUT": "soon", "LOG_FORMAT": "xml", "MAX_ATTEMPTS": "0"},
			wantErr: []string{"SHUTDOWN_TIMEOUT", "LOG_FORMAT", "MAX_ATTEMPTS"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := Load(env(tt.vars))
			if err == nil {
				t.Fatalf("Load() error = nil, want error containing %q", tt.wantErr)
			}
			for _, s := range tt.wantErr {
				if !strings.Contains(err.Error(), s) {
					t.Errorf("error %q does not contain %q", err, s)
				}
			}
			if tt.notIn != "" && strings.Contains(err.Error(), tt.notIn) {
				t.Errorf("error %q leaks %q", err, tt.notIn)
			}
			if !reflect.DeepEqual(cfg, Config{}) {
				t.Errorf("Load() returned a partial config %+v alongside an error", cfg)
			}
		})
	}
}
