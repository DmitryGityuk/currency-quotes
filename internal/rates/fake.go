package rates

import (
	"context"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
)

// usdValue is roughly how many US dollars one unit of a currency is worth.
// The fake derives every cross rate from it, so EUR/MXN and MXN/EUR agree.
var usdValue = map[quotes.Currency]float64{
	"USD": 1,
	"EUR": 1.16,
	"MXN": 1 / 18.4,
}

// Fake returns plausible rates without network access: for demos without
// internet, for tests and for showing how failures are handled. It is not a
// money calculation, so floating point is fine here; the result leaves as text.
type Fake struct {
	// Delay simulates the provider's latency, so the asynchronous flow is
	// visible: an update stays in processing for this long.
	Delay time.Duration
	// FailureRate is the share of calls, from 0 to 1, that fail with a
	// temporary error, to demonstrate retries.
	FailureRate float64
	// Now returns the current time; nil means time.Now.
	Now func() time.Time
}

// Rate implements Provider.
func (f *Fake) Rate(ctx context.Context, pair quotes.Pair) (Rate, error) {
	const name = "fake"
	if f.Delay > 0 {
		t := time.NewTimer(f.Delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return Rate{}, ctx.Err()
		case <-t.C:
		}
	}
	base, okBase := usdValue[pair.Base]
	quote, okQuote := usdValue[pair.Quote]
	if !okBase || !okQuote {
		return Rate{}, permanent(name, "no rate for %s", pair)
	}
	if f.FailureRate > 0 && rand.Float64() < f.FailureRate { //nolint:gosec // a demo knob, not security
		return Rate{}, temporary(name, 0, "simulated outage")
	}

	jitter := 1 + (rand.Float64()-0.5)/100 //nolint:gosec // ±0.5%, so repeated updates differ
	value := base / quote * jitter
	decimals := 4
	if value < 1 {
		decimals = 6 // MXN/EUR is about 0.047: keep significant digits
	}
	price := strconv.FormatFloat(value, 'f', decimals, 64)

	now := time.Now
	if f.Now != nil {
		now = f.Now
	}
	y, m, d := now().UTC().Date()
	return Rate{Price: quotes.Price(price), Date: time.Date(y, m, d, 0, 0, 0, 0, time.UTC)}, nil
}
