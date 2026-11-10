// Package quotes holds the domain types of the service: currency pairs,
// quote updates and their statuses. It has no dependencies on HTTP, the
// database or Kafka.
package quotes

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Currency is an ISO 4217 code in upper case, such as "EUR".
type Currency string

// Pair is a currency pair: the price of one unit of Base expressed in Quote.
type Pair struct {
	Base  Currency
	Quote Currency
}

// String returns the pair as "EUR/MXN".
func (p Pair) String() string { return string(p.Base) + "/" + string(p.Quote) }

// Errors returned by ParsePair and Currencies.Validate. They are wrapped with
// details, so compare them with errors.Is.
var (
	ErrInvalidPair         = errors.New("pair must look like EUR/MXN")
	ErrSameCurrency        = errors.New("pair has the same currency on both sides")
	ErrUnsupportedCurrency = errors.New("currency is not supported")
)

// ParsePair parses a pair written as "EUR/MXN" in any letter case.
// It checks only the format; Currencies.Validate checks the currencies.
func ParsePair(s string) (Pair, error) {
	base, quote, ok := strings.Cut(s, "/")
	if !ok || !isCode(base) || !isCode(quote) {
		return Pair{}, fmt.Errorf("%w, got %q", ErrInvalidPair, s)
	}
	return Pair{
		Base:  Currency(strings.ToUpper(base)),
		Quote: Currency(strings.ToUpper(quote)),
	}, nil
}

// isCode reports whether s is three ASCII letters.
func isCode(s string) bool {
	if len(s) != 3 {
		return false
	}
	for i := range len(s) {
		c := s[i] | 0x20 // lower-case ASCII letters, leave the rest alone
		if c < 'a' || c > 'z' {
			return false
		}
	}
	return true
}

// Currencies is the set of currencies the service supports.
type Currencies struct {
	codes []Currency // sorted, without duplicates
}

// NewCurrencies builds the set from ISO codes in any letter case.
func NewCurrencies(codes ...string) (Currencies, error) {
	if len(codes) == 0 {
		return Currencies{}, errors.New("at least one currency is required")
	}
	set := make([]Currency, 0, len(codes))
	for _, code := range codes {
		code = strings.TrimSpace(code)
		if !isCode(code) {
			return Currencies{}, fmt.Errorf("invalid currency code %q", code)
		}
		set = append(set, Currency(strings.ToUpper(code)))
	}
	slices.Sort(set)
	return Currencies{codes: slices.Compact(set)}, nil
}

// MustCurrencies is NewCurrencies that panics on error, for constants and tests.
func MustCurrencies(codes ...string) Currencies {
	c, err := NewCurrencies(codes...)
	if err != nil {
		panic(err)
	}
	return c
}

// Contains reports whether c is supported.
func (cs Currencies) Contains(c Currency) bool {
	_, found := slices.BinarySearch(cs.codes, c)
	return found
}

// Validate reports whether the service can quote pair p.
func (cs Currencies) Validate(p Pair) error {
	for _, c := range []Currency{p.Base, p.Quote} {
		if !cs.Contains(c) {
			return fmt.Errorf("%w: %s; supported: %s", ErrUnsupportedCurrency, c, cs)
		}
	}
	if p.Base == p.Quote {
		return fmt.Errorf("%w: %s", ErrSameCurrency, p)
	}
	return nil
}

// String lists the currencies as "EUR, MXN, USD".
func (cs Currencies) String() string {
	parts := make([]string, len(cs.codes))
	for i, c := range cs.codes {
		parts[i] = string(c)
	}
	return strings.Join(parts, ", ")
}
