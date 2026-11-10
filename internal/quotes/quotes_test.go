package quotes

import (
	"errors"
	"testing"
)

func TestParsePair(t *testing.T) {
	tests := []struct {
		in      string
		want    Pair
		wantErr error
	}{
		{in: "EUR/MXN", want: Pair{Base: "EUR", Quote: "MXN"}},
		{in: "usd/eur", want: Pair{Base: "USD", Quote: "EUR"}},
		{in: "Mxn/Usd", want: Pair{Base: "MXN", Quote: "USD"}},
		{in: "EURMXN", wantErr: ErrInvalidPair},
		{in: "EUR-MXN", wantErr: ErrInvalidPair},
		{in: "EUR/MX", wantErr: ErrInvalidPair},
		{in: "EUR/MXN/USD", wantErr: ErrInvalidPair},
		{in: "EU1/MXN", wantErr: ErrInvalidPair},
		{in: "EU@/MXN", wantErr: ErrInvalidPair}, // '@' | 0x20 is '`', just below 'a'
		{in: "EU[/MXN", wantErr: ErrInvalidPair}, // '[' | 0x20 is '{', just above 'z'
		{in: " EUR/MXN", wantErr: ErrInvalidPair},
		{in: "", wantErr: ErrInvalidPair},
		{in: "ЕВР/MXN", wantErr: ErrInvalidPair},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParsePair(tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ParsePair(%q) error = %v, want %v", tt.in, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("ParsePair(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestPairString(t *testing.T) {
	if got := (Pair{Base: "EUR", Quote: "MXN"}).String(); got != "EUR/MXN" {
		t.Errorf("String() = %q", got)
	}
}

func TestCurrencies(t *testing.T) {
	cs, err := NewCurrencies("usd", " EUR", "MXN", "USD")
	if err != nil {
		t.Fatalf("NewCurrencies() error = %v", err)
	}
	if got := cs.String(); got != "EUR, MXN, USD" {
		t.Errorf("String() = %q, want sorted codes without duplicates", got)
	}

	tests := []struct {
		pair    Pair
		wantErr error
	}{
		{pair: Pair{Base: "EUR", Quote: "MXN"}},
		{pair: Pair{Base: "MXN", Quote: "USD"}},
		{pair: Pair{Base: "GBP", Quote: "USD"}, wantErr: ErrUnsupportedCurrency},
		{pair: Pair{Base: "USD", Quote: "JPY"}, wantErr: ErrUnsupportedCurrency},
		{pair: Pair{Base: "EUR", Quote: "EUR"}, wantErr: ErrSameCurrency},
	}
	for _, tt := range tests {
		t.Run(tt.pair.String(), func(t *testing.T) {
			if err := cs.Validate(tt.pair); !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate(%s) error = %v, want %v", tt.pair, err, tt.wantErr)
			}
		})
	}
}

func TestNewCurrenciesErrors(t *testing.T) {
	for _, codes := range [][]string{nil, {"EURO"}, {"US"}, {"USD", ""}} {
		if _, err := NewCurrencies(codes...); err == nil {
			t.Errorf("NewCurrencies(%q) error = nil, want error", codes)
		}
	}
}

func TestMustCurrenciesPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("MustCurrencies with an invalid code did not panic")
		}
	}()
	MustCurrencies("nope")
}

func TestParsePrice(t *testing.T) {
	valid := []string{"21.4512", "0.0539", "1", "18", "0.00000001", "100.10"}
	for _, s := range valid {
		if got, err := ParsePrice(s); err != nil || string(got) != s {
			t.Errorf("ParsePrice(%q) = %q, %v; want the same text back", s, got, err)
		}
	}
	invalid := []string{"", "0", "0.000", "-1.5", "+1.5", "1e-5", "1.", ".5", "1,5", "21.45.1", " 1", "NaN", "Infinity"}
	for _, s := range invalid {
		if _, err := ParsePrice(s); !errors.Is(err, ErrInvalidPrice) {
			t.Errorf("ParsePrice(%q) error = %v, want ErrInvalidPrice", s, err)
		}
	}
}

func TestStatusFinished(t *testing.T) {
	for s, want := range map[Status]bool{
		StatusPending: false, StatusProcessing: false, StatusDone: true, StatusFailed: true,
	} {
		if got := s.Finished(); got != want {
			t.Errorf("%s.Finished() = %v, want %v", s, got, want)
		}
	}
}
