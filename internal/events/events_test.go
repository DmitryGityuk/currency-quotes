package events

import (
	"errors"
	"testing"
	"time"
	"uuid"
)

func TestUpdateRequestedRoundTrip(t *testing.T) {
	in := UpdateRequested{
		UpdateID:    uuid.MustParse("01972f4e-8b3a-7c21-9d4e-5f6a7b8c9d0e"),
		Pair:        "EUR/MXN",
		RequestedAt: time.Date(2026, 11, 6, 10, 15, 0, 123000000, time.UTC),
	}
	b, err := in.Encode()
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	want := `{"update_id":"01972f4e-8b3a-7c21-9d4e-5f6a7b8c9d0e","pair":"EUR/MXN","requested_at":"2026-11-06T10:15:00.123Z"}`
	if string(b) != want {
		t.Errorf("Encode() = %s\nwant       %s", b, want)
	}

	out, err := DecodeUpdateRequested(b)
	if err != nil {
		t.Fatalf("DecodeUpdateRequested() error = %v", err)
	}
	if out != in {
		t.Errorf("round trip = %+v, want %+v", out, in)
	}
	if in.Key() != "EUR/MXN" {
		t.Errorf("Key() = %q, want the pair", in.Key())
	}
}

func TestDecodeUpdateRequestedIgnoresUnknownFields(t *testing.T) {
	_, err := DecodeUpdateRequested([]byte(`{"update_id":"01972f4e-8b3a-7c21-9d4e-5f6a7b8c9d0e","new_field":1}`))
	if err != nil {
		t.Errorf("DecodeUpdateRequested() error = %v, want nil", err)
	}
}

func TestDecodeUpdateRequestedMalformed(t *testing.T) {
	for _, payload := range []string{
		``,
		`not json`,
		`{}`,
		`{"update_id":"not-a-uuid"}`,
		`{"update_id":"00000000-0000-0000-0000-000000000000"}`,
	} {
		if _, err := DecodeUpdateRequested([]byte(payload)); !errors.Is(err, ErrMalformed) {
			t.Errorf("DecodeUpdateRequested(%q) error = %v, want ErrMalformed", payload, err)
		}
	}
}
