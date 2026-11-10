// Package demo walks through the assignment against a running API and
// explains each step, so the service can be shown without curl or jq:
// `docker compose run --rm demo`. Every step is also checked, so the same
// command doubles as an end-to-end test in CI.
package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
	"uuid"
)

// Runner talks to the API at BaseURL and writes the story to Out.
type Runner struct {
	BaseURL string
	Client  *http.Client
	Out     io.Writer
	// PollEvery and Timeout bound the wait for an update to finish.
	PollEvery time.Duration
	Timeout   time.Duration
}

type update struct {
	ID        string `json:"id"`
	Pair      string `json:"pair"`
	Status    string `json:"status"`
	Price     string `json:"price"`
	RateDate  string `json:"rate_date"`
	UpdatedAt string `json:"updated_at"`
	Error     string `json:"error"`
}

// reply is a response that has already been read and closed.
type reply struct {
	status int
	header http.Header
	body   []byte
}

type problem struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

// Run executes the scenario and returns the first check that failed.
func (r *Runner) Run(ctx context.Context) error {
	start := time.Now()
	r.say("Currency Quotes: демо асинхронного обновления котировок")
	r.say("API: %s\n", r.BaseURL)

	steps := []struct {
		title string
		run   func(context.Context) error
	}{
		{"Запрос обновления и результат по id", r.asyncUpdate},
		{"Идемпотентность по Idempotency-Key", r.idempotencyKey},
		{"Одновременные запросы по одной паре", r.concurrentRequests},
		{"Ошибки валидации", r.validation},
	}
	for i, s := range steps {
		r.say("%d. %s", i+1, s.title)
		if err := s.run(ctx); err != nil {
			r.say("   ✗ %v", err)
			return fmt.Errorf("step %d (%s): %w", i+1, s.title, err)
		}
		r.say("")
	}
	r.say("Всё работает, демо заняло %.1f с.", time.Since(start).Seconds())
	r.say("Метрики: %s/metrics, документация: %s/docs", r.BaseURL, r.BaseURL)
	return nil
}

func (r *Runner) asyncUpdate(ctx context.Context) error {
	const pair = "EUR/MXN"
	began := time.Now()
	resp, upd, err := r.post(ctx, pair, "")
	if err != nil {
		return err
	}
	if resp.status != http.StatusAccepted {
		return fmt.Errorf("POST returned %d, want 202", resp.status)
	}
	r.say("   POST /api/v1/quotes/updates {\"pair\": %q}", pair)
	r.say("   ← 202 Accepted за %d мс, status %s, id %s", time.Since(began).Milliseconds(), upd.Status, upd.ID)
	r.say("   Location: %s", resp.header.Get("Location"))
	r.say("   Обработчик только зарегистрировал заявку: курс запрашивает воркер в фоне.")

	done, err := r.waitFinished(ctx, upd.ID, began)
	if err != nil {
		return err
	}
	if done.Status != "done" {
		return fmt.Errorf("update finished with status %s: %s", done.Status, done.Error)
	}
	r.say("   ✓ цена %s, курс на %s, обновлено %s", done.Price, done.RateDate, done.UpdatedAt)

	var latest struct {
		Price    string `json:"price"`
		UpdateID string `json:"update_id"`
	}
	status, err := r.getJSON(ctx, "/api/v1/quotes/latest?pair="+pair, &latest)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("GET latest returned %d, want 200", status)
	}
	r.say("   GET /api/v1/quotes/latest?pair=%s ← %s (обновление %s)", pair, latest.Price, latest.UpdateID)
	return nil
}

// waitFinished polls the update and prints every status change.
func (r *Runner) waitFinished(ctx context.Context, id string, began time.Time) (update, error) {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	last := ""
	for {
		var u update
		status, err := r.getJSON(ctx, "/api/v1/quotes/updates/"+id, &u)
		if err != nil {
			return update{}, err
		}
		if status != http.StatusOK {
			return update{}, fmt.Errorf("GET update returned %d, want 200", status)
		}
		if u.Status != last {
			r.say("   GET /api/v1/quotes/updates/{id} ← %s через %.1f с", u.Status, time.Since(began).Seconds())
			last = u.Status
		}
		if u.Status == "done" || u.Status == "failed" {
			return u, nil
		}
		select {
		case <-ctx.Done():
			return update{}, fmt.Errorf("update %s is still %s after %s", id, u.Status, r.Timeout)
		case <-time.After(r.PollEvery):
		}
	}
}

func (r *Runner) idempotencyKey(ctx context.Context) error {
	key := uuid.NewV7().String()
	_, first, err := r.post(ctx, "USD/EUR", key)
	if err != nil {
		return err
	}
	resp, again, err := r.post(ctx, "usd/eur", key)
	if err != nil {
		return err
	}
	replayed := resp.header.Get("Idempotent-Replayed")
	r.say("   Idempotency-Key: %s", key)
	r.say("   первый POST ← id %s", first.ID)
	r.say("   повтор     ← id %s, Idempotent-Replayed: %s", again.ID, replayed)
	if again.ID != first.ID || replayed != "true" {
		return errors.New("a repeated key must return the same update")
	}

	p, err := r.postProblem(ctx, "EUR/USD", key)
	if err != nil {
		return err
	}
	r.say("   тот же ключ с другой парой ← %d %s", p.Status, p.Code)
	if p.Status != http.StatusUnprocessableEntity || p.Code != "idempotency_key_reused" {
		return errors.New("the same key with another body must be rejected with 422")
	}
	r.say("   ✓ повтор не создаёт второе обновление, чужое тело отклоняется")
	return nil
}

func (r *Runner) concurrentRequests(ctx context.Context) error {
	const n = 5
	ids := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			_, u, err := r.post(ctx, "MXN/USD", "")
			ids[i], errs[i] = u.ID, err
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	unique := map[string]bool{}
	for _, id := range ids {
		unique[id] = true
	}
	r.say("   %d одновременных POST по MXN/USD ← %d уникальный id", n, len(unique))
	if len(unique) != 1 {
		return fmt.Errorf("got %d different updates, want one", len(unique))
	}
	r.say("   ✓ пока по паре идёт обновление, новые запросы присоединяются к нему")
	return nil
}

func (r *Runner) validation(ctx context.Context) error {
	for _, c := range []struct {
		pair       string
		wantStatus int
		wantCode   string
	}{
		{"GBP/USD", http.StatusUnprocessableEntity, "unsupported_currency"},
		{"EUR/EUR", http.StatusUnprocessableEntity, "same_currency"},
		{"EURMXN", http.StatusBadRequest, "invalid_pair"},
	} {
		p, err := r.postProblem(ctx, c.pair, "")
		if err != nil {
			return err
		}
		r.say("   %-8s ← %d %s", c.pair, p.Status, p.Code)
		if p.Status != c.wantStatus || p.Code != c.wantCode {
			return fmt.Errorf("%s: got %d %s, want %d %s", c.pair, p.Status, p.Code, c.wantStatus, c.wantCode)
		}
	}
	r.say("   ✓ ошибки в формате RFC 9457 с машинным кодом")
	return nil
}

func (r *Runner) post(ctx context.Context, pair, key string) (reply, update, error) {
	resp, err := r.do(ctx, pair, key)
	if err != nil {
		return reply{}, update{}, err
	}
	var u update
	if resp.status == http.StatusAccepted {
		if err := json.Unmarshal(resp.body, &u); err != nil {
			return reply{}, update{}, fmt.Errorf("decode POST response: %w", err)
		}
	}
	return resp, u, nil
}

func (r *Runner) postProblem(ctx context.Context, pair, key string) (problem, error) {
	resp, err := r.do(ctx, pair, key)
	if err != nil {
		return problem{}, err
	}
	var p problem
	if err := json.Unmarshal(resp.body, &p); err != nil {
		return problem{}, fmt.Errorf("decode error response: %w", err)
	}
	return p, nil
}

func (r *Runner) do(ctx context.Context, pair, key string) (reply, error) {
	payload, _ := json.Marshal(map[string]string{"pair": pair})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.BaseURL+"/api/v1/quotes/updates", bytes.NewReader(payload))
	if err != nil {
		return reply{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	return r.send(req)
}

func (r *Runner) getJSON(ctx context.Context, path string, v any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.BaseURL+path, http.NoBody)
	if err != nil {
		return 0, err
	}
	resp, err := r.send(req)
	if err != nil {
		return 0, err
	}
	if resp.status == http.StatusOK {
		if err := json.Unmarshal(resp.body, v); err != nil {
			return 0, fmt.Errorf("decode %s: %w", path, err)
		}
	}
	return resp.status, nil
}

// send performs the request and reads the whole response, so callers never
// hold an open body.
func (r *Runner) send(req *http.Request) (reply, error) {
	resp, err := r.Client.Do(req)
	if err != nil {
		return reply{}, fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return reply{}, fmt.Errorf("read response: %w", err)
	}
	return reply{status: resp.StatusCode, header: resp.Header, body: body}, nil
}

func (r *Runner) say(format string, args ...any) {
	_, _ = fmt.Fprintln(r.Out, strings.TrimRight(fmt.Sprintf(format, args...), " "))
}
