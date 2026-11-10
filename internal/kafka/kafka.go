// Package kafka connects the worker to Kafka with franz-go: it publishes the
// outbox and consumes update requests in a consumer group.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmitryGityuk/currency-quotes/internal/events"
	"github.com/DmitryGityuk/currency-quotes/internal/outbox"
	"github.com/DmitryGityuk/currency-quotes/internal/worker"
)

// NewClient returns the worker's Kafka client. One client both produces (the
// outbox and the DLQ) and consumes TopicUpdateRequested in group.
//
//   - Offsets are committed by hand, after the database write, so a message
//     is processed at least once.
//   - BlockRebalanceOnPoll keeps partitions from moving to another worker
//     while a polled batch is being processed.
//   - The cooperative-sticky balancer moves only the partitions that have to
//     move when workers join or leave, instead of stopping the whole group.
//   - A new group starts from the earliest offset, so requests made before
//     the first worker started are not skipped.
//   - Records that cannot be delivered for 30 seconds fail instead of
//     waiting forever, which is franz-go's default.
func NewClient(brokers []string, group string, logger *slog.Logger) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("currency-quotes-worker"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(events.TopicUpdateRequested),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(30*time.Second),
		kgo.WithLogger(slogLogger{logger}),
	)
}

// Close leaves the consumer group and closes the client, waiting at most
// timeout for the group coordinator. If the coordinator is gone or has
// forgotten the group, a plain Close retries forever, and a worker that
// cannot exit cannot be restarted either. ctx is usually already cancelled
// at shutdown, so only its values are kept.
func Close(ctx context.Context, cl *kgo.Client, timeout time.Duration) {
	cl.AllowRebalance()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	_ = cl.LeaveGroupContext(ctx)
	cl.Close()
}

// Producer publishes outbox messages.
type Producer struct {
	cl *kgo.Client
}

// NewProducer returns a Producer that sends through cl.
func NewProducer(cl *kgo.Client) *Producer { return &Producer{cl: cl} }

// Publish implements outbox.Publisher: it returns once every message is
// acknowledged by all in-sync replicas. The producer is idempotent, so
// retries inside the client do not duplicate messages.
func (p *Producer) Publish(ctx context.Context, msgs []outbox.Message) error {
	recs := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		recs[i] = &kgo.Record{Topic: m.Topic, Key: []byte(m.Key), Value: m.Value, Headers: recordHeaders(m.Headers)}
	}
	return p.cl.ProduceSync(ctx, recs...).FirstErr()
}

// Handler processes one message; see worker.Processor.Handle for the meaning
// of its error.
type Handler func(ctx context.Context, msg worker.Message) error

// Observer receives what the consumer does, for metrics.
type Observer interface {
	// MessageHandled reports a record: "processed", "dlq" or "retried".
	MessageHandled(outcome string)
}

type nopObserver struct{}

func (nopObserver) MessageHandled(string) {}

// Consumer feeds polled records to a Handler.
type Consumer struct {
	observer   Observer
	cl         *kgo.Client
	handle     Handler
	logger     *slog.Logger
	pollSize   int
	retryDelay time.Duration // before handling a failed record again

	// While fetches fail, polls are spaced out from minPause to maxPause:
	// otherwise a broker outage turns the loop into a busy spin that writes
	// thousands of log lines a second.
	minPause, maxPause time.Duration
	// topicGrace is how long UNKNOWN_TOPIC_ID is tolerated; see checkTopicIDs.
	topicGrace     time.Duration
	unknownTopicAt time.Time
	now            func() time.Time
}

// NewConsumer returns a consumer that passes records from cl to handle.
func NewConsumer(cl *kgo.Client, handle Handler, logger *slog.Logger) *Consumer {
	return &Consumer{
		observer:   nopObserver{},
		cl:         cl,
		handle:     handle,
		logger:     logger,
		pollSize:   100,
		retryDelay: time.Second,
		minPause:   100 * time.Millisecond,
		maxPause:   5 * time.Second,
		topicGrace: 30 * time.Second,
		now:        time.Now,
	}
}

// WithObserver makes the consumer report to o and returns the consumer.
func (c *Consumer) WithObserver(o Observer) *Consumer {
	c.observer = o
	return c
}

// ErrTopicRecreated is returned by Run when the topic was deleted and
// created again while the worker was running.
var ErrTopicRecreated = errors.New("kafka topic was deleted and recreated")

// Run consumes until ctx is cancelled. Partitions of one poll are processed
// in parallel, records within a partition in order: all updates of one pair
// share a partition, so they are handled one after another.
func (c *Consumer) Run(ctx context.Context) error {
	var pause time.Duration
	for {
		fetches := c.cl.PollRecords(ctx, c.pollSize)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil //nolint:nilerr // shutdown is a normal exit, not a failure
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			if err := c.checkTopicIDs(errs); err != nil {
				return err
			}
			pause = min(max(pause*2, c.minPause), c.maxPause)
			c.logger.WarnContext(ctx, "kafka fetch failed",
				slog.String("topic", errs[0].Topic),
				slog.Int("failed_partitions", len(errs)),
				slog.Any("error", errs[0].Err),
				slog.Duration("retry_in", pause))
		} else {
			pause = 0
			c.unknownTopicAt = time.Time{}
		}

		var wg sync.WaitGroup
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			wg.Go(func() {
				for _, r := range p.Records {
					if !c.process(ctx, r) {
						return // shutting down: the rest stays uncommitted
					}
				}
			})
		})
		wg.Wait()
		c.cl.AllowRebalance()

		if pause > 0 && fetches.NumRecords() == 0 {
			t := time.NewTimer(pause)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil
			case <-t.C:
			}
		}
	}
}

// checkTopicIDs gives up when UNKNOWN_TOPIC_ID persists. franz-go keeps the
// id a topic had when the client first saw it and does not adopt a new one,
// so after the topic is recreated (for example, the Kafka container was
// recreated while the worker kept running) the client never recovers.
// Exiting lets Docker or Kubernetes restart the worker with a fresh client.
func (c *Consumer) checkTopicIDs(errs []kgo.FetchError) error {
	for _, fe := range errs {
		if !errors.Is(fe.Err, kerr.UnknownTopicID) {
			continue
		}
		now := c.now()
		if c.unknownTopicAt.IsZero() {
			c.unknownTopicAt = now
			return nil
		}
		if now.Sub(c.unknownTopicAt) >= c.topicGrace {
			return fmt.Errorf("%w: %s; restarting to pick up its new id", ErrTopicRecreated, fe.Topic)
		}
		return nil
	}
	c.unknownTopicAt = time.Time{}
	return nil
}

// process handles one record until it is done, sent to the DLQ, or the
// consumer is shutting down. It reports whether the record was committed.
func (c *Consumer) process(ctx context.Context, r *kgo.Record) bool {
	msg := worker.Message{Key: string(r.Key), Value: r.Value, Headers: headerMap(r.Headers)}
	for {
		outcome := "processed"
		err := c.handle(ctx, msg)
		if errors.Is(err, worker.ErrPoison) {
			outcome = "dlq"
			err = c.toDLQ(ctx, r, err)
		}
		if err == nil {
			c.commit(ctx, r)
			c.observer.MessageHandled(outcome)
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		c.observer.MessageHandled("retried")
		// The database or the broker is down. Retrying the same record keeps
		// the order within the partition; offsets are not moved past it.
		c.logger.WarnContext(ctx, "message not processed, will retry",
			slog.String("topic", r.Topic), slog.Int("partition", int(r.Partition)),
			slog.Int64("offset", r.Offset), slog.Any("error", err))
		t := time.NewTimer(c.retryDelay)
		select {
		case <-ctx.Done():
			t.Stop()
			return false
		case <-t.C:
		}
	}
}

// toDLQ publishes a record that can never be processed, with the reason and
// where it came from, so it can be inspected and replayed by hand.
func (c *Consumer) toDLQ(ctx context.Context, r *kgo.Record, reason error) error {
	headers := append(slices.Clone(r.Headers),
		kgo.RecordHeader{Key: events.HeaderDLQReason, Value: []byte(reason.Error())},
		kgo.RecordHeader{Key: "source_topic", Value: []byte(r.Topic)},
		kgo.RecordHeader{Key: "source_partition", Value: []byte(strconv.Itoa(int(r.Partition)))},
		kgo.RecordHeader{Key: "source_offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
	)
	dlq := &kgo.Record{Topic: events.TopicUpdateRequestedDLQ, Key: r.Key, Value: r.Value, Headers: headers}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.cl.ProduceSync(ctx, dlq).FirstErr(); err != nil {
		return err
	}
	c.logger.WarnContext(ctx, "message sent to the DLQ",
		slog.Int("partition", int(r.Partition)), slog.Int64("offset", r.Offset), slog.Any("reason", reason))
	return nil
}

// commit stores the offset after the record. It gets its own short timeout
// so a record processed just before shutdown is still committed; if the
// commit fails anyway, the record is redelivered, which is harmless.
func (c *Consumer) commit(ctx context.Context, r *kgo.Record) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := c.cl.CommitRecords(ctx, r); err != nil {
		c.logger.WarnContext(ctx, "offset commit failed, the message may be redelivered",
			slog.Int("partition", int(r.Partition)), slog.Int64("offset", r.Offset), slog.Any("error", err))
	}
}

func recordHeaders(m map[string]string) []kgo.RecordHeader {
	headers := make([]kgo.RecordHeader, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(m[k])})
	}
	return headers
}

func headerMap(hs []kgo.RecordHeader) map[string]string {
	m := make(map[string]string, len(hs))
	for _, h := range hs {
		m[h.Key] = string(h.Value)
	}
	return m
}

// slogLogger routes franz-go's own log lines (warnings and errors only:
// reconnects, rebalances gone wrong) into the service log.
type slogLogger struct{ l *slog.Logger }

func (s slogLogger) Level() kgo.LogLevel { return kgo.LogLevelWarn }

func (s slogLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	lvl := slog.LevelWarn
	if level == kgo.LogLevelError {
		lvl = slog.LevelError
	}
	s.l.Log(context.Background(), lvl, "kafka: "+msg, keyvals...)
}
