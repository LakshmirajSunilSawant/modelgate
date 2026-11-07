package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/modelgate/modelgate/internal/analysis"
)

// Sink delivers a batch of events. Implementations may be slow; they run on
// the batcher's goroutine, never on the request path.
type Sink interface {
	Send(ctx context.Context, events []analysis.Event) error
}

// Batcher is a non-blocking Publisher: events go into a bounded queue and a
// background goroutine flushes them to a Sink. When the queue is full,
// events are dropped and counted. The rollout then pauses for lack of
// samples instead of the model's callers seeing errors.
type Batcher struct {
	ch       chan analysis.Event
	sink     Sink
	maxBatch int
	interval time.Duration
	Dropped  atomic.Int64
	Failed   atomic.Int64
	log      *slog.Logger
}

// NewBatcher creates a batcher. Call Run to start flushing.
func NewBatcher(sink Sink, queue, maxBatch int, interval time.Duration, log *slog.Logger) *Batcher {
	return &Batcher{ch: make(chan analysis.Event, queue), sink: sink, maxBatch: maxBatch, interval: interval, log: log}
}

// Publish enqueues without blocking.
func (b *Batcher) Publish(e analysis.Event) {
	select {
	case b.ch <- e:
	default:
		b.Dropped.Add(1)
	}
}

// Run flushes until ctx is done, then drains what is queued.
func (b *Batcher) Run(ctx context.Context) {
	t := time.NewTicker(b.interval)
	defer t.Stop()
	batch := make([]analysis.Event, 0, b.maxBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := b.sink.Send(sctx, batch); err != nil {
			b.Failed.Add(int64(len(batch)))
			b.log.Warn("event delivery failed; dropping batch", "events", len(batch), "err", err)
		}
		cancel()
		batch = batch[:0]
	}
	for {
		select {
		case e := <-b.ch:
			batch = append(batch, e)
			if len(batch) >= b.maxBatch {
				flush()
			}
		case <-t.C:
			flush()
		case <-ctx.Done():
			for {
				select {
				case e := <-b.ch:
					batch = append(batch, e)
				default:
					flush()
					return
				}
			}
		}
	}
}

// HTTPSink posts batches to the analyzer's /v1/events endpoint. It is the
// simplest transport and needs no NATS.
type HTTPSink struct {
	URL    string
	Client *http.Client
}

// Batches are split so one request or message stays under these sizes.
// The analyzer accepts 16 MiB per request; NATS accepts 1 MiB per message
// by default.
const (
	httpChunkBytes = 4 << 20
	natsChunkBytes = 512 << 10
)

// encodeChunks encodes events as JSON arrays of at most limit bytes each.
// An event too large on its own is sent without its payload, so its
// prediction still counts.
func encodeChunks(events []analysis.Event, limit int) ([][]byte, error) {
	var chunks [][]byte
	cur := []byte{'['}
	for i := range events {
		b, err := json.Marshal(&events[i])
		if err != nil {
			return nil, err
		}
		if len(b)+2 > limit && events[i].Payload != nil {
			e := events[i]
			e.Payload = nil
			if b, err = json.Marshal(&e); err != nil {
				return nil, err
			}
		}
		if len(cur) > 1 && len(cur)+len(b)+1 > limit {
			chunks = append(chunks, append(cur, ']'))
			cur = []byte{'['}
		}
		if len(cur) > 1 {
			cur = append(cur, ',')
		}
		cur = append(cur, b...)
	}
	if len(cur) > 1 {
		chunks = append(chunks, append(cur, ']'))
	}
	return chunks, nil
}

func (s *HTTPSink) Send(ctx context.Context, events []analysis.Event) error {
	chunks, err := encodeChunks(events, httpChunkBytes)
	if err != nil {
		return err
	}
	for _, body := range chunks {
		if err := s.post(ctx, body); err != nil {
			return err
		}
	}
	return nil
}

func (s *HTTPSink) post(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c := s.Client
	if c == nil {
		c = http.DefaultClient
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("analyzer returned %s", resp.Status)
	}
	return nil
}

// NATSSubject is the subject prefix events are published under; the full
// subject is <prefix>.<namespace>.<name>.
const NATSSubject = "modelgate.events"

// NATSSink publishes each batch as one JetStream message.
type NATSSink struct {
	JS      nats.JetStreamContext
	Subject string

	mu     sync.Mutex
	stream bool
}

// NewNATSSink returns a sink that connects in the background. A sidecar that
// starts while NATS is down keeps serving and starts publishing once NATS
// is back; batches sent in the meantime fail and are dropped.
func NewNATSSink(url, rollout string) (*NATSSink, *nats.Conn, error) {
	nc, err := nats.Connect(url, nats.Name("modelgate-sidecar"), nats.MaxReconnects(-1), nats.RetryOnFailedConnect(true))
	if err != nil {
		return nil, nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return &NATSSink{JS: js, Subject: SubjectFor(rollout)}, nc, nil
}

func (s *NATSSink) ensureStream() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stream {
		return nil
	}
	if err := EnsureStream(s.JS); err != nil {
		return err
	}
	s.stream = true
	return nil
}

// SubjectFor maps "namespace/name" to a NATS subject.
func SubjectFor(rollout string) string {
	return NATSSubject + "." + string(bytes.ReplaceAll([]byte(rollout), []byte("/"), []byte(".")))
}

// EnsureStream creates the MODELGATE stream if it does not exist. Events are
// kept for an hour, which covers analyzer restarts mid-rollout.
func EnsureStream(js nats.JetStreamContext) error {
	if _, err := js.StreamInfo("MODELGATE"); err == nil {
		return nil
	}
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "MODELGATE",
		Subjects: []string{NATSSubject + ".>"},
		MaxAge:   time.Hour,
		Storage:  nats.FileStorage,
		Discard:  nats.DiscardOld,
	})
	return err
}

func (s *NATSSink) Send(ctx context.Context, events []analysis.Event) error {
	chunks, err := encodeChunks(events, natsChunkBytes)
	if err != nil {
		return err
	}
	if err := s.ensureStream(); err != nil {
		return err
	}
	for _, body := range chunks {
		if _, err := s.JS.Publish(s.Subject, body, nats.Context(ctx)); err != nil {
			return err
		}
	}
	return nil
}
