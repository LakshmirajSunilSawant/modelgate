package analyzer

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"

	"github.com/modelgate/modelgate/internal/analysis"
	"github.com/modelgate/modelgate/internal/capture"
)

// ConsumeNATS replays and then follows the MODELGATE JetStream stream. The
// analyzer keeps sketches in memory only; replaying the stream's retained
// hour on start rebuilds them, so an analyzer restart mid-rollout loses no
// evidence.
func (s *Server) ConsumeNATS(ctx context.Context, url string) error {
	nc, err := nats.Connect(url, nats.Name("modelgate-analyzer"), nats.MaxReconnects(-1))
	if err != nil {
		return err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return err
	}
	if err := capture.EnsureStream(js); err != nil {
		nc.Close()
		return err
	}
	sub, err := js.Subscribe(capture.NATSSubject+".>", func(m *nats.Msg) {
		var events []analysis.Event
		if err := json.Unmarshal(m.Data, &events); err != nil {
			s.Log.Warn("dropping malformed event batch", "subject", m.Subject, "err", err)
			return
		}
		s.Ingest(events)
	}, nats.OrderedConsumer(), nats.DeliverAll())
	if err != nil {
		nc.Close()
		return err
	}
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
		nc.Close()
	}()
	return nil
}
