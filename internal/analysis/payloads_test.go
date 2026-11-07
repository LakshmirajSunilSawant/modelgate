package analysis

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestPayloadsAreKeptApartFromStats(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := now
	store := NewStore(StoreOptions{Now: func() time.Time { return clock }})
	label := "a"
	for i := 0; i < maxPayloads+10; i++ {
		store.Ingest(Event{Rollout: "ns/m", Revision: "s", RequestID: fmt.Sprint(i), Time: now, Status: 200, Label: &label,
			Payload: &Payload{Request: json.RawMessage(`{"i":1}`)}})
	}
	if got := store.Track("ns/m", "s", now.Add(-time.Minute)).Requests; got != maxPayloads+10 {
		t.Errorf("requests %d: payload events must count once as predictions", got)
	}
	got := store.Payloads("ns/m", "", 0)
	if len(got) != maxPayloads || got[0].RequestID != fmt.Sprint(maxPayloads+9) {
		t.Errorf("kept %d payloads, newest %s", len(got), got[0].RequestID)
	}
	for _, p := range store.rollouts["ns/m"].pending {
		if p.ev.Payload != nil {
			t.Fatal("pairing buffer holds a payload")
		}
	}
	clock = now.Add(payloadTTL + time.Minute)
	if got := store.Payloads("ns/m", "", 0); len(got) != 0 {
		t.Errorf("%d payloads older than the TTL returned", len(got))
	}
	store.Ingest(Event{Rollout: "ns/m", Revision: "s", Time: clock, Status: 200}) // keep the rollout alive
	store.GC()
	if n := len(store.rollouts["ns/m"].payloads); n != 0 {
		t.Errorf("GC kept %d expired payloads", n)
	}
}
