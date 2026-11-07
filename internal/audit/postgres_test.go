package audit

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// TestPostgres runs against a real database when MODELGATE_TEST_POSTGRES_DSN
// is set, for example:
//
//	docker run -d --name mg-pg -e POSTGRES_PASSWORD=pg -p 5432:5432 postgres:17-alpine
//	MODELGATE_TEST_POSTGRES_DSN=postgres://postgres:pg@localhost:5432/postgres?sslmode=disable go test ./internal/audit/
func TestPostgres(t *testing.T) {
	dsn := os.Getenv("MODELGATE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("MODELGATE_TEST_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	p, err := NewPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	ns := "audit-test-" + time.Now().Format("150405.000000")
	t0 := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	verdict, _ := json.Marshal(map[string]any{"decision": "fail", "disagreements": []any{map[string]string{"requestID": "r1"}}})
	entries := []Entry{
		{Key: "a", Time: t0, Namespace: ns, Rollout: "fraud", RolloutUID: "u1", Action: ActionRolloutStarted, Actor: ControllerActor, StableRevision: "s", CanaryRevision: "c"},
		{Key: "look-1", Time: t0.Add(time.Second), Namespace: ns, Rollout: "fraud", RolloutUID: "u1", Action: ActionVerdict, Actor: ControllerActor, CanaryRevision: "c", Verdict: verdict},
		{Key: "b", Time: t0.Add(2 * time.Second), Namespace: ns, Rollout: "fraud", RolloutUID: "u1", Action: ActionRolledBack, Actor: "alice@example.com", CanaryRevision: "c", Reason: "aborted by alice"},
	}
	if err := p.Write(ctx, entries); err != nil {
		t.Fatal(err)
	}
	// A retried reconcile writes the same entries again; they are stored once.
	if err := p.Write(ctx, entries); err != nil {
		t.Fatal(err)
	}
	got, err := p.Query(ctx, ns, "fraud")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 entries, got %d", len(got))
	}
	if got[2].Actor != "alice@example.com" || got[2].Action != ActionRolledBack {
		t.Errorf("unexpected last entry %+v", got[2])
	}
	var v map[string]any
	if err := json.Unmarshal(got[1].Verdict, &v); err != nil || v["decision"] != "fail" {
		t.Errorf("verdict not round-tripped: %s (%v)", got[1].Verdict, err)
	}
}

func TestPostgresUnreachable(t *testing.T) {
	p, err := NewPostgres("postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Ping(context.Background()); err == nil {
		t.Fatal("ping to closed port succeeded")
	}
	if err := p.Write(context.Background(), []Entry{{Action: ActionPromoted}}); err == nil {
		t.Fatal("write to closed port succeeded")
	}
}

func TestEntryIDIsStable(t *testing.T) {
	e := Entry{Key: "k", Namespace: "ml", Rollout: "fraud", RolloutUID: "u", Action: ActionVerdict, Time: time.Now()}
	f := e
	f.Time = e.Time.Add(time.Hour)
	if e.ID() != f.ID() {
		t.Error("keyed entries must not depend on time")
	}
	g := e
	g.Key = ""
	h := g
	h.Time = g.Time.Add(time.Second)
	if g.ID() == h.ID() {
		t.Error("unkeyed entries at different times must differ")
	}
}
