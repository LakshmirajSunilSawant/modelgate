package audit

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema is created on first use. It is safe to run repeatedly.
const Schema = `
CREATE TABLE IF NOT EXISTS modelgate_audit (
  id              text PRIMARY KEY,
  recorded_at     timestamptz NOT NULL,
  namespace       text NOT NULL,
  rollout         text NOT NULL,
  rollout_uid     text NOT NULL,
  action          text NOT NULL,
  actor           text NOT NULL,
  stable_revision text NOT NULL DEFAULT '',
  canary_revision text NOT NULL DEFAULT '',
  step            integer NOT NULL DEFAULT 0,
  canary_weight   integer NOT NULL DEFAULT 0,
  reason          text NOT NULL DEFAULT '',
  verdict         jsonb
);
CREATE INDEX IF NOT EXISTS modelgate_audit_rollout_time ON modelgate_audit (namespace, rollout, recorded_at);
CREATE INDEX IF NOT EXISTS modelgate_audit_action_time ON modelgate_audit (action, recorded_at);
`

const insertSQL = `
INSERT INTO modelgate_audit (id, recorded_at, namespace, rollout, rollout_uid, action, actor,
  stable_revision, canary_revision, step, canary_weight, reason, verdict)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (id) DO NOTHING`

// Postgres stores entries in PostgreSQL. The pool connects lazily, so the
// controller starts even when the database is down; the schema is created
// on the first successful contact.
type Postgres struct {
	pool    *pgxpool.Pool
	timeout time.Duration

	mu     sync.Mutex
	schema bool
}

// NewPostgres parses the DSN and returns a log. It does not connect.
func NewPostgres(dsn string) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	return &Postgres{pool: pool, timeout: 5 * time.Second}, nil
}

// Close releases the pool.
func (p *Postgres) Close() { p.pool.Close() }

func (p *Postgres) ensureSchema(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.schema {
		return nil
	}
	if _, err := p.pool.Exec(ctx, Schema); err != nil {
		return err
	}
	p.schema = true
	return nil
}

func (p *Postgres) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if err := p.pool.Ping(ctx); err != nil {
		return ErrUnavailable{err}
	}
	if err := p.ensureSchema(ctx); err != nil {
		return ErrUnavailable{err}
	}
	return nil
}

func (p *Postgres) Write(ctx context.Context, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if err := p.ensureSchema(ctx); err != nil {
		return ErrUnavailable{err}
	}
	batch := &pgx.Batch{}
	for _, e := range entries {
		var verdict any
		if len(e.Verdict) > 0 {
			verdict = string(e.Verdict)
		}
		batch.Queue(insertSQL, e.ID(), e.Time.UTC(), e.Namespace, e.Rollout, e.RolloutUID, e.Action, e.Actor,
			e.StableRevision, e.CanaryRevision, e.Step, e.CanaryWeight, e.Reason, verdict)
	}
	// A batch outside an explicit transaction runs as one implicit
	// transaction, so either every entry is stored or none is.
	if err := p.pool.SendBatch(ctx, batch).Close(); err != nil {
		return ErrUnavailable{err}
	}
	return nil
}

// Query returns a rollout's entries, oldest first. Used by tests and tools.
func (p *Postgres) Query(ctx context.Context, namespace, rollout string) ([]Entry, error) {
	rows, err := p.pool.Query(ctx, `SELECT recorded_at, namespace, rollout, rollout_uid, action, actor,
	  stable_revision, canary_revision, step, canary_weight, reason, coalesce(verdict::text, '')
	  FROM modelgate_audit WHERE namespace = $1 AND rollout = $2 ORDER BY recorded_at, action`, namespace, rollout)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var e Entry
		var verdict string
		if err := rows.Scan(&e.Time, &e.Namespace, &e.Rollout, &e.RolloutUID, &e.Action, &e.Actor,
			&e.StableRevision, &e.CanaryRevision, &e.Step, &e.CanaryWeight, &e.Reason, &verdict); err != nil {
			return nil, err
		}
		if verdict != "" {
			e.Verdict = []byte(verdict)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
