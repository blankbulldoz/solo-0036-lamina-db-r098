package engine

import (
	"context"
	"lamina/internal/datum"
	"lamina/internal/query"
	"time"
)

// Query runs a structured or SQL-derived request under ctx. The context
// carries both client disconnects and the derived time budget. Every query
// acquires a read pin for its chosen snapshot before touching version data and
// releases exactly that pin on exit, so cancellation cannot remove the history
// protection of a still-running reader or transaction.
func (d *DB) Query(ctx context.Context, q query.Request) (result query.Result, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err = d.usable(); err != nil {
		return query.Result{}, err
	}
	r, err := d.catalog.Relation(q.Relation)
	if err != nil {
		return query.Result{}, invalid(err)
	}
	// Binding is the first cancellation boundary: a request already abandoned
	// before admission must not pin a snapshot or run.
	if err = query.Gate(ctx, query.StageBinding); err != nil {
		return query.Result{}, d.mapQueryError(err)
	}
	at := d.sequence
	var tx *Transaction
	if q.Transaction != "" {
		if q.At != nil {
			return query.Result{}, fault("invalid", "at and transaction are mutually exclusive")
		}
		tx, err = d.tx(q.Transaction)
		if err != nil {
			return query.Result{}, err
		}
		at = tx.Snapshot
	} else if q.At != nil {
		at = *q.At
	}
	if at < d.horizon || at > d.sequence {
		return query.Result{}, fault("snapshot", "snapshot unavailable; retained interval %d..%d", d.horizon, d.sequence)
	}
	if at < r.Created {
		return query.Result{}, fault("snapshot", "relation did not exist at snapshot")
	}
	if err = query.Bind(r, q); err != nil {
		return query.Result{}, invalid(err)
	}
	if tx != nil && len(tx.writes[q.Relation]) > 0 {
		q.ForceScan = true
	}
	// Pin the read basis while the executor may dereference versions. The pin
	// is released on every exit path, including cancellation and timeout.
	pin := d.readPins.acquire(at, "query")
	defer d.readPins.release(pin)

	plan, keys := query.Choose(d.catalog, d.indexes, q, at, len(d.store.Tables[q.Relation]))
	rows := map[string]datum.Row{}
	if plan.Access == "index" {
		// Index candidates are rechecked against real versions; the candidate
		// walk is bounded and gated at entry, visibility reads remain pinned.
		if err = query.Gate(ctx, query.StageMaterialize); err != nil {
			return query.Result{}, d.mapQueryError(err)
		}
		processed := 0
		for _, key := range keys {
			if row, exists := d.store.Read(q.Relation, key, at); exists {
				rows[key] = row
			}
			processed++
			if processed%query.CheckpointEvery == 0 {
				if err = query.Gate(ctx, query.StageMaterialize); err != nil {
					return query.Result{}, d.mapQueryError(err)
				}
			}
		}
	} else {
		gate := func() error { return query.Gate(ctx, query.StageMaterialize) }
		rows, err = d.store.ScanCancellable(q.Relation, at, gate)
		if err != nil {
			return query.Result{}, d.mapQueryError(err)
		}
	}
	if tx != nil {
		overlay(rows, tx.writes, q.Relation)
	}
	result, err = query.Execute(ctx, r, q, rows, plan, keys)
	if err != nil {
		return query.Result{}, d.mapQueryError(err)
	}
	d.queries++
	return result, nil
}

// mapQueryError translates cooperative query stops into domain errors while
// counting them for /stats. Binding and validation faults are passed through
// unchanged; only typed query control errors are reclassified.
func (d *DB) mapQueryError(err error) error {
	switch e := err.(type) {
	case *query.CanceledError:
		d.canceled++
		return fault("canceled", "%s", e.Error())
	case *query.TimeoutError:
		d.timedOut++
		return fault("timeout", "%s", e.Error())
	default:
		return err
	}
}

// WithBudget derives a deadline context from an optional timeout_ms budget.
// Zero means no budget and returns the parent unchanged, preserving the legacy
// unbounded request semantics exactly. The start time recorded on the context
// is the single origin used for elapsed/remaining feedback.
func WithBudget(parent context.Context, timeoutMS int64) (context.Context, context.CancelFunc, error) {
	if timeoutMS == 0 {
		return parent, func() {}, nil
	}
	if timeoutMS < 0 || timeoutMS > query.MaxTimeoutMS {
		return nil, nil, fault("invalid", "timeout_ms must be 0..%d", query.MaxTimeoutMS)
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(query.WithStart(parent, start), time.Duration(timeoutMS)*time.Millisecond)
	return ctx, cancel, nil
}

func (d *DB) Get(ctx context.Context, id, table string, key datum.Value) (datum.Row, error) {
	q := query.Request{Relation: table, Transaction: id, Limit: 1}
	d.mu.RLock()
	r, err := d.catalog.Relation(table)
	d.mu.RUnlock()
	if err != nil {
		return nil, invalid(err)
	}
	col, _ := r.Column(r.Primary)
	if key.Null() || key.Kind != col.Type {
		return nil, fault("invalid", "invalid primary value")
	}
	q.Where = []query.Predicate{{Column: r.Primary, Op: "=", Value: key}}
	result, err := d.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 {
		return nil, fault("not_found", "tuple not found")
	}
	return result.Rows[0], nil
}
