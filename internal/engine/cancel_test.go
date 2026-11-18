package engine

import (
	"context"
	"errors"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/query"
	"lamina/internal/versions"
	"path/filepath"
	"testing"
	"time"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func seedRows(t *testing.T, db *DB, n int) {
	t.Helper()
	rel := catalog.Relation{
		Name:    "spectra",
		Primary: "sample",
		Columns: []catalog.Column{
			{Name: "sample", Type: "int"},
			{Name: "band", Type: "text"},
			{Name: "amplitude", Type: "int", Nullable: true},
		},
	}
	if _, err := db.CreateRelation(rel); err != nil {
		t.Fatalf("create relation: %v", err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for i := 1; i <= n; i++ {
		row := datum.Row{"sample": datum.Integer(int64(i)), "band": datum.Text("b"), "amplitude": datum.Integer(int64(i))}
		if err = db.Put(tx.ID, "spectra", row, true); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if _, err = db.Commit(tx.ID); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestQueryPreCanceledReturnsCanceledAndNoRows(t *testing.T) {
	db := openTestDB(t)
	seedRows(t, db, 300)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := db.Query(ctx, query.Request{Relation: "spectra", TimeoutMS: 5000})
	var domain *Error
	if !errors.As(err, &domain) || domain.Code != "canceled" {
		t.Fatalf("expected canceled domain error, got %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("canceled query leaked rows: %d", len(result.Rows))
	}
	stats, _ := db.Stats()
	if stats.Canceled != 1 {
		t.Fatalf("canceled counter wrong: %d", stats.Canceled)
	}
	// The pin must be released even though the query stopped early.
	if stats.ReadPins != 0 {
		t.Fatalf("canceled query retained a read pin: %d", stats.ReadPins)
	}
}

func TestQueryTimeoutFailsAggregateNotHalfCount(t *testing.T) {
	db := openTestDB(t)
	seedRows(t, db, 500)
	// A context that deterministically reports DeadlineExceeded. With
	// TimeoutMS set, Gate classifies an exceeded deadline as a timeout, so the
	// test does not depend on wall-clock timer delivery (notably under -race).
	ctx := &expiredDeadlineContext{}
	result, qerr := db.Query(ctx, query.Request{
		Relation:  "spectra",
		Aggregate: &query.Aggregate{Function: "count", Column: "*"},
		TimeoutMS: 1000,
	})
	var domain *Error
	if !errors.As(qerr, &domain) || domain.Code != "timeout" {
		t.Fatalf("expected timeout domain error, got %v", qerr)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("timed-out count returned a partial result: %v", result.Rows)
	}
	stats, _ := db.Stats()
	if stats.TimedOut != 1 {
		t.Fatalf("timeout counter wrong: %d", stats.TimedOut)
	}
}

// expiredDeadlineContext is a minimal context whose Err always reports
// DeadlineExceeded, modeling an already-exhausted time budget deterministically.
type expiredDeadlineContext struct{}

func (expiredDeadlineContext) Deadline() (time.Time, bool) { return time.Now(), true }
func (expiredDeadlineContext) Done() <-chan struct{}       { return closedChan }
func (expiredDeadlineContext) Err() error                  { return context.DeadlineExceeded }
func (expiredDeadlineContext) Value(any) any               { return nil }

var closedChan = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

func TestBudgetedSuccessCarriesExecutionAndMatchesLegacy(t *testing.T) {
	db := openTestDB(t)
	seedRows(t, db, 50)
	legacy, err := db.Query(context.Background(), query.Request{Relation: "spectra"})
	if err != nil {
		t.Fatalf("legacy query: %v", err)
	}
	ctx, cancel, err := WithBudget(context.Background(), 5000)
	if err != nil {
		t.Fatalf("budget: %v", err)
	}
	defer cancel()
	budgeted, err := db.Query(ctx, query.Request{Relation: "spectra", TimeoutMS: 5000})
	if err != nil {
		t.Fatalf("budgeted query: %v", err)
	}
	if budgeted.Execution == nil || budgeted.Execution.Status != query.StageComplete {
		t.Fatalf("missing complete execution feedback: %+v", budgeted.Execution)
	}
	if len(budgeted.Rows) != len(legacy.Rows) || budgeted.Matched != legacy.Matched {
		t.Fatalf("budget changed result semantics: budgeted=%d/%d legacy=%d/%d", len(budgeted.Rows), budgeted.Matched, len(legacy.Rows), legacy.Matched)
	}
}

func TestVacuumFloorIncludesExecutorPinsAndTransactions(t *testing.T) {
	db := openTestDB(t)
	seedRows(t, db, 10)
	head := db.sequence

	// An open transaction pins its starting snapshot.
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	txSnap := db.transactions[tx.ID].Snapshot

	// An executor pin at an even lower snapshot must also constrain vacuum.
	pin := db.readPins.acquire(txSnap-1, "test")
	got := db.retentionFloor(head)
	if got != txSnap-1 {
		t.Fatalf("retention floor ignored executor pin: got %d want %d", got, txSnap-1)
	}
	db.readPins.release(pin)

	// After releasing the executor pin, only the transaction pins the floor.
	got = db.retentionFloor(head)
	if got != txSnap {
		t.Fatalf("retention floor should follow transaction only: got %d want %d", got, txSnap)
	}

	// Releasing the transaction lets vacuum reach the requested horizon.
	if err = db.Rollback(tx.ID); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	got = db.retentionFloor(head)
	if got != head {
		t.Fatalf("floor should be head after all readers close: got %d want %d", got, head)
	}
}

func TestCancelingOnePinDoesNotReleaseAnother(t *testing.T) {
	reg := newPinRegistry()
	a := reg.acquire(5, "query")
	b := reg.acquire(3, "query")
	reg.release(a) // cancel query A only
	if reg.active() != 1 {
		t.Fatalf("releasing one pin removed another: %d", reg.active())
	}
	if floor := reg.floor(100); floor != 3 {
		t.Fatalf("surviving pin protection lost: floor=%d", floor)
	}
	reg.release(b)
	if reg.active() != 0 {
		t.Fatalf("pins did not drain: %d", reg.active())
	}
	// Double release is a safe no-op.
	reg.release(a)
}

func TestVacuumCannotCrossActiveTransactionThenAdvancesAfterClose(t *testing.T) {
	db := openTestDB(t)
	seedRows(t, db, 4)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	txSnap := db.transactions[tx.ID].Snapshot
	// Add an update so there is a historical version to reclaim.
	writer, err := db.Begin(false)
	if err != nil {
		t.Fatalf("writer begin: %v", err)
	}
	if err = db.Put(writer.ID, "spectra", datum.Row{"sample": datum.Integer(1), "band": datum.Text("b"), "amplitude": datum.Integer(999)}, false); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err = db.Commit(writer.ID); err != nil {
		t.Fatalf("commit: %v", err)
	}
	head := db.sequence
	pinned, err := db.Vacuum(head)
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if !pinned.Pinned || pinned.Horizon != txSnap {
		t.Fatalf("vacuum crossed active snapshot: %+v", pinned)
	}
	if err = db.Rollback(tx.ID); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	advanced, err := db.Vacuum(head)
	if err != nil {
		t.Fatalf("vacuum after close: %v", err)
	}
	if advanced.Pinned || advanced.Horizon != head {
		t.Fatalf("vacuum did not advance after reader closed: %+v", advanced)
	}
}

func TestStoreScanCancellableStopsAndDoesNotClaimCompleteness(t *testing.T) {
	store := versions.New()
	store.Ensure("spectra")
	mutations := make([]versions.Mutation, 0, 70)
	for i := 1; i <= 70; i++ {
		mutations = append(mutations, versions.Mutation{
			Relation: "spectra",
			Key:      datum.Integer(int64(i)).Key(),
			Row:      datum.Row{"sample": datum.Integer(int64(i))},
		})
	}
	store.Apply(1, mutations)
	calls := 0
	gate := func() error {
		calls++
		return context.Canceled
	}
	partial, err := store.ScanCancellable("spectra", 1, gate)
	if err == nil {
		t.Fatal("expected gate error to surface")
	}
	// The partial map exists but is explicitly not a complete result; callers
	// must treat the error as terminal. We assert it is never mislabeled.
	if calls == 0 {
		t.Fatal("scan gate was never consulted")
	}
	_ = partial
}
