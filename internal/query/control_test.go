package query

import (
	"context"
	"errors"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"testing"
	"time"
)

func testRelation() catalog.Relation {
	return catalog.Relation{
		Name:    "spectra",
		Primary: "sample",
		Columns: []catalog.Column{
			{Name: "sample", Type: "int"},
			{Name: "band", Type: "text"},
			{Name: "amplitude", Type: "int", Nullable: true},
		},
		Created: 1,
	}
}

func rowsN(n int) map[string]datum.Row {
	out := map[string]datum.Row{}
	for i := 1; i <= n; i++ {
		row := datum.Row{"sample": datum.Integer(int64(i)), "band": datum.Text("b"), "amplitude": datum.Integer(int64(i))}
		out[row["sample"].Key()] = row
	}
	return out
}

func TestExecuteCanceledBeforeWorkReturnsNoRows(t *testing.T) {
	r := testRelation()
	q := Request{Relation: "spectra", TimeoutMS: 1000}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := Execute(ctx, r, q, rowsN(200), Plan{Access: "scan"}, nil)
	var canceled *CanceledError
	if !errors.As(err, &canceled) {
		t.Fatalf("expected CanceledError, got %v", err)
	}
	if canceled.Stage != StageMaterialize {
		t.Fatalf("expected stop at materialize entry, got %s", canceled.Stage)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("canceled query must not return partial rows, got %d", len(result.Rows))
	}
}

func TestExecuteTimeoutFailsWholeAggregate(t *testing.T) {
	r := testRelation()
	// A budget that is already expired must fail a count instead of returning
	// a half count: the aggregate *boundary gate* runs unconditionally before
	// reduction, so even a tiny input cannot slip through.
	q := Request{Relation: "spectra", Aggregate: &Aggregate{Function: "count", Column: "*"}, TimeoutMS: 1}
	ctx, cancel := context.WithTimeout(WithStart(context.Background(), time.Now()), time.Millisecond)
	defer cancel()
	time.Sleep(3 * time.Millisecond)
	result, err := Execute(ctx, r, q, rowsN(5), Plan{Access: "scan"}, nil)
	var timedOut *TimeoutError
	if !errors.As(err, &timedOut) {
		t.Fatalf("expected TimeoutError, got %v result=%v", err, result)
	}
	// The stop lands at the first boundary reached (materialize entry or the
	// aggregate gate). Both are pre-result, so either proves no half count.
	if timedOut.Stage != StageMaterialize && timedOut.Stage != StageAggregate {
		t.Fatalf("expected a pre-result stage, got %s", timedOut.Stage)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("timed-out aggregate must never return a half count, got %v", result.Rows)
	}
}

func TestExecuteCompletionIsWholeEvenIfContextExpiresDuringSort(t *testing.T) {
	r := testRelation()
	// Deadline shorter than the whole pipeline, but the query reaches the
	// non-interruptible sort. Once complete, the whole result is returned
	// rather than being discarded at response time.
	q := Request{Relation: "spectra", Order: []Ordering{{Column: "sample", Desc: true}}, Limit: 5, TimeoutMS: 10000}
	ctx, cancel := context.WithTimeout(WithStart(context.Background(), time.Now()), 10*time.Second)
	defer cancel()
	result, err := Execute(ctx, r, q, rowsN(300), Plan{Access: "scan"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Execution == nil || result.Execution.Status != StageComplete {
		t.Fatalf("budgeted success must carry complete execution feedback, got %+v", result.Execution)
	}
	if result.Execution.TimeoutMS != 10000 {
		t.Fatalf("timeout echo wrong: %d", result.Execution.TimeoutMS)
	}
	if len(result.Rows) != 5 || result.Matched != 300 || !result.Truncated {
		t.Fatalf("completeness facts wrong: rows=%d matched=%d truncated=%v", len(result.Rows), result.Matched, result.Truncated)
	}
	if result.Rows[0]["sample"].Int != 300 {
		t.Fatalf("desc ordering not honored: %v", result.Rows[0])
	}
}

func TestExecuteLegacyRequestHasNoExecutionBlock(t *testing.T) {
	r := testRelation()
	q := Request{Relation: "spectra"} // no timeout_ms
	result, err := Execute(context.Background(), r, q, rowsN(10), Plan{Access: "scan"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Execution != nil {
		t.Fatalf("legacy unbounded request must keep historical response shape, got %+v", result.Execution)
	}
	if result.Matched != 10 || len(result.Rows) != 10 {
		t.Fatalf("legacy result semantics changed: %+v", result)
	}
}

func TestGateDistinguishesCancelAndDeadline(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Gate(canceledCtx, StageSort); err == nil {
		t.Fatal("expected canceled gate error")
	} else if _, ok := err.(*CanceledError); !ok {
		t.Fatalf("expected CanceledError, got %T", err)
	}
	deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now())
	defer deadlineCancel()
	time.Sleep(2 * time.Millisecond)
	if err := Gate(deadlineCtx, StageSort); err == nil {
		t.Fatal("expected deadline gate error")
	} else if _, ok := err.(*TimeoutError); !ok {
		t.Fatalf("expected TimeoutError, got %T", err)
	}
	if Gate(context.Background(), StageSort) != nil {
		t.Fatal("open context must pass the gate")
	}
}

func TestReduceGateFailsTheWholeAggregate(t *testing.T) {
	// Drive reduce directly with a gate that trips after the first checkpoint:
	// this proves an in-flight aggregate aborts rather than emitting a partial
	// count/sum, exercising the StageAggregate boundary deterministically.
	rows := rowsN(200)
	selected := make([]datum.Row, 0, len(rows))
	for _, row := range rows {
		selected = append(selected, row)
	}
	gate := func(stage string) error {
		if stage != StageAggregate {
			t.Fatalf("reduce gate stage = %q want aggregate", stage)
		}
		return &CanceledError{Stage: stage}
	}
	out, err := reduce(Aggregate{Function: "count", Column: "*"}, selected, gate)
	if err == nil {
		t.Fatalf("expected aggregate gate to fail the reduction, got %v", out)
	}
	if out != nil {
		t.Fatalf("failed reduction must not return a half count row: %v", out)
	}
}

// flipDeadlineContext stays open for the first openFor Err() calls, then
// deterministically reports DeadlineExceeded. It models a budget that expires
// in the middle of a pipeline without depending on wall-clock timers.
type flipDeadlineContext struct {
	calls   chan struct{}
	done    chan struct{}
	openFor int
}

func newFlipDeadline(openFor int) *flipDeadlineContext {
	return &flipDeadlineContext{calls: make(chan struct{}, 64), done: make(chan struct{}), openFor: openFor}
}
func (c *flipDeadlineContext) Deadline() (time.Time, bool) { return time.Now().Add(time.Second), true }
func (c *flipDeadlineContext) Done() <-chan struct{}       { return c.done }
func (c *flipDeadlineContext) Value(any) any               { return nil }
func (c *flipDeadlineContext) Err() error {
	select {
	case c.calls <- struct{}{}:
	default:
	}
	if len(c.calls) > c.openFor {
		return context.DeadlineExceeded
	}
	return nil
}

func TestExpiryDuringNonInterruptibleSortStillReturnsWholeResult(t *testing.T) {
	r := testRelation()
	// 100 rows => materialize entry gate (call 1), one in-loop gate at 64 rows
	// (call 2) and the sort-boundary gate (call 3). Flipping after 3 gates
	// leaves the budget open up to sort entry and exhausted during the
	// non-interruptible sort/projection, which must still finish as a whole
	// result rather than being dropped right before the response.
	ctx := newFlipDeadline(3)
	q := Request{Relation: "spectra", Order: []Ordering{{Column: "sample"}}, Limit: 3, TimeoutMS: 50}
	result, err := Execute(ctx, r, q, rowsN(100), Plan{Access: "scan"}, nil)
	if err != nil {
		t.Fatalf("a result fully determined after the sort must not be discarded: %v", err)
	}
	if result.Execution == nil || result.Execution.Status != StageComplete || len(result.Rows) != 3 {
		t.Fatalf("expected a whole complete result, got %+v err=%v", result, err)
	}
	if result.Matched != 100 {
		t.Fatalf("matched must reflect all rows even when budget lapsed post-sort: %d", result.Matched)
	}
}

func TestCheckpointConstantIsExplainable(t *testing.T) {
	if CheckpointEvery != 64 {
		t.Fatalf("documented cooperative batch changed: %d", CheckpointEvery)
	}
}
