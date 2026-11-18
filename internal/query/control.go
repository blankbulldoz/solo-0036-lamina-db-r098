package query

import (
	"context"
	"fmt"
	"time"
)

// Stage names identify the cancellation boundaries of a query. They are the
// only points at which a budget or cancellation can take effect, so callers
// can explain exactly which phase was stopped. Binding, materialization and
// aggregation are interruptible; the in-memory sort (and the bounded
// projection that follows it) is non-interruptible and only gated on entry.
const (
	StageBinding     = "binding"
	StageMaterialize = "materialize"
	StageAggregate   = "aggregate"
	StageSort        = "sort_boundary"
	// StageProject names the non-interruptible output phase for execution
	// feedback; it has no gate because a fully computed result is never
	// discarded before being sent.
	StageProject  = "project"
	StageComplete = "complete"
)

// CheckpointEvery is the fixed number of processed elements between two
// cancellation checks. Checking every element would tax small queries, while
// a sparse check would let large scans keep running long after the budget
// ended. A constant 64-row batch keeps the per-row cost near zero and bounds
// the stop latency to 64 elements of work plus the current stage boundary.
const CheckpointEvery = 64

// Execution reports how the request budget was spent. It is only attached when
// the request carried a timeout_ms budget; legacy requests without a budget
// keep their exact historical response shape.
type Execution struct {
	Status      string `json:"status"`
	Stage       string `json:"stage"`
	TimeoutMS   int64  `json:"timeout_ms"`
	ElapsedMS   int64  `json:"elapsed_ms"`
	RemainingMS int64  `json:"remaining_ms,omitempty"`
	Checkpoints int    `json:"checkpoints"`
}

// CanceledError means the caller abandoned the request (closed connection or
// canceled context). It never represents a completed query.
type CanceledError struct {
	Stage string
}

func (e *CanceledError) Error() string {
	return fmt.Sprintf("query canceled during %s", e.Stage)
}

// TimeoutError means the explicit time budget expired before the query reached
// StageComplete. Partial aggregates or rows are never returned with it.
type TimeoutError struct {
	Stage   string
	Elapsed time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("query time budget exhausted during %s after %dms", e.Stage, e.Elapsed.Milliseconds())
}

type startKey struct{}

// WithStart records the budget start time on the context. The elapsed
// feedback on a timeout is measured against this single origin so that the
// reported duration matches the timeout_ms the caller supplied.
func WithStart(ctx context.Context, start time.Time) context.Context {
	return context.WithValue(ctx, startKey{}, start)
}

func deadlineElapsed(ctx context.Context) time.Duration {
	if start, ok := ctx.Value(startKey{}).(time.Time); ok && !start.IsZero() {
		return time.Since(start)
	}
	return 0
}

// Gate is invoked at stage entry points and every CheckpointEvery elements.
// It returns a typed terminal error before any result is declared complete.
func Gate(ctx context.Context, stage string) error {
	if ctx == nil {
		return nil
	}
	cause := ctx.Err()
	if cause == nil {
		return nil
	}
	if cause == context.Canceled {
		return &CanceledError{Stage: stage}
	}
	return &TimeoutError{Stage: stage, Elapsed: deadlineElapsed(ctx)}
}
