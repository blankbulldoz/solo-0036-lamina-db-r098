package query

import (
	"context"
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"math"
	"sort"
	"time"
)

// Execute runs a fully bound request against rows already materialized at one
// snapshot. Cancellation and budgets are cooperative: they are observed only
// at well-defined boundaries (stage entry and every CheckpointEvery elements),
// never inside the non-interruptible in-memory sort. A result is returned only
// when StageComplete is reached; any earlier stop is a typed error and never
// carries partial rows, count or sum.
func Execute(ctx context.Context, r catalog.Relation, q Request, rows map[string]datum.Row, plan Plan, keys []string) (Result, error) {
	result := Result{Rows: []datum.Row{}, Plan: plan}
	checkpoints := 0
	gate := func(stage string) error {
		checkpoints++
		return Gate(ctx, stage)
	}
	// Stage entry: reject a request that was already canceled before work.
	if err := gate(StageMaterialize); err != nil {
		return result, err
	}
	if plan.Access == "scan" {
		keys = datum.Keys(rows)
	}
	selected := make([]datum.Row, 0)
	processed := 0
	for _, key := range keys {
		row, ok := rows[key]
		if !ok {
			continue
		}
		result.Scanned++
		if Matches(row, q.Where) {
			selected = append(selected, row)
		}
		processed++
		// Amortized check: bounded stop latency without per-row context cost.
		if processed%CheckpointEvery == 0 {
			if err := gate(StageMaterialize); err != nil {
				return result, err
			}
		}
	}
	// Boundary after materialization: aggregates cannot return half counts and
	// sorting is non-interruptible, so both require the budget to still be open.
	if q.Aggregate != nil {
		if err := gate(StageAggregate); err != nil {
			return result, err
		}
		row, err := reduce(*q.Aggregate, selected, gate)
		if err != nil {
			return result, err
		}
		result.Matched = len(selected)
		result.Rows = append(result.Rows, row)
		return finish(ctx, q, &result, checkpoints)
	}
	if err := gate(StageSort); err != nil {
		return result, err
	}
	// Non-interruptible stage: no checks inside sort. All selected rows are
	// already in memory, so the work is bounded by the admitted result size.
	sort.Slice(selected, func(i, j int) bool {
		for _, o := range q.Order {
			n := datum.Order(selected[i][o.Column], selected[j][o.Column])
			if n != 0 {
				if o.Desc {
					return n > 0
				}
				return n < 0
			}
		}
		return datum.Order(selected[i][r.Primary], selected[j][r.Primary]) < 0
	})
	limit := q.Limit
	if limit == 0 {
		limit = 1000
	}
	// Matched is the pre-limit count of matching rows and is fixed before any
	// truncation, so a row cap never changes its meaning.
	result.Matched = len(selected)
	if len(selected) > limit {
		selected = selected[:limit]
		result.Truncated = true
	}
	// No fatal gate here: once the non-interruptible sort finished, the result
	// is fully determined and projection is a bounded deterministic mapping
	// (at most the admitted limit rows). Discarding it now would drop a
	// completed result right before the response, so the query runs to
	// StageComplete. The last *cancellable* point is the sort boundary above.
	for _, row := range selected {
		if len(q.Columns) == 0 {
			result.Rows = append(result.Rows, datum.Clone(row))
		} else {
			projection := datum.Row{}
			for _, name := range q.Columns {
				projection[name] = row[name]
			}
			result.Rows = append(result.Rows, projection)
		}
	}
	return finish(ctx, q, &result, checkpoints)
}

// finish marks the boundary at which the result becomes complete. A context
// that expired during the final non-interruptible stretch does not discard a
// fully computed result; a result is returned only here and is always whole.
func finish(ctx context.Context, q Request, result *Result, checkpoints int) (Result, error) {
	if q.TimeoutMS > 0 {
		deadline, ok := ctx.Deadline()
		elapsed := deadlineElapsed(ctx)
		execution := &Execution{Status: StageComplete, Stage: StageComplete, TimeoutMS: q.TimeoutMS, ElapsedMS: elapsed.Milliseconds(), Checkpoints: checkpoints}
		if ok {
			if remaining := time.Until(deadline).Milliseconds(); remaining > 0 {
				execution.RemainingMS = remaining
			}
		}
		result.Execution = execution
	}
	return *result, nil
}

func reduce(a Aggregate, rows []datum.Row, gate func(string) error) (datum.Row, error) {
	var total int64
	var count int64
	processed := 0
	for _, row := range rows {
		v := row[a.Column]
		if a.Column != "" && a.Column != "*" && v.Null() {
			continue
		}
		count++
		if a.Function == "sum" {
			if v.Int > 0 && total > math.MaxInt64-v.Int || v.Int < 0 && total < math.MinInt64-v.Int {
				return nil, fmt.Errorf("sum overflow")
			}
			total += v.Int
		}
		processed++
		if processed%CheckpointEvery == 0 {
			// Stopping here means the whole aggregate fails: no half count/sum.
			if err := gate(StageAggregate); err != nil {
				return nil, err
			}
		}
	}
	if a.Function == "count" {
		return datum.Row{"count": datum.Integer(count)}, nil
	}
	if count == 0 {
		return datum.Row{"sum": datum.Value{Kind: "null"}}, nil
	}
	return datum.Row{"sum": datum.Integer(total)}, nil
}
