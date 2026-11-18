package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/engine"
	"lamina/internal/query"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// contextWithCanceled returns a context already canceled, mirroring a client
// that closes the connection before the handler produces a response.
func contextWithCanceled(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	cancel()
	return ctx, cancel
}

func newTestServer(t *testing.T) (*Server, *engine.DB) {
	t.Helper()
	db, err := engine.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rel := catalog.Relation{
		Name:    "spectra",
		Primary: "sample",
		Columns: []catalog.Column{
			{Name: "sample", Type: "int"},
			{Name: "band", Type: "text"},
			{Name: "amplitude", Type: "int", Nullable: true},
		},
	}
	if _, err = db.CreateRelation(rel); err != nil {
		t.Fatalf("create relation: %v", err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for i := 1; i <= 200; i++ {
		row := datum.Row{"sample": datum.Integer(int64(i)), "band": datum.Text("b"), "amplitude": datum.Integer(int64(i))}
		if err = db.Put(tx.ID, "spectra", row, true); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if _, err = db.Commit(tx.ID); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return New(db), db
}

func postJSON(t *testing.T, h http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return out
}

func TestHTTPCanceledRequestReturns499WithoutPartialRows(t *testing.T) {
	s, _ := newTestServer(t)
	// A canceled request context mirrors a client closing the connection. The
	// handler must surface a typed 499 and never a partial aggregate/result.
	req := httptest.NewRequest(http.MethodPost, "/v1/lamina/query",
		bytes.NewReader(mustJSON(map[string]any{"relation": "spectra", "timeout_ms": 5000})))
	req.Header.Set("Content-Type", "application/json")
	ctx, cancel := contextWithCanceled(req.Context())
	defer cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != 499 {
		t.Fatalf("canceled request status = %d want 499 body=%s", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	errBody, ok := body["error"].(map[string]any)
	if !ok || errBody["code"] != "canceled" {
		t.Fatalf("canceled body wrong: %v", body)
	}
	if _, hasRows := body["rows"]; hasRows {
		t.Fatalf("canceled response must not carry rows: %v", body)
	}
}

func TestHTTPTimeoutFieldRejectedOutOfRange(t *testing.T) {
	s, _ := newTestServer(t)
	rec := postJSON(t, s, "/v1/lamina/query", map[string]any{"relation": "spectra", "timeout_ms": 40000})
	if rec.Code != 400 {
		t.Fatalf("out-of-range budget status = %d want 400", rec.Code)
	}
}

func TestHTTPSQLAndStructuredShareBudgetAndResultSemantics(t *testing.T) {
	s, _ := newTestServer(t)
	structured := postJSON(t, s, "/v1/lamina/query", map[string]any{
		"relation":   "spectra",
		"limit":      5,
		"timeout_ms": 5000,
		"order":      []any{map[string]any{"column": "sample", "desc": true}},
	})
	if structured.Code != 200 {
		t.Fatalf("structured budgeted query failed: %s", structured.Body.String())
	}
	sql := postJSON(t, s, "/v1/lamina/sql", map[string]any{
		"sql":        "SELECT * FROM spectra ORDER BY sample DESC LIMIT 5;",
		"timeout_ms": 5000,
	})
	if sql.Code != 200 {
		t.Fatalf("sql budgeted query failed: %s", sql.Body.String())
	}
	a := decodeBody(t, structured)
	b := decodeBody(t, sql)
	if !jsonEqual(a["rows"], b["rows"]) {
		t.Fatalf("SQL and structured rows diverged: %v vs %v", a["rows"], b["rows"])
	}
	exec, ok := b["execution"].(map[string]any)
	if !ok || exec["status"] != "complete" {
		t.Fatalf("SQL budgeted success missing complete execution block: %v", b)
	}
	// Legacy request without budget has no execution block.
	legacy := postJSON(t, s, "/v1/lamina/query", map[string]any{"relation": "spectra", "limit": 5})
	legacyBody := decodeBody(t, legacy)
	if _, present := legacyBody["execution"]; present {
		t.Fatalf("legacy request shape changed: %v", legacyBody)
	}
}

func TestHTTPNoHalfAggregateOnExpiredBudget(t *testing.T) {
	s, _ := newTestServer(t)
	// A zero-row immediate expiry is not directly expressible through the
	// public JSON budget (server derives a fresh timer), so the contract is
	// asserted end-to-end via an already-canceled context on an aggregate.
	req := httptest.NewRequest(http.MethodPost, "/v1/lamina/sql",
		bytes.NewReader(mustJSON(map[string]any{"sql": "SELECT count(*) FROM spectra;", "timeout_ms": 5000})))
	req.Header.Set("Content-Type", "application/json")
	ctx, cancel := contextWithCanceled(req.Context())
	defer cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != 499 {
		t.Fatalf("status = %d want 499", rec.Code)
	}
	body := decodeBody(t, rec)
	if _, hasRows := body["rows"]; hasRows {
		t.Fatalf("failed aggregate must not return a half count: %v", body)
	}
}

func TestHTTPBudgetTimeoutReturns408(t *testing.T) {
	s, _ := newTestServer(t)
	server := httptest.NewServer(s)
	defer server.Close()
	// A 1ms budget against a full scan will normally expire mid-pipeline. The
	// exact expiry instant is timing-dependent, so retry until a 408 is seen;
	// every non-408 response must still be a complete 200, never a partial one.
	saw408 := false
	for attempt := 0; attempt < 20 && !saw408; attempt++ {
		raw := mustJSON(map[string]any{"relation": "spectra", "timeout_ms": 1, "force_scan": true})
		resp, err := http.Post(server.URL+"/v1/lamina/query", "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusRequestTimeout:
			body := map[string]any{}
			_ = json.Unmarshal(buf.Bytes(), &body)
			errBody, ok := body["error"].(map[string]any)
			if !ok || errBody["code"] != "timeout" {
				t.Fatalf("408 body wrong: %s", buf.String())
			}
			if _, hasRows := body["rows"]; hasRows {
				t.Fatalf("408 must not carry partial rows: %s", buf.String())
			}
			saw408 = true
		case http.StatusOK:
			// A race where the query finished inside 1ms: result must be whole.
			body := map[string]any{}
			_ = json.Unmarshal(buf.Bytes(), &body)
			if exec, ok := body["execution"].(map[string]any); !ok || exec["status"] != "complete" {
				t.Fatalf("fast 200 lacked complete execution block: %s", buf.String())
			}
		default:
			t.Fatalf("unexpected status %d body=%s", resp.StatusCode, buf.String())
		}
	}
	if !saw408 {
		t.Skip("1ms budget never expired on this fast machine; 408 path covered by engine unit test")
	}
}

func TestConcurrentCancellationCommitsVacuumAndReads(t *testing.T) {
	s, db := newTestServer(t)
	server := httptest.NewServer(s)
	defer server.Close()

	// A long-lived read-only transaction pins an early snapshot for the whole
	// test, proving that canceled scans cannot remove its history protection.
	pinnedTx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("begin pinned tx: %v", err)
	}
	pinnedSnapshot := pinnedTx.Snapshot

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Worker 1: many budgeted scans, some canceled, each result either a whole
	// 200 or a typed error, never a partial count.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			raw := mustJSON(map[string]any{"relation": "spectra", "timeout_ms": 2, "force_scan": true,
				"aggregate": map[string]any{"function": "count", "column": "*"}})
			resp, perr := http.Post(server.URL+"/v1/lamina/query", "application/json", bytes.NewReader(raw))
			if perr != nil {
				continue
			}
			buf := new(bytes.Buffer)
			_, _ = buf.ReadFrom(resp.Body)
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusOK:
				body := map[string]any{}
				_ = json.Unmarshal(buf.Bytes(), &body)
				rows, ok := body["rows"].([]any)
				if !ok || len(rows) != 1 {
					t.Errorf("successful count must be a whole single row, got %s", buf.String())
					return
				}
			case http.StatusRequestTimeout, 499:
				// acceptable terminal stop, no partial body
			default:
				t.Errorf("unexpected scan status %d: %s", resp.StatusCode, buf.String())
				return
			}
		}
	}()
	// Worker 2: concurrent commits keep advancing the head sequence. The full
	// set must become visible to the follow-up read.
	writerDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(writerDone)
		for i := 0; i < 20; i++ {
			tx, berr := db.Begin(false)
			if berr != nil {
				t.Errorf("begin writer: %v", berr)
				return
			}
			key := int64(1000 + i)
			payload := map[string]any{
				"relation":    "spectra",
				"insert_only": true,
				"row":         map[string]any{"sample": key, "band": "c", "amplitude": key},
			}
			raw, _ := json.Marshal(payload)
			req := httptest.NewRequest(http.MethodPost,
				"/v1/lamina/transactions/"+tx.ID+"/put", bytes.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("put failed: %s", rec.Body.String())
				_ = db.Rollback(tx.ID)
				return
			}
			if _, cerr := db.Commit(tx.ID); cerr != nil {
				t.Errorf("commit: %v", cerr)
				return
			}
		}
	}()
	// Worker 3: vacuum cannot pass the pinned transaction while it stays open.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			select {
			case <-stop:
				return
			default:
			}
			stats, _ := db.Stats()
			if res, verr := db.Vacuum(stats.Sequence); verr == nil && res.Horizon < pinnedSnapshot {
				t.Errorf("vacuum crossed pinned snapshot: %+v", res)
				return
			}
		}
	}()
	// Let the workers interleave, then stop the bounded loops and join.
	time.Sleep(60 * time.Millisecond)
	close(stop)
	wg.Wait()
	<-writerDone

	// The pinned transaction still reads its original snapshot through all the
	// canceled scans and concurrent commits.
	pinnedResult, err := db.Query(context.Background(), mapToRequest(pinnedTx.ID))
	if err != nil {
		t.Fatalf("pinned transaction read after churn: %v", err)
	}
	if pinnedResult.Matched != 200 {
		t.Fatalf("pinned snapshot lost its history protection: matched=%d", pinnedResult.Matched)
	}

	// After releasing the pin, vacuum may advance and a follow-up current read
	// observes every committed write (200 seed + 20 new).
	if err = db.Rollback(pinnedTx.ID); err != nil {
		t.Fatalf("rollback pinned: %v", err)
	}
	stats, _ := db.Stats()
	if _, err = db.Vacuum(stats.Sequence); err != nil {
		t.Fatalf("final vacuum: %v", err)
	}
	current, err := db.Query(context.Background(), query.Request{Relation: "spectra", ForceScan: true})
	if err != nil {
		t.Fatalf("follow-up current read: %v", err)
	}
	if current.Matched != 220 {
		t.Fatalf("follow-up read missed committed writes: matched=%d want 220", current.Matched)
	}
}

func mapToRequest(tx string) query.Request {
	return query.Request{Relation: "spectra", Transaction: tx, ForceScan: true}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func jsonEqual(a, b any) bool {
	ba, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(ba, bb)
}
