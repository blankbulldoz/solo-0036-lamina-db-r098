package transport

import (
	"lamina/internal/engine"
	"lamina/internal/query"
	"lamina/internal/syntax"
	"net/http"
)

// runQuery wires the HTTP lifecycle into one query. r.Context() closes when the
// client disconnects or the server shuts down; timeout_ms adds an explicit
// cooperative budget on top. The cancel func is always invoked so a request
// that finishes before its deadline does not retain the timer.
func (s *Server) runQuery(w http.ResponseWriter, r *http.Request, q query.Request) {
	ctx, cancel, err := engine.WithBudget(r.Context(), q.TimeoutMS)
	if err != nil {
		respondError(w, err)
		return
	}
	defer cancel()
	out, err := s.db.Query(ctx, q)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	var in query.Request
	if !decode(w, r, &in) {
		return
	}
	s.runQuery(w, r, in)
}

func (s *Server) sql(w http.ResponseWriter, r *http.Request) {
	var in struct {
		SQL         string  `json:"sql"`
		Transaction string  `json:"transaction"`
		At          *uint64 `json:"at"`
		TimeoutMS   int64   `json:"timeout_ms,omitempty"`
	}
	if !decode(w, r, &in) {
		return
	}
	q, err := syntax.Parse(in.SQL)
	if err != nil {
		respondError(w, &engine.Error{Code: "invalid", Message: err.Error()})
		return
	}
	q.Transaction = in.Transaction
	q.At = in.At
	// The same budget field drives both entries so SQL and structured requests
	// observe identical cancellation and completeness semantics.
	q.TimeoutMS = in.TimeoutMS
	s.runQuery(w, r, q)
}
