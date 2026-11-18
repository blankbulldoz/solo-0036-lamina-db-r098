package transport

import (
	"lamina/internal/datum"
	"net/http"
)

func (s *Server) begin(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ReadOnly bool `json:"read_only"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.Begin(in.ReadOnly)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 201, out)
}
func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Relation   string    `json:"relation"`
		Row        datum.Row `json:"row"`
		InsertOnly bool      `json:"insert_only"`
	}
	if !decode(w, r, &in) {
		return
	}
	if err := s.db.Put(r.PathValue("tx"), in.Relation, in.Row, in.InsertOnly); err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]string{"state": "staged"})
}
func (s *Server) deleteRow(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Relation string      `json:"relation"`
		Key      datum.Value `json:"key"`
	}
	if !decode(w, r, &in) {
		return
	}
	if err := s.db.Delete(r.PathValue("tx"), in.Relation, in.Key); err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]string{"state": "staged"})
}
func (s *Server) commit(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.Commit(r.PathValue("tx"))
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
func (s *Server) rollback(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Rollback(r.PathValue("tx")); err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]string{"state": "rolled_back"})
}
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Relation    string      `json:"relation"`
		Key         datum.Value `json:"key"`
		Transaction string      `json:"transaction"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.Get(r.Context(), in.Transaction, in.Relation, in.Key)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]any{"row": out})
}
