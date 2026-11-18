package versions

import "lamina/internal/datum"

type Mutation struct {
	Relation string    `json:"relation"`
	Key      string    `json:"key"`
	Row      datum.Row `json:"row,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
}
type Version struct {
	Sequence uint64    `json:"sequence"`
	Row      datum.Row `json:"row,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
}
type Store struct {
	Tables map[string]map[string][]Version `json:"tables"`
}

func New() *Store { return &Store{Tables: map[string]map[string][]Version{}} }
func (s *Store) Ensure(name string) {
	if s.Tables[name] == nil {
		s.Tables[name] = map[string][]Version{}
	}
}
func (s *Store) Apply(seq uint64, writes []Mutation) {
	for _, m := range writes {
		s.Ensure(m.Relation)
		s.Tables[m.Relation][m.Key] = append(s.Tables[m.Relation][m.Key], Version{Sequence: seq, Row: datum.Clone(m.Row), Deleted: m.Deleted})
	}
}
func Visible(chain []Version, at uint64) (Version, bool) {
	lo, hi := 0, len(chain)
	for lo < hi {
		mid := (lo + hi) / 2
		if chain[mid].Sequence <= at {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return Version{}, false
	}
	v := chain[lo-1]
	return v, !v.Deleted
}
func (s *Store) Read(table, key string, at uint64) (datum.Row, bool) {
	v, ok := Visible(s.Tables[table][key], at)
	return datum.Clone(v.Row), ok
}
func (s *Store) Latest(table, key string) uint64 {
	v := s.Tables[table][key]
	if len(v) == 0 {
		return 0
	}
	return v[len(v)-1].Sequence
}
func (s *Store) Scan(table string, at uint64) map[string]datum.Row {
	out := map[string]datum.Row{}
	for k, c := range s.Tables[table] {
		if v, ok := Visible(c, at); ok {
			out[k] = datum.Clone(v.Row)
		}
	}
	return out
}

// ScanCancellable is the cooperative variant of Scan. Each visit observes the
// supplied gate so a canceled or budget-exhausted reader stops walking version
// chains instead of materializing rows nobody will receive. It returns the
// rows gathered so far together with the gate's terminal error; callers must
// not treat the partial map as a query result.
func (s *Store) ScanCancellable(table string, at uint64, gate func() error) (map[string]datum.Row, error) {
	out := map[string]datum.Row{}
	rows := s.Tables[table]
	processed := 0
	for k, c := range rows {
		if v, ok := Visible(c, at); ok {
			out[k] = datum.Clone(v.Row)
		}
		processed++
		if processed%queryCheckpointEvery == 0 {
			if err := gate(); err != nil {
				return out, err
			}
		}
	}
	return out, nil
}

// queryCheckpointEvery mirrors query.CheckpointEvery without importing the
// query package (versions is a lower layer).
const queryCheckpointEvery = 64

func (s *Store) Clone() *Store {
	out := New()
	for name, rows := range s.Tables {
		out.Ensure(name)
		for key, chain := range rows {
			for _, v := range chain {
				v.Row = datum.Clone(v.Row)
				out.Tables[name][key] = append(out.Tables[name][key], v)
			}
		}
	}
	return out
}
func (s *Store) Count() (keys, versions int) {
	for _, rows := range s.Tables {
		keys += len(rows)
		for _, v := range rows {
			versions += len(v)
		}
	}
	return
}
