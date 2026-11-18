package engine

import "sync"

// ReadPin is one reader's claim on a snapshot. It is the shared ownership
// primitive that lets request termination and version reclamation cooperate:
// vacuum may not advance its horizon past any live pin, while canceling a
// query releases only that query's pin and can never remove the history
// protection another active transaction or executor still relies on.
type ReadPin struct {
	id       uint64
	snapshot uint64
	kind     string
}

// pinRegistry tracks in-flight executor reads separately from long-lived
// transactions. Transactions already live in DB.transactions, but an ad-hoc
// query (no transaction) or a query whose HTTP session closes mid-flight still
// needs its snapshot protected until the executor stops touching the store.
type pinRegistry struct {
	mu   sync.Mutex
	next uint64
	pins map[uint64]ReadPin
}

func newPinRegistry() *pinRegistry {
	return &pinRegistry{pins: map[uint64]ReadPin{}}
}

// acquire registers a reader at snapshot. The caller must hold DB.mu so that
// the pin is visible to any vacuum decision computed under the same lock.
func (r *pinRegistry) acquire(snapshot uint64, kind string) ReadPin {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	pin := ReadPin{id: r.next, snapshot: snapshot, kind: kind}
	r.pins[pin.id] = pin
	return pin
}

// release removes exactly one pin. Releasing a pin that was already released
// is a no-op, so a deferred release racing with an explicit one is safe. It
// never touches another reader's pin.
func (r *pinRegistry) release(pin ReadPin) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.pins, pin.id)
}

// floor returns the lowest snapshot any live executor still needs. With no
// executor pins it returns ceiling (a value no constraint can fall below),
// which lets the caller combine it with other floors using a simple min.
func (r *pinRegistry) floor(ceiling uint64) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	floor := ceiling
	for _, pin := range r.pins {
		if pin.snapshot < floor {
			floor = pin.snapshot
		}
	}
	return floor
}

func (r *pinRegistry) active() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pins)
}

// retentionFloor is the single horizon rule shared by vacuum and any future
// reclamation entry point. A horizon cannot pass:
//   - an open transaction's snapshot, nor
//   - an in-flight executor pin's snapshot.
//
// Caller holds DB.mu.
func (d *DB) retentionFloor(requested uint64) uint64 {
	horizon := requested
	for _, tx := range d.transactions {
		if tx.Snapshot < horizon {
			horizon = tx.Snapshot
		}
	}
	if executorFloor := d.readPins.floor(horizon); executorFloor < horizon {
		horizon = executorFloor
	}
	return horizon
}
