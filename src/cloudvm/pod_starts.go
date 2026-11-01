// Tracks whether each pod's container start may have reached its VM's runner, so a pod deleted before its VM's tunnel
// registered is released without the runner instead of failing the VM's cleanup.
package main

import (
	"errors"
	"sync"
)

// errPodDeleted reports a container start refused because the pod was deleted.
var errPodDeleted = errors.New("the pod was deleted")

// podStarts records, per pod placed by this process, whether a start request may have reached the VM's runner. A pod
// it has no record of, such as one placed before a restart, counts as started.
type podStarts struct {
	mu   sync.Mutex
	pods map[string]*podStart
}

type podStart struct {
	inFlight int  // start requests not yet answered
	reached  bool // a start request reached the runner, or failed in a way that may have reached it
	deleted  bool // the pod was deleted, so no start request may be sent
}

func newPodStarts() *podStarts {
	return &podStarts{pods: make(map[string]*podStart)}
}

// begin records a start request about to be sent, unless the pod was deleted, which deleted reports under the lock so
// a delete that tombstones the pod first never misses the request.
func (p *podStarts) begin(uid string, deleted func() bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.pods[uid]
	if st == nil {
		st = &podStart{}
		p.pods[uid] = st
	}
	if st.deleted || deleted() {
		st.deleted = true
		return false
	}
	st.inFlight++
	return true
}

// end records the answer to a start request: sent is false only when the request never left the plugin.
func (p *podStarts) end(uid string, sent bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if st := p.pods[uid]; st != nil {
		st.inFlight--
		st.reached = st.reached || sent
	}
}

// delete refuses every later start request of the pod and reports whether none of its start requests can have reached
// the runner, which means the VM has no container to remove.
func (p *podStarts) delete(uid string) (neverStarted bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.pods[uid]
	if st == nil {
		return false
	}
	st.deleted = true
	return !st.reached && st.inFlight == 0
}

// forget drops the record of a deleted pod whose container the plugin removed or released.
func (p *podStarts) forget(uid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.pods, uid)
}
