// Test doubles: an in-memory claim API, a fake plugin pod API over httptest, and a clientset reactor that counts API
// calls by verb and resource.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// fakeClaims is an in-memory nodeclaim.API whose List can be made to fail.
type fakeClaims struct {
	mu     sync.Mutex
	claims map[string]nodeclaim.NodeClaim
	fail   bool
	lists  atomic.Int64
}

func newFakeClaims(claims ...nodeclaim.NodeClaim) *fakeClaims {
	f := &fakeClaims{claims: make(map[string]nodeclaim.NodeClaim)}
	for _, c := range claims {
		f.claims[c.Name] = c
	}
	return f
}

func (f *fakeClaims) set(c nodeclaim.NodeClaim) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims[c.Name] = c
}

func (f *fakeClaims) remove(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.claims, name)
}

func (f *fakeClaims) setFail(fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = fail
}

func (f *fakeClaims) Catalog(context.Context, []string) (*nodeclaim.Catalog, error) {
	return &nodeclaim.Catalog{}, nil
}

func (f *fakeClaims) Create(_ context.Context, req *nodeclaim.CreateRequest) (*nodeclaim.NodeClaim, error) {
	c := nodeclaim.NodeClaim{Name: req.Name, State: nodeclaim.StateLaunching}
	f.set(c)
	return &c, nil
}

func (f *fakeClaims) Get(_ context.Context, name string) (*nodeclaim.NodeClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.claims[name]
	if !ok {
		return nil, &nodeclaim.APIError{Status: http.StatusNotFound, Reason: nodeclaim.ReasonNotFound}
	}
	return &c, nil
}

func (f *fakeClaims) GetByProviderID(ctx context.Context, id string) (*nodeclaim.NodeClaim, error) {
	items, _ := f.List(ctx)
	for i := range items {
		if items[i].ProviderID == id {
			return &items[i], nil
		}
	}
	return nil, &nodeclaim.APIError{Status: http.StatusNotFound, Reason: nodeclaim.ReasonNotFound}
}

func (f *fakeClaims) List(context.Context) ([]nodeclaim.NodeClaim, error) {
	f.lists.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return nil, errors.New("store unavailable")
	}
	out := make([]nodeclaim.NodeClaim, 0, len(f.claims))
	for _, c := range f.claims {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (f *fakeClaims) Delete(_ context.Context, name string) error {
	f.remove(name)
	return nil
}

// fakePlugin serves /create, /delete, /status and /getLogs and records what it received.
type fakePlugin struct {
	mu        sync.Mutex
	created   map[string]createRequest // pod UID -> request
	deleted   []string
	running   map[string]bool // pod UID -> container running
	polled    map[string]bool // pod UIDs a /status request named
	statusReq atomic.Int64
	server    *httptest.Server
}

func newFakePlugin(t *testing.T) *fakePlugin {
	t.Helper()
	p := &fakePlugin{created: make(map[string]createRequest), running: make(map[string]bool), polled: make(map[string]bool)}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /create", func(w http.ResponseWriter, r *http.Request) {
		var req createRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Pod == nil {
			http.Error(w, "bad pod", http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.created[string(req.Pod.UID)] = req
		p.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"PodUID": string(req.Pod.UID), "PodJID": "i-1:" + string(req.Pod.UID)})
	})
	mux.HandleFunc("POST /delete", func(_ http.ResponseWriter, r *http.Request) {
		var req createRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		p.mu.Lock()
		if req.Pod != nil {
			p.deleted = append(p.deleted, string(req.Pod.UID))
		}
		p.mu.Unlock()
	})
	mux.HandleFunc("POST /status", func(w http.ResponseWriter, r *http.Request) {
		p.statusReq.Add(1)
		var pods []corev1.Pod
		if err := json.NewDecoder(r.Body).Decode(&pods); err != nil {
			http.Error(w, "bad status body", http.StatusBadRequest)
			return
		}
		out := make([]PodStatus, 0, len(pods))
		p.mu.Lock()
		for i := range pods {
			pod := &pods[i]
			p.polled[string(pod.UID)] = true
			st := PodStatus{UID: string(pod.UID), Name: pod.Name, Namespace: pod.Namespace, PodIP: "10.9.0.1"}
			for _, c := range pod.Spec.Containers {
				cs := ContainerStatus{Name: c.Name, Image: c.Image}
				if p.running[string(pod.UID)] {
					cs.Ready = true
					cs.State.Running = &RunningState{StartedAt: time.Unix(1700000000, 0)}
				} else {
					cs.State.Waiting = &WaitingState{Reason: "ContainerCreating"}
				}
				st.Containers = append(st.Containers, cs)
			}
			out = append(out, st)
		}
		p.mu.Unlock()
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("POST /getLogs", func(w http.ResponseWriter, r *http.Request) {
		var req logsRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_, _ = w.Write([]byte("hello from " + req.PodName + "/" + req.ContainerName + "\n"))
	})
	p.server = httptest.NewServer(mux)
	t.Cleanup(p.server.Close)
	return p
}

// callCounter counts clientset calls by "verb resource[/subresource]".
type callCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func countCalls(cs *fake.Clientset) *callCounter {
	c := &callCounter{counts: make(map[string]int)}
	cs.PrependReactor("*", "*", func(a k8stesting.Action) (bool, runtime.Object, error) {
		key := a.GetVerb() + " " + a.GetResource().Resource
		if sub := a.GetSubresource(); sub != "" {
			key += "/" + sub
		}
		c.mu.Lock()
		c.counts[key]++
		c.mu.Unlock()
		return false, nil, nil
	})
	return c
}

func (c *callCounter) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.counts))
	for k, v := range c.counts {
		out[k] = v
	}
	return out
}

func (c *callCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = make(map[string]int)
}
