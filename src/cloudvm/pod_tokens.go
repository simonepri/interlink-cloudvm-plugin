// Keeps the projected service account tokens of running pods fresh: mints a new pod-bound token once the last one is
// past 80% of its lifetime or a day old, as the kubelet does, and has the worker replace the token file.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"
)

// podTokenMaxAge is the age after which the kubelet refreshes a token whatever its lifetime.
const podTokenMaxAge = 24 * time.Hour

// VolumeFile is a file of a pod's file volume that the runner replaces atomically.
type VolumeFile struct {
	Volume string `json:"volume"`
	Path   string `json:"path"`
	Data   []byte `json:"data"`
	Mode   int32  `json:"mode"`
}

// String keeps the file contents out of logs.
func (f VolumeFile) String() string { return fmt.Sprintf("VolumeFile{%s/%s}", f.Volume, f.Path) }

// PodVolumeFilesDeliverer sends replacement volume files to the worker that runs the pod.
type PodVolumeFilesDeliverer func(ctx context.Context, podUID string, files []VolumeFile) error

type podTokenEntry struct {
	pod     PodRef
	tokens  []PodToken
	pending map[string]VolumeFile
}

// PodTokens refreshes the projected service account tokens of the pods it tracks.
type PodTokens struct {
	issuer ServiceAccountTokenIssuer
	now    func() time.Time

	mu   sync.Mutex
	pods map[string]*podTokenEntry
}

// NewPodTokens creates a PodTokens that mints tokens through issuer.
func NewPodTokens(issuer ServiceAccountTokenIssuer) *PodTokens {
	return &PodTokens{issuer: issuer, now: time.Now, pods: map[string]*podTokenEntry{}}
}

// Track registers the pod's token files, which may be none; one with no IssuedAt is minted and delivered on the next
// round.
func (p *PodTokens) Track(pod PodRef, tokens []PodToken) {
	if pod.UID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pods[pod.UID] = &podTokenEntry{pod: pod, tokens: append([]PodToken(nil), tokens...), pending: map[string]VolumeFile{}}
}

// Tracked reports whether the pod's tokens are refreshed.
func (p *PodTokens) Tracked(podUID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.pods[podUID]
	return ok
}

// Forget stops refreshing the pod's tokens.
func (p *PodTokens) Forget(podUID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.pods, podUID)
}

func (t *PodToken) needsRefresh(now time.Time) bool {
	if t.IssuedAt.IsZero() {
		return true
	}
	lifetime := t.ExpiresAt.Sub(t.IssuedAt)
	return now.After(t.IssuedAt.Add(lifetime*8/10)) || now.Sub(t.IssuedAt) > podTokenMaxAge
}

// RefreshOnce mints the tokens that need it and delivers every undelivered file. A pod whose delivery reports
// errPodCredentialsGone stops being tracked.
func (p *PodTokens) RefreshOnce(ctx context.Context, deliver PodVolumeFilesDeliverer) {
	now := p.now()
	type due struct {
		uid string
		pod PodRef
		idx int
		tok PodToken
	}
	var stale []due
	p.mu.Lock()
	uids := make([]string, 0, len(p.pods))
	for uid, e := range p.pods {
		uids = append(uids, uid)
		for i := range e.tokens {
			if e.tokens[i].needsRefresh(now) {
				stale = append(stale, due{uid, e.pod, i, e.tokens[i]})
			}
		}
	}
	p.mu.Unlock()

	for _, d := range stale {
		token, expiresAt, err := p.issuer.CreatePodToken(ctx, d.pod, d.tok.Audience, d.tok.ExpirationSeconds)
		if err != nil {
			log.Printf("[cloudvm] Cannot refresh the service account token of pod %s: %v", d.uid, err)
			continue
		}
		p.mu.Lock()
		if e, ok := p.pods[d.uid]; ok && d.idx < len(e.tokens) {
			e.tokens[d.idx].IssuedAt, e.tokens[d.idx].ExpiresAt = now, expiresAt
			e.pending[d.tok.Volume+"/"+d.tok.Path] = VolumeFile{Volume: d.tok.Volume, Path: d.tok.Path, Data: []byte(token), Mode: d.tok.Mode}
		}
		p.mu.Unlock()
	}

	for _, uid := range uids {
		p.mu.Lock()
		e, ok := p.pods[uid]
		if !ok || len(e.pending) == 0 {
			p.mu.Unlock()
			continue
		}
		files := make([]VolumeFile, 0, len(e.pending))
		for _, f := range e.pending {
			files = append(files, f)
		}
		p.mu.Unlock()

		err := deliver(ctx, uid, files)
		p.mu.Lock()
		switch {
		case errors.Is(err, errPodCredentialsGone):
			delete(p.pods, uid)
		case err != nil:
			log.Printf("[cloudvm] Cannot deliver the service account token of pod %s: %v", uid, err)
		default:
			if e, ok = p.pods[uid]; ok {
				for _, f := range files {
					if cur, same := e.pending[f.Volume+"/"+f.Path]; same && string(cur.Data) == string(f.Data) {
						delete(e.pending, f.Volume+"/"+f.Path)
					}
				}
			}
		}
		p.mu.Unlock()
	}
}

// Run refreshes and delivers tokens every interval until ctx ends.
func (p *PodTokens) Run(ctx context.Context, interval time.Duration, deliver PodVolumeFilesDeliverer) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.RefreshOnce(ctx, deliver)
		}
	}
}

// podVolumeFilesPath is the runner route that replaces files of a pod's file volumes.
func podVolumeFilesPath(podUID string) string {
	return "/containers/volume-files?podUID=" + url.QueryEscape(podUID)
}
