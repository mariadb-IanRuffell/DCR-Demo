package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type backend struct {
	addr     string // ip:port
	healthy  atomic.Bool
	inflight atomic.Int64
	win      Window
}

type remote struct {
	dc      string
	addr    string // wan:117xx
	healthy atomic.Bool
	win     Window
}

type lbState struct {
	topo            Topology
	mu              sync.RWMutex
	backends        map[string]*backend
	remotes         []*remote
	rr              atomic.Uint64
	all             Window
	client          *http.Client
	servedForRemote Window
	failoverOut     Window
}

func runLB() {
	s := &lbState{topo: loadTopology(), backends: map[string]*backend{}}
	s.client = &http.Client{Timeout: 6 * time.Second, Transport: &http.Transport{
		MaxIdleConnsPerHost: 256, MaxIdleConns: 1024, IdleConnTimeout: 30 * time.Second,
		DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
	}}
	for _, p := range s.topo.Peers() {
		r := &remote{dc: p.ID, addr: fmt.Sprintf("wan:%d", 11700+p.Index)}
		s.remotes = append(s.remotes, r)
	}
	svc := env("BACKEND_DNS", "payments-api-all")
	go s.discover(svc)
	go s.healthLoop()
	mux := http.NewServeMux()
	mux.HandleFunc("/lbhealth", func(w http.ResponseWriter, r *http.Request) {
		if s.healthyLocal() > 0 {
			writeJSON(w, 200, map[string]any{"ok": true})
		} else {
			writeJSON(w, 503, map[string]any{"ok": false})
		}
	})
	mux.HandleFunc("/", s.handle)
	admin := http.NewServeMux()
	admin.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.metrics()) })
	go func() { log.Fatal(http.ListenAndServe(":9000", admin)) }()
	log.Printf("lb %s listening :8080 backends=%s", s.topo.Self, svc)
	log.Fatal(http.ListenAndServe(":8080", mux))
}

func (s *lbState) discover(svc string) {
	for {
		ips, err := net.LookupHost(svc)
		if err == nil {
			seen := map[string]bool{}
			s.mu.Lock()
			for _, ip := range ips {
				a := net.JoinHostPort(ip, "8080")
				seen[a] = true
				if _, ok := s.backends[a]; !ok {
					s.backends[a] = &backend{addr: a}
				}
			}
			for a := range s.backends {
				if !seen[a] {
					delete(s.backends, a)
				}
			}
			s.mu.Unlock()
		}
		time.Sleep(time.Second)
	}
}

func (s *lbState) healthLoop() {
	hc := &http.Client{Timeout: 1500 * time.Millisecond}
	for {
		var wg sync.WaitGroup
		s.mu.RLock()
		for _, b := range s.backends {
			wg.Add(1)
			go func(b *backend) {
				defer wg.Done()
				b.healthy.Store(probe(hc, "http://"+b.addr+"/healthz"))
			}(b)
		}
		for _, r := range s.remotes {
			wg.Add(1)
			go func(r *remote) {
				defer wg.Done()
				c := &http.Client{Timeout: 4 * time.Second}
				r.healthy.Store(probe(c, "http://"+r.addr+"/lbhealth"))
			}(r)
		}
		s.mu.RUnlock()
		wg.Wait()
		time.Sleep(700 * time.Millisecond)
	}
}

func probe(c *http.Client, url string) bool {
	resp, err := c.Get(url)
	if err != nil {
		return false
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode == 200
}

func (s *lbState) healthyLocal() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, b := range s.backends {
		if b.healthy.Load() {
			n++
		}
	}
	return n
}

// pick the healthy local backend with the fewest in-flight requests (round-robin tiebreak).
func (s *lbState) pickLocal(exclude map[string]bool) *backend {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var c []*backend
	for _, b := range s.backends {
		if b.healthy.Load() && !exclude[b.addr] {
			c = append(c, b)
		}
	}
	if len(c) == 0 {
		return nil
	}
	sort.Slice(c, func(i, j int) bool { return c[i].addr < c[j].addr })
	start := int(s.rr.Add(1))
	best := c[start%len(c)]
	for i := 0; i < len(c); i++ {
		b := c[(start+i)%len(c)]
		if b.inflight.Load() < best.inflight.Load() {
			best = b
		}
	}
	return best
}

func (s *lbState) pickRemote(exclude map[string]bool) *remote {
	var c []*remote
	for _, r := range s.remotes {
		if r.healthy.Load() && !exclude[r.addr] {
			c = append(c, r)
		}
	}
	if len(c) == 0 {
		return nil
	}
	return c[int(s.rr.Add(1))%len(c)]
}

func (s *lbState) handle(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()
	body, _ := io.ReadAll(r.Body)
	hop := r.Header.Get("X-DR-Hop") != ""
	origin := r.Header.Get("X-DR-Origin")
	if origin == "" {
		origin = s.topo.Self
	}
	tried := map[string]bool{}
	var lastErr string
	for attempt := 0; attempt < 3; attempt++ {
		var target, tag string
		var b *backend
		var rm *remote
		if b = s.pickLocal(tried); b != nil {
			target = b.addr
			tag = s.topo.Self
		} else if !hop {
			if rm = s.pickRemote(tried); rm != nil {
				target = rm.addr
				tag = rm.dc
			}
		}
		if target == "" {
			break
		}
		tried[target] = true
		req, _ := http.NewRequestWithContext(r.Context(), r.Method, "http://"+target+r.URL.RequestURI(), bytes.NewReader(body))
		req.Header = r.Header.Clone()
		req.Header.Set("X-DR-Origin", origin)
		if rm != nil {
			req.Header.Set("X-DR-Hop", "1")
		}
		if b != nil {
			b.inflight.Add(1)
		}
		resp, err := s.client.Do(req)
		if b != nil {
			b.inflight.Add(-1)
		}
		if err == nil && resp.StatusCode < 500 {
			dur := time.Since(t0)
			for k, v := range resp.Header {
				w.Header()[k] = v
			}
			w.Header().Set("X-Served-By-LB", s.topo.Self)
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			resp.Body.Close()
			s.all.Add(dur, true, tag)
			if b != nil {
				b.win.Add(dur, true, "")
				if hop || origin != s.topo.Self {
					s.servedForRemote.Add(dur, true, origin)
				}
			}
			if rm != nil {
				rm.win.Add(dur, true, "")
				s.failoverOut.Add(dur, true, rm.dc)
			}
			return
		}
		if err != nil {
			lastErr = err.Error()
		} else {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = resp.Status
		}
		if b != nil {
			b.healthy.Store(false) // eject until the next health probe passes
			b.win.Add(time.Since(t0), false, "")
		}
		if rm != nil {
			rm.healthy.Store(false)
		}
	}
	s.all.Add(time.Since(t0), false, "")
	writeJSON(w, 503, map[string]string{"error": "no healthy backend", "detail": lastErr, "lb": s.topo.Self})
	_ = context.Background
}

func (s *lbState) metrics() any {
	span := 5 * time.Second
	all := s.all.Snapshot(span)
	var bs []map[string]any
	s.mu.RLock()
	addrs := make([]string, 0, len(s.backends))
	for a := range s.backends {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs)
	healthy := 0
	for _, a := range addrs {
		b := s.backends[a]
		sn := b.win.Snapshot(span)
		if b.healthy.Load() {
			healthy++
		}
		bs = append(bs, map[string]any{"addr": a, "healthy": b.healthy.Load(), "inflight": b.inflight.Load(), "rps": sn.Rps, "p99ms": sn.P99})
	}
	s.mu.RUnlock()
	var rs []map[string]any
	for _, r := range s.remotes {
		rs = append(rs, map[string]any{"dc": r.dc, "healthy": r.healthy.Load(), "rps": r.win.Snapshot(span).Rps})
	}
	out := s.failoverOut.Snapshot(span)
	in := s.servedForRemote.Snapshot(span)
	mode := "normal"
	if healthy == 0 {
		mode = "failover"
	}
	return map[string]any{
		"dc": s.topo.Self, "mode": mode, "rps": all.Rps, "errRps": all.ErrRps,
		"p50ms": all.P50, "p95ms": all.P95, "p99ms": all.P99, "byServedDC": all.ByTag,
		"failoverOutRps": out.Rps, "failoverOutTo": out.ByTag,
		"servedForRemoteRps": in.Rps, "servedForRemoteFrom": in.ByTag,
		"healthyBackends": healthy, "backends": bs, "remotes": rs,
	}
}
