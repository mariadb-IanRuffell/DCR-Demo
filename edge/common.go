package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type DC struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Index     int    `json:"index"`
	HostIP    string `json:"hostIp"`
	LBPort    int    `json:"lbPort"`
	XDCPort   int    `json:"xdcPort"`
	AgentPort int    `json:"agentPort"`
}

type Topology struct {
	Self string `json:"self"`
	DCs  []DC   `json:"dcs"`
}

func (t Topology) Me() DC {
	for _, d := range t.DCs {
		if d.ID == t.Self {
			return d
		}
	}
	return DC{}
}

func (t Topology) Peers() []DC {
	var p []DC
	for _, d := range t.DCs {
		if d.ID != t.Self {
			p = append(p, d)
		}
	}
	return p
}

func loadTopology() Topology {
	f := env("TOPOLOGY_FILE", "/etc/dr/topology.json")
	b, err := os.ReadFile(f)
	if err != nil {
		log.Fatalf("topology: %v", err)
	}
	var t Topology
	if err := json.Unmarshal(b, &t); err != nil {
		log.Fatalf("topology: %v", err)
	}
	return t
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// Window is a rolling window of request outcomes used for rps / latency percentiles.
type ev struct {
	t   time.Time
	lat time.Duration
	ok  bool
	tag string
}

type Window struct {
	mu sync.Mutex
	ev []ev
}

func (w *Window) Add(lat time.Duration, ok bool, tag string) {
	now := time.Now()
	w.mu.Lock()
	w.ev = append(w.ev, ev{now, lat, ok, tag})
	if len(w.ev) > 2000 && now.Sub(w.ev[0].t) > 12*time.Second {
		i := sort.Search(len(w.ev), func(i int) bool { return now.Sub(w.ev[i].t) <= 12*time.Second })
		w.ev = append([]ev(nil), w.ev[i:]...)
	}
	w.mu.Unlock()
}

type Snap struct {
	Rps    float64            `json:"rps"`
	ErrRps float64            `json:"errRps"`
	P50    float64            `json:"p50ms"`
	P95    float64            `json:"p95ms"`
	P99    float64            `json:"p99ms"`
	ByTag  map[string]float64 `json:"byTag"`
}

func (w *Window) Snapshot(span time.Duration) Snap {
	cut := time.Now().Add(-span)
	w.mu.Lock()
	var lats []float64
	s := Snap{ByTag: map[string]float64{}}
	var okN, errN float64
	for i := len(w.ev) - 1; i >= 0; i-- {
		e := w.ev[i]
		if e.t.Before(cut) {
			break
		}
		if e.ok {
			okN++
			lats = append(lats, float64(e.lat.Microseconds())/1000)
			if e.tag != "" {
				s.ByTag[e.tag]++
			}
		} else {
			errN++
		}
	}
	w.mu.Unlock()
	sec := span.Seconds()
	s.Rps, s.ErrRps = okN/sec, errN/sec
	for k := range s.ByTag {
		s.ByTag[k] /= sec
	}
	sort.Float64s(lats)
	pc := func(p float64) float64 {
		if len(lats) == 0 {
			return 0
		}
		i := int(float64(len(lats)) * p)
		if i >= len(lats) {
			i = len(lats) - 1
		}
		return round1(lats[i])
	}
	s.P50, s.P95, s.P99 = pc(.5), pc(.95), pc(.99)
	s.Rps, s.ErrRps = round1(s.Rps), round1(s.ErrRps)
	return s
}

func round1(f float64) float64 { return float64(int(f*10+0.5)) / 10 }
