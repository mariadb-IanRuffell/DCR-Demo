package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// WAN emulator. For every peer DC it listens on two ports and forwards to the peer's published ports,
// delaying every chunk by rtt/2 (+jitter) in each direction so a request/response sees ~rtt:
//
//	11800+index  -> peer GridGain client port   (GridGain DCR replication traffic)
//	11700+index  -> peer load balancer          (cross-DC failover traffic)
type link struct {
	peer     DC
	mu       sync.Mutex
	rttMs    int
	jitterMs int
	down     bool
	conns    map[net.Conn]struct{}
	bytes    atomic.Int64
	active   atomic.Int64
}

func (l *link) cfg() (int, int, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rttMs, l.jitterMs, l.down
}

func (l *link) delay() time.Duration {
	rtt, j, _ := l.cfg()
	d := float64(rtt) / 2
	if j > 0 {
		d += (rand.Float64()*2 - 1) * float64(j) / 2
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d * float64(time.Millisecond))
}

func (l *link) track(c net.Conn, add bool) {
	l.mu.Lock()
	if add {
		l.conns[c] = struct{}{}
	} else {
		delete(l.conns, c)
	}
	l.mu.Unlock()
}

func (l *link) killAll() {
	l.mu.Lock()
	for c := range l.conns {
		c.Close()
	}
	l.mu.Unlock()
}

type chunk struct {
	at   time.Time
	data []byte
}

func (l *link) pipe(dst, src net.Conn) {
	ch := make(chan chunk, 8192)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for c := range ch {
			if d := time.Until(c.at); d > 0 {
				time.Sleep(d)
			}
			if _, err := dst.Write(c.data); err != nil {
				for range ch {
				}
				return
			}
		}
	}()
	buf := make([]byte, 32*1024)
	var last time.Time
	for {
		n, err := src.Read(buf)
		if n > 0 {
			l.bytes.Add(int64(n))
			at := time.Now().Add(l.delay())
			if at.Before(last) {
				at = last
			}
			last = at
			ch <- chunk{at, append([]byte(nil), buf[:n]...)}
		}
		if err != nil {
			break
		}
	}
	close(ch)
	<-done
	dst.Close()
	src.Close()
}

func (l *link) serve(listen string, target string) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatalf("listen %s: %v", listen, err)
	}
	log.Printf("wan %s -> %s (%s)", listen, target, l.peer.ID)
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		go func() {
			if _, _, down := l.cfg(); down {
				c.Close()
				return
			}
			// connection establishment costs one RTT
			rtt, _, _ := l.cfg()
			time.Sleep(time.Duration(rtt) * time.Millisecond)
			up, err := net.DialTimeout("tcp", target, 3*time.Second)
			if err != nil {
				c.Close()
				return
			}
			if _, _, down := l.cfg(); down {
				c.Close()
				up.Close()
				return
			}
			l.track(c, true)
			l.track(up, true)
			l.active.Add(1)
			go l.pipe(up, c)
			l.pipe(c, up)
			l.active.Add(-1)
			l.track(c, false)
			l.track(up, false)
		}()
	}
}

func runWAN() {
	topo := loadTopology()
	links := map[string]*link{}
	for _, p := range topo.Peers() {
		l := &link{peer: p, rttMs: atoiDefault(env("DEFAULT_RTT_MS", "20")), conns: map[net.Conn]struct{}{}}
		links[p.ID] = l
		go l.serve(fmt.Sprintf(":%d", 11800+p.Index), fmt.Sprintf("%s:%d", p.HostIP, p.XDCPort))
		go l.serve(fmt.Sprintf(":%d", 11700+p.Index), fmt.Sprintf("%s:%d", p.HostIP, p.LBPort))
	}
	state := func() any {
		out := map[string]any{}
		for id, l := range links {
			rtt, j, down := l.cfg()
			out[id] = map[string]any{"rttMs": rtt, "jitterMs": j, "down": down, "activeConns": l.active.Load(), "bytes": l.bytes.Load()}
		}
		return out
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/state", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, state()) })
	mux.HandleFunc("/link", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Peer     string `json:"peer"`
			RttMs    *int   `json:"rttMs"`
			JitterMs *int   `json:"jitterMs"`
			Down     *bool  `json:"down"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		l := links[req.Peer]
		if l == nil {
			writeJSON(w, 404, map[string]string{"error": "unknown peer"})
			return
		}
		l.mu.Lock()
		if req.RttMs != nil {
			l.rttMs = *req.RttMs
		}
		if req.JitterMs != nil {
			l.jitterMs = *req.JitterMs
		}
		kill := false
		if req.Down != nil {
			kill = *req.Down && !l.down
			l.down = *req.Down
		}
		l.mu.Unlock()
		if kill {
			l.killAll()
		}
		writeJSON(w, 200, state())
	})
	log.Fatal(http.ListenAndServe(":9000", mux))
}

func atoiDefault(s string) int {
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}

var _ = io.EOF
