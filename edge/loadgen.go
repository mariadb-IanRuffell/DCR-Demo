package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	mrand "math/rand"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

func uuid() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func runLoadgen() {
	topo := loadTopology()
	me := topo.Me()
	target := env("TARGET", "http://lb:8080")
	accounts := atoiDefault(env("ACCOUNTS_PER_DC", "5000"))
	var tps atomic.Int64
	tps.Store(int64(atoiDefault(env("TPS", "25"))))
	var win Window
	var dropped, retried atomic.Int64
	sem := make(chan struct{}, 400)
	client := &http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 400, DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext}}
	base := me.Index * 100000

	send := func() {
		defer func() { <-sem }()
		from := base + 1 + mrand.Intn(accounts)
		to := base + 1 + mrand.Intn(accounts)
		for to == from {
			to = base + 1 + mrand.Intn(accounts)
		}
		body, _ := json.Marshal(map[string]any{"id": uuid(), "from": from, "to": to,
			"amount": fmt.Sprintf("%d.%02d", 1+mrand.Intn(40), mrand.Intn(100)), "origin": me.ID})
		t0 := time.Now()
		for attempt := 0; attempt < 2; attempt++ {
			resp, err := client.Post(target+"/payments", "application/json", bytes.NewReader(body))
			if err == nil && resp.StatusCode == 200 {
				var r struct {
					ServedBy string `json:"servedBy"`
				}
				json.NewDecoder(resp.Body).Decode(&r)
				resp.Body.Close()
				win.Add(time.Since(t0), true, r.ServedBy)
				return
			}
			if resp != nil {
				resp.Body.Close()
			}
			retried.Add(1)
			time.Sleep(150 * time.Millisecond)
		}
		win.Add(time.Since(t0), false, "")
	}

	go func() { // paced at the configured rate, re-evaluated every tick
		next := time.Now()
		for {
			rate := tps.Load()
			if rate <= 0 {
				time.Sleep(200 * time.Millisecond)
				next = time.Now()
				continue
			}
			next = next.Add(time.Second / time.Duration(rate))
			if d := time.Until(next); d > 0 {
				time.Sleep(d)
			} else if d < -time.Second {
				next = time.Now()
			}
			select {
			case sem <- struct{}{}:
				go send()
			default:
				dropped.Add(1)
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s := win.Snapshot(5 * time.Second)
		writeJSON(w, 200, map[string]any{"dc": me.ID, "targetTps": tps.Load(), "okRps": s.Rps, "errRps": s.ErrRps,
			"p50ms": s.P50, "p95ms": s.P95, "p99ms": s.P99, "servedByDC": s.ByTag, "dropped": dropped.Load(), "retries": retried.Load()})
	})
	mux.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Tps int `json:"tps"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Tps < 0 || req.Tps > 5000 {
			writeJSON(w, 400, map[string]string{"error": "tps must be 0..5000"})
			return
		}
		tps.Store(int64(req.Tps))
		writeJSON(w, 200, map[string]any{"targetTps": req.Tps})
	})
	log.Printf("loadgen %s -> %s at %d tps", me.ID, target, tps.Load())
	log.Fatal(http.ListenAndServe(":9000", mux))
}
