// dashboard: topology / health / control console for the multi-DC GridGain demo.
// It talks only to the per-DC agents (HTTP), so it can run anywhere that can reach the three hosts.
package main

import (
	"bytes"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

//go:embed static
var static embed.FS

type DC struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Index     int    `json:"index"`
	HostIP    string `json:"hostIp"`
	LBPort    int    `json:"lbPort"`
	AgentPort int    `json:"agentPort"`
}

type topology struct {
	DCs []DC `json:"dcs"`
}

type dcState struct {
	DC        DC             `json:"dc"`
	Reachable bool           `json:"reachable"`
	PollMs    int64          `json:"pollMs"`
	Error     string         `json:"error,omitempty"`
	Status    map[string]any `json:"status"`
}

type server struct {
	topo  topology
	mu    sync.RWMutex
	state map[string]*dcState
	hc    *http.Client
}

func (s *server) agent(id string) string {
	for _, d := range s.topo.DCs {
		if d.ID == id {
			return fmt.Sprintf("http://%s:%d", d.HostIP, d.AgentPort)
		}
	}
	return ""
}

func (s *server) send(dc string, body map[string]any) (string, error) {
	b, _ := json.Marshal(body)
	resp, err := s.hc.Post(s.agent(dc)+"/action", "application/json", bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return string(out), fmt.Errorf("%s: %s", dc, out)
	}
	return string(out), nil
}

func (s *server) peers(dc string) []string {
	var p []string
	for _, d := range s.topo.DCs {
		if d.ID != dc {
			p = append(p, d.ID)
		}
	}
	return p
}

func (s *server) link(a, b string, extra map[string]any) error {
	var firstErr error
	for _, pair := range [][2]string{{a, b}, {b, a}} {
		m := map[string]any{"op": "setLink", "peer": pair[1]}
		for k, v := range extra {
			m[k] = v
		}
		if _, err := s.send(pair[0], m); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *server) action(req map[string]any) (any, error) {
	op, _ := req["op"].(string)
	dc, _ := req["dc"].(string)
	switch op {
	case "setLatency": // {a,b,rttMs,jitterMs}
		a, b := req["a"].(string), req["b"].(string)
		extra := map[string]any{"rttMs": req["rttMs"]}
		if j, ok := req["jitterMs"]; ok {
			extra["jitterMs"] = j
		}
		return nil, s.link(a, b, extra)
	case "setLinkDown": // {a,b,down}
		return nil, s.link(req["a"].(string), req["b"].(string), map[string]any{"down": req["down"]})
	case "isolateDC", "healDC": // network partition of one DC from all the others
		down := op == "isolateDC"
		var err error
		for _, p := range s.peers(dc) {
			if e := s.link(dc, p, map[string]any{"down": down}); e != nil {
				err = e
			}
		}
		return nil, err
	case "resetAll":
		var err error
		for _, d := range s.topo.DCs {
			for _, p := range s.peers(d.ID) {
				s.link(d.ID, p, map[string]any{"down": false})
			}
			if _, e := s.send(d.ID, map[string]any{"op": "recoverDC"}); e != nil {
				err = e
			}
		}
		return nil, err
	default:
		if dc == "" {
			return nil, fmt.Errorf("dc required")
		}
		out, err := s.send(dc, req)
		return out, err
	}
}

func main() {
	topo := flag.String("topology", "topology.json", "topology json")
	listen := flag.String("listen", ":8088", "listen address")
	flag.Parse()
	b, err := os.ReadFile(*topo)
	if err != nil {
		log.Fatal(err)
	}
	s := &server{state: map[string]*dcState{}, hc: &http.Client{Timeout: 25 * time.Second}}
	if err := json.Unmarshal(b, &s.topo); err != nil {
		log.Fatal(err)
	}
	poller := &http.Client{Timeout: 3 * time.Second}
	go func() {
		for {
			var wg sync.WaitGroup
			for _, d := range s.topo.DCs {
				wg.Add(1)
				go func(d DC) {
					defer wg.Done()
					t0 := time.Now()
					st := &dcState{DC: d}
					resp, err := poller.Get(s.agent(d.ID) + "/status")
					if err == nil {
						err = json.NewDecoder(resp.Body).Decode(&st.Status)
						resp.Body.Close()
					}
					st.PollMs = time.Since(t0).Milliseconds()
					if err != nil {
						st.Error = err.Error()
					} else {
						st.Reachable = true
					}
					s.mu.Lock()
					s.state[d.ID] = st
					s.mu.Unlock()
				}(d)
			}
			wg.Wait()
			time.Sleep(time.Second)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		var out []*dcState
		for _, d := range s.topo.DCs {
			if st := s.state[d.ID]; st != nil {
				out = append(out, st)
			} else {
				out = append(out, &dcState{DC: d, Error: "not polled yet"})
			}
		}
		s.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		json.NewEncoder(w).Encode(map[string]any{"ts": time.Now().UnixMilli(), "dcs": out})
	})
	mux.HandleFunc("/api/action", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		log.Printf("action %v", req)
		out, err := s.action(req)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": out})
	})
	sub, _ := fs.Sub(static, "static")
	mux.Handle("/", http.FileServer(http.FS(sub)))
	log.Printf("dashboard listening on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, mux))
}
