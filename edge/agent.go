package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"
)

// The agent runs inside each DC. It aggregates status from every component of the DC (Kubernetes,
// GridGain REST, the LB, load generator, WAN emulator, payments pods, DCR) and carries out control actions
// (scale, fail/recover nodes, fail/recover the DC, latency changes) using kubectl and the component admin APIs.

type agent struct {
	topo          Topology
	mu            sync.RWMutex
	st            map[string]any
	dcr           map[string]any
	stat          map[string]any
	maxNodes      int
	lastRepair    map[string]time.Time
	lastScaleDown time.Time
	hc            *http.Client
}

type k8sList struct {
	Items []struct {
		Kind     string `json:"kind"`
		Metadata struct {
			Name              string            `json:"name"`
			Labels            map[string]string `json:"labels"`
			Annotations       map[string]string `json:"annotations"`
			DeletionTimestamp *string           `json:"deletionTimestamp"`
		} `json:"metadata"`
		Spec struct {
			Replicas *int `json:"replicas"`
		} `json:"spec"`
		Status struct {
			Phase             string                          `json:"phase"`
			PodIP             string                          `json:"podIP"`
			Conditions        []struct{ Type, Status string } `json:"conditions"`
			ContainerStatuses []struct {
				RestartCount int `json:"restartCount"`
			} `json:"containerStatuses"`
			ReadyReplicas int `json:"readyReplicas"`
		} `json:"status"`
	} `json:"items"`
}

func kc(timeout time.Duration, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", append([]string{"-n", "dr"}, args...)...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	if err != nil {
		return out.String() + errb.String(), fmt.Errorf("%v: %s", err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

func (a *agent) getJSON(url string, v any) error {
	resp, err := a.hc.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(v)
}

func (a *agent) postJSON(url string, body any) (string, error) {
	b, _ := json.Marshal(body)
	resp, err := a.hc.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return string(out), fmt.Errorf("%s", resp.Status)
	}
	return string(out), nil
}

func runAgent() {
	a := &agent{topo: loadTopology(), maxNodes: atoiDefault(env("MAX_NODES", "6")), hc: &http.Client{Timeout: 2 * time.Second},
		st: map[string]any{}, dcr: map[string]any{}, stat: map[string]any{}, lastRepair: map[string]time.Time{}}
	go func() {
		for {
			a.refresh()
			time.Sleep(1500 * time.Millisecond)
		}
	}()
	go func() {
		for {
			a.refreshDCR()
			time.Sleep(5 * time.Second)
		}
	}()
	go func() { // hot-store retention, executed through one ready payments pod
		for {
			time.Sleep(10 * time.Second)
			if ip := a.readyAppIP(); ip != "" {
				c := &http.Client{Timeout: 60 * time.Second}
				if resp, err := c.Get("http://" + ip + ":8080/admin/purge"); err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		a.mu.RLock()
		defer a.mu.RUnlock()
		out := map[string]any{}
		for k, v := range a.st {
			out[k] = v
		}
		out["dcr"] = a.dcr
		out["stats"] = a.stat
		writeJSON(w, 200, out)
	})
	mux.HandleFunc("/action", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		msg, err := a.action(req)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error(), "output": msg})
			return
		}
		writeJSON(w, 200, map[string]string{"ok": "true", "message": msg})
	})
	log.Printf("agent %s listening :9090", a.topo.Self)
	log.Fatal(http.ListenAndServe(":9090", mux))
}

type podInfo struct {
	Name     string `json:"name"`
	IP       string `json:"ip"`
	Status   string `json:"status"` // UP STARTING FAILED STOPPING PENDING
	Ready    bool   `json:"ready"`
	Restarts int    `json:"restarts"`
	Metrics  any    `json:"metrics,omitempty"`
}

func (a *agent) refresh() {
	st := map[string]any{"dc": a.topo.Self, "ts": time.Now().UnixMilli(), "maxNodes": a.maxNodes}
	out, err := kc(10*time.Second, "get", "pods,sts,deploy", "-o", "json")
	if err != nil {
		st["error"] = "kubernetes: " + err.Error()
		a.set(st)
		return
	}
	var l k8sList
	json.Unmarshal([]byte(out), &l)
	var gg, apps []podInfo
	replicas := map[string]int{}
	for _, it := range l.Items {
		name := it.Metadata.Name
		switch it.Kind {
		case "StatefulSet", "Deployment":
			if it.Spec.Replicas != nil {
				replicas[name] = *it.Spec.Replicas
			}
		case "Pod":
			p := podInfo{Name: name, IP: it.Status.PodIP}
			for _, c := range it.Status.Conditions {
				if c.Type == "Ready" && c.Status == "True" {
					p.Ready = true
				}
			}
			for _, c := range it.Status.ContainerStatuses {
				p.Restarts += c.RestartCount
			}
			switch {
			case it.Metadata.Annotations["dr/failed"] == "true":
				p.Status, p.Ready = "FAILED", false
			case it.Metadata.DeletionTimestamp != nil:
				p.Status, p.Ready = "STOPPING", false
			case p.Ready:
				p.Status = "UP"
			case it.Status.Phase == "Running":
				p.Status = "STARTING"
			default:
				p.Status = "PENDING"
			}
			switch it.Metadata.Labels["app"] {
			case "gg":
				gg = append(gg, p)
			case "payments-api":
				apps = append(apps, p)
			}
		}
	}
	sort.Slice(gg, func(i, j int) bool { return podOrd(gg[i].Name) < podOrd(gg[j].Name) })
	sort.Slice(apps, func(i, j int) bool { return apps[i].Name < apps[j].Name })

	// payments pods
	lag := map[string]float64{}
	var statsPod string
	for i := range apps {
		if apps[i].Ready {
			var m map[string]any
			if a.getJSON("http://"+apps[i].IP+":8080/metrics", &m) == nil {
				apps[i].Metrics = m
				if stl, ok := m["replicationStalenessMs"].(map[string]any); ok {
					for peer, v := range stl {
						if f, ok := v.(float64); ok {
							if cur, ok := lag[peer]; !ok || f < cur {
								lag[peer] = f
							}
						}
					}
				}
			}
			if statsPod == "" {
				statsPod = apps[i].IP
			}
		}
	}
	if statsPod != "" {
		var pl map[string]float64
		if a.getJSON("http://"+statsPod+":8080/lag", &pl) == nil {
			for peer, v := range pl { // data-based lag; the heartbeat can only over-estimate, so take the smaller
				if cur, ok := lag[peer]; !ok || v < cur {
					lag[peer] = v
				}
			}
		}
	}
	if statsPod != "" && time.Now().Unix()%5 < 2 {
		var s map[string]any
		if a.getJSON("http://"+statsPod+":8080/stats", &s) == nil {
			a.mu.Lock()
			a.stat = s
			a.mu.Unlock()
		}
	}

	// GridGain cluster state (via REST on any ready node)
	ready, cmgReady := 0, 0
	var restIP string
	for _, p := range gg {
		if p.Ready {
			ready++
			if podOrd(p.Name) < 3 {
				cmgReady++
			}
			if restIP == "" {
				restIP = p.IP
			}
		}
	}
	cluster := map[string]any{"desiredNodes": replicas["gg"], "readyNodes": ready, "cmgReady": cmgReady}
	if restIP != "" {
		var cs map[string]any
		if a.getJSON("http://"+restIP+":10300/management/v1/cluster/state", &cs) == nil {
			cluster["initialised"] = true
			if ct, ok := cs["clusterTag"].(map[string]any); ok {
				cluster["name"] = ct["clusterName"]
			}
		}
		var topo []any
		if a.getJSON("http://"+restIP+":10300/management/v1/cluster/topology/logical", &topo) == nil {
			cluster["logicalNodes"] = len(topo)
		}
	}
	switch {
	case cmgReady < 2:
		cluster["status"] = "UNAVAILABLE"
	case ready < replicas["gg"]:
		cluster["status"] = "DEGRADED"
	default:
		cluster["status"] = "HEALTHY"
	}

	var lbm, lgm, wan map[string]any
	a.getJSON("http://lb:9000/metrics", &lbm)
	a.getJSON("http://loadgen:9000/metrics", &lgm)
	a.getJSON("http://wan:9000/state", &wan)

	failedAll := len(gg) > 0
	for _, p := range gg {
		if p.Status != "FAILED" {
			failedAll = false
		}
	}
	st["gg"], st["apps"], st["cluster"] = gg, apps, cluster
	st["lb"], st["loadgen"], st["wan"], st["repLagMs"] = lbm, lgm, wan, lag
	st["replicas"] = replicas
	st["dcFailed"] = failedAll
	a.set(st)
}

func (a *agent) readyAppIP() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if apps, ok := a.st["apps"].([]podInfo); ok {
		for _, p := range apps {
			if p.Ready {
				return p.IP
			}
		}
	}
	return ""
}

func (a *agent) set(st map[string]any) {
	a.mu.Lock()
	a.st = st
	a.mu.Unlock()
}

func podOrd(n string) int {
	i := strings.LastIndex(n, "-")
	var v int
	fmt.Sscanf(n[i+1:], "%d", &v)
	return v
}

// ---------------------------------------------------------------- DCR
// Replication is administered through the GridGain REST API (the CLI is a JVM and must not run inside node pods).

func (a *agent) readyGGIP() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if gg, ok := a.st["gg"].([]podInfo); ok {
		for _, p := range gg {
			if p.Ready {
				return p.IP
			}
		}
	}
	return ""
}

const dcrPath = ":10300/management/v1/dcr/replications"

func (a *agent) dcrCall(method, ip, suffix string, body any) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, "http://"+ip+dcrPath+suffix, rd)
	req.Header.Set("Content-Type", "application/json")
	c := &http.Client{Timeout: 40 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return out, fmt.Errorf("%s %s: %s %s", method, suffix, resp.Status, strings.TrimSpace(string(out)))
	}
	return out, nil
}

func (a *agent) listDCR(ip string) ([]map[string]any, error) {
	out, err := a.dcrCall("GET", ip, "", nil)
	if err != nil {
		return nil, err
	}
	var raw []map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func (a *agent) refreshDCR() {
	ip := a.readyGGIP()
	res := map[string]any{}
	if ip == "" {
		res["error"] = "no ready GridGain node"
		res["replications"] = []map[string]any{}
	} else if raw, err := a.listDCR(ip); err != nil {
		res["error"] = err.Error()
		res["replications"] = []map[string]any{}
	} else {
		rows := []map[string]any{}
		for _, r := range raw {
			row := map[string]any{"name": r["name"], "status": r["status"], "from": r["sourceClusterName"], "to": r["targetClusterName"],
				"fst": r["fstProgress"], "worker": r["workerNodeName"], "tables": r["tableStatuses"]}
			rows = append(rows, row)
		}
		sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i]["name"]) < fmt.Sprint(rows[j]["name"]) })
		res["replications"] = rows
	}
	a.mu.Lock()
	a.dcr = res
	a.mu.Unlock()
	if env("AUTO_REPAIR", "true") == "true" && ip != "" && res["error"] == nil {
		a.repairDCR(ip, res["replications"].([]map[string]any))
	}
}

// repairDCR keeps the full mesh alive: a replication that is missing, stopped or FAILED (e.g. after the source DC was
// down for longer than the retention window) is recreated. Links that are deliberately partitioned are left alone.
func (a *agent) repairDCR(ip string, rows []map[string]any) {
	a.mu.RLock()
	wan, _ := a.st["wan"].(map[string]any)
	a.mu.RUnlock()
	for _, p := range a.topo.Peers() {
		if l, ok := wan[p.ID].(map[string]any); ok {
			if down, _ := l["down"].(bool); down {
				continue
			}
		}
		name := "from_" + p.ID
		status := ""
		for _, r := range rows {
			if r["name"] == name {
				status, _ = r["status"].(string)
			}
		}
		if status == "" || status == "FAILED" || status == "STOPPED" {
			a.mu.Lock()
			last := a.lastRepair[name]
			if time.Since(last) < 20*time.Second {
				a.mu.Unlock()
				continue
			}
			a.lastRepair[name] = time.Now()
			a.mu.Unlock()
			log.Printf("repairing replication %s (status=%q)", name, status)
			if msg, err := a.ensureReplication(ip, p, status == "FAILED"); err != nil {
				log.Printf("repair %s: %v %s", name, err, msg)
			}
		}
	}
}

func (a *agent) ensureReplication(ip string, p DC, rebuild bool) (string, error) {
	name := "from_" + p.ID
	var log strings.Builder
	raw, err := a.listDCR(ip)
	if err != nil {
		return "", err
	}
	exists := false
	for _, r := range raw {
		if r["name"] == name {
			exists = true
		}
	}
	if exists && rebuild {
		a.dcrCall("POST", ip, "/"+name+"/stop", map[string]any{"allTables": true})
		if _, err := a.dcrCall("POST", ip, "/"+name+"/delete", nil); err != nil {
			return log.String(), err
		}
		exists = false
		log.WriteString("deleted " + name + "; ")
	}
	if !exists {
		// spread the replication workers over different nodes (rotate the candidate list per source DC)
		nodes := []string{"gg-0", "gg-1", "gg-2"}
		r := p.Index % 3
		nodes = append(nodes[r:], nodes[:r]...)
		if _, err := a.dcrCall("POST", ip, "", map[string]any{
			"name": name, "sourceClusterAddresses": []string{fmt.Sprintf("wan:%d", 11800+p.Index)},
			"authConfig":       map[string]string{"username": "ignite", "password": "ignite"},
			"replicationNodes": nodes}); err != nil {
			return log.String(), err
		}
		log.WriteString("created " + name + "; ")
	}
	if _, err := a.dcrCall("POST", ip, "/"+name+"/start", map[string]any{"schemaName": "PUBLIC", "allTables": false, "tables": []string{"PAYMENTS", "HEARTBEAT"}}); err != nil &&
		!strings.Contains(err.Error(), "already") {
		return log.String(), err
	}
	log.WriteString("started " + name)
	return log.String(), nil
}

func (a *agent) setupDCR(rebuild bool) (string, error) {
	ip := a.readyGGIP()
	if ip == "" {
		return "", fmt.Errorf("no ready GridGain node")
	}
	var out []string
	for _, p := range a.topo.Peers() {
		msg, err := a.ensureReplication(ip, p, rebuild)
		out = append(out, msg)
		if err != nil {
			return strings.Join(out, " | "), err
		}
	}
	return strings.Join(out, " | "), nil
}

// ---------------------------------------------------------------- actions

func str(m map[string]any, k string) string { s, _ := m[k].(string); return s }
func num(m map[string]any, k string) int    { f, _ := m[k].(float64); return int(f) }

func (a *agent) ggPods() []podInfo {
	a.mu.RLock()
	defer a.mu.RUnlock()
	gg, _ := a.st["gg"].([]podInfo)
	return gg
}

func (a *agent) failNode(name string) error {
	if _, err := kc(15*time.Second, "annotate", "pod", name, "dr/failed=true", "--overwrite"); err != nil {
		return err
	}
	_, err := kc(15*time.Second, "exec", name, "--", "touch", "/fail/down")
	return err
}

func (a *agent) recoverNode(name string) error {
	kc(15*time.Second, "exec", name, "--", "rm", "-f", "/fail/down")
	_, err := kc(15*time.Second, "annotate", "pod", name, "dr/failed-", "--overwrite")
	return err
}

func (a *agent) action(req map[string]any) (string, error) {
	switch op := str(req, "op"); op {
	case "scaleNodes":
		n := num(req, "n")
		if n < 3 || n > a.maxNodes {
			return "", fmt.Errorf("nodes must be between 3 and %d", a.maxNodes)
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		cur := 0
		if r, ok := a.st["replicas"].(map[string]int); ok {
			cur = r["gg"]
		}
		if n < cur {
			// Scaling down removes replica holders. Removing several at once (or before the previous removal has been
			// rebalanced) can take a 3-replica partition below quorum, so: one node at a time, cluster healthy, cooldown.
			if n != cur-1 {
				return "", fmt.Errorf("scale down one node at a time (currently %d)", cur)
			}
			if c, ok := a.st["cluster"].(map[string]any); !ok || c["status"] != "HEALTHY" {
				return "", fmt.Errorf("cluster must be HEALTHY before scaling down")
			}
			if wait := time.Until(a.lastScaleDown.Add(75 * time.Second)); wait > 0 {
				return "", fmt.Errorf("previous node removal is still rebalancing; retry in %ds", int(wait.Seconds())+1)
			}
			a.lastScaleDown = time.Now()
		}
		return kc(20*time.Second, "scale", "sts/gg", fmt.Sprintf("--replicas=%d", n))
	case "scaleApps":
		n := num(req, "n")
		if n < 0 || n > 10 {
			return "", fmt.Errorf("apps must be between 0 and 10")
		}
		return kc(20*time.Second, "scale", "deploy/payments-api", fmt.Sprintf("--replicas=%d", n))
	case "failNode":
		return "", a.failNode(str(req, "name"))
	case "recoverNode":
		return "", a.recoverNode(str(req, "name"))
	case "failDC":
		// whole-DC outage: every GridGain node dies and the payments tier goes with it; the edge (LB) survives
		// and fails traffic over to the other DCs.
		var wg sync.WaitGroup
		for _, p := range a.ggPods() {
			wg.Add(1)
			go func(n string) { defer wg.Done(); a.failNode(n) }(p.Name)
		}
		wg.Wait()
		cur := 3
		a.mu.RLock()
		if r, ok := a.st["replicas"].(map[string]int); ok && r["payments-api"] > 0 {
			cur = r["payments-api"]
		}
		a.mu.RUnlock()
		kc(10*time.Second, "annotate", "deploy/payments-api", fmt.Sprintf("dr/prev-replicas=%d", cur), "--overwrite")
		return kc(20*time.Second, "scale", "deploy/payments-api", "--replicas=0")
	case "recoverDC":
		var wg sync.WaitGroup
		for _, p := range a.ggPods() {
			wg.Add(1)
			go func(n string) { defer wg.Done(); a.recoverNode(n) }(p.Name)
		}
		wg.Wait()
		prev, _ := kc(10*time.Second, "get", "deploy/payments-api", "-o", "jsonpath={.metadata.annotations.dr/prev-replicas}")
		if strings.TrimSpace(prev) == "" {
			prev = "3"
		}
		return kc(20*time.Second, "scale", "deploy/payments-api", "--replicas="+strings.TrimSpace(prev))
	case "setLoad":
		return a.postJSON2("http://loadgen:9000/config", map[string]any{"tps": num(req, "tps")})
	case "setLink":
		return a.postJSON2("http://wan:9000/link", req)
	case "setupDCR":
		return a.setupDCR(false)
	case "rebuildDCR":
		return a.setupDCR(true)
	default:
		return "", fmt.Errorf("unknown op %q", op)
	}
}

func (a *agent) postJSON2(url string, body any) (string, error) { return a.postJSON(url, body) }
