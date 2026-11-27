# GridGain 9.2 — multi-data-centre HA & DR demo

Three simulated data centres, each a **separate Kubernetes cluster** running its own **3-node GridGain 9.2 cluster**,
all **active/active**, kept in sync by GridGain **Data Center Replication (DCR)** in a full mesh. A payments
application and a steady synthetic load run in every DC; a separate **dashboard** shows the whole topology and lets you
break things.

```
        users                    users                    users
          │                        │                        │
   ┌──────▼──────┐          ┌──────▼──────┐          ┌──────▼──────┐
   │ load gen    │          │ load gen    │          │ load gen    │      one per DC, steady rate
   │ load balancer│ ◄─ failover ─► load balancer ◄─ failover ─► load balancer│  cross-DC failover via WAN emulator
   │ payments-api│          │ payments-api│          │ payments-api│      3 stateless pods, local cluster only
   │ GridGain ×3 │◄══ DCR ══►│ GridGain ×3 │◄══ DCR ══►│ GridGain ×3 │     6 replications (every DC pulls from the other two)
   └──────┬──────┘          └──────┬──────┘          └──────┬──────┘
     wan emulator ───── latency / jitter / partition ───── wan emulator
     dc-agent (status + control API)                      dc-agent
          ▲                        ▲                        ▲
          └──────────── dashboard (runs on your laptop) ────┘
   DC1 London  (kind on this Mac)   DC2 Frankfurt + DC3 New York (kind on llama.local)
```

## Quick start

Prerequisites: Docker, `kind`, `kubectl`, Go (for the dashboard), SSH access to `llama.local` with Docker + `kind` there,
a GridGain 9 **Enterprise/Ultimate** licence (DCR is a licensed feature) at `secrets/gridgain-license.json`, and the
`gridgain/gridgain9:9.2.0-b1` image on both machines.

```bash
bin/dr up          # ~8 min: creates the 3 clusters, builds images on both hosts, deploys, initialises, links with DCR
bin/dr dashboard   # http://localhost:8088
bin/dr status      # pods + replication state in every DC
bin/dr down        # delete everything
```

Everything configurable lives in [`topology.env`](topology.env) (which DC runs where, node counts, image).
`bin/dr redeploy` rebuilds images and re-applies manifests without touching data.

## What each DC contains (namespace `dr`)

| component | what it does |
|---|---|
| `gg` StatefulSet (3 → 5 nodes) | GridGain 9.2 nodes with persistence. `gg-0..2` are the CMG/Metastorage group. Zone `PAY`, 3 replicas per partition. |
| `payments-api` Deployment (3 pods) | Java service on the GridGain thin client, talks **only to its own DC's cluster**. |
| `lb` | Least-connections HTTP load balancer with health checks and retries. When it has no healthy local backend it **fails over to the other DCs' load balancers** (through the WAN emulator, so failover pays the inter-DC latency). |
| `loadgen` | Steady payment traffic for that DC's users (default 8 tx/s, adjustable 0–600 from the dashboard). Retries failed payments once with the same idempotency key. |
| `wan` | TCP proxy for the inter-DC links. Delays every chunk by RTT/2 (+jitter) per direction, or cuts the link entirely. **Both DCR replication traffic and failover traffic go through it**, so the latency slider is real. |
| `dc-agent` | Aggregates the status of everything in the DC and executes control actions (`kubectl`, GridGain REST, component admin APIs). The dashboard only ever talks to the three agents. |
| `gg-cli` | Admin pod holding the GridGain CLI (it is a JVM; never run it inside a node pod, it OOM-kills the node). |

## The payments application

`POST /payments {"id","from","to","amount","origin"}` runs one transaction on the local cluster: it checks both accounts
(exists, `ACTIVE`, within the daily limit) and **inserts** the payment. `GET /payments/{id}` works in *any* DC for a
payment taken in any other DC once it has replicated.

The data model is deliberately **insert-only** (`ACCOUNTS` is static reference data seeded identically in every DC and deliberately *not* replicated, `PAYMENTS` is an append-only
ledger keyed by an idempotency id, `HEARTBEAT` is one row per DC per second). That is what makes active/active safe: a
row is written by exactly one DC, so DCR's last-writer-wins conflict resolution never has anything to resolve, and
retries/failover replays are harmless (`DUPLICATE`). A retention job (90 s) keeps the working set small; the deletes replicate too.

## Scenarios to try on the dashboard

| action | what you should see |
|---|---|
| **Kill a node** (click a node circle) | cluster goes `DEGRADED`, a short blip while Raft re-elects, traffic continues; click again to recover, node rejoins → `HEALTHY` |
| **+ / − nodes** | StatefulSet scales (3–5). New nodes join the logical topology and the zone rebalances; scaling down removes the highest ordinal, one node at a time |
| **Fail DC** | all GridGain nodes + the payments tier of that DC die; its load balancer flips to `FAILOVER` and that DC's users are served — with no errors — by the other two DCs (orange arrows). **Recover DC** restarts the nodes; the cluster rejoins and DCR catches it up |
| **Isolate (partition)** | the DC keeps running and serving *its own* users, but replication to/from it stops; replication lag grows. **Heal** and watch it converge (the *Data convergence* table shows rows visible per DC, by origin) |
| **Latency sliders** | RTT between any pair of DCs, live. Lag and failover latency follow it |
| **Load slider** | change each DC's steady rate |

Direct API (same thing the dashboard uses): `curl -XPOST localhost:8088/api/action -d '{"op":"failDC","dc":"dc3"}'`
(ops: `failDC recoverDC isolateDC healDC failNode recoverNode scaleNodes scaleApps setLoad setLatency setLinkDown resetAll`).

## How replication is set up

Each DC's agent creates one DCR replication per peer through the GridGain REST API
(`POST /management/v1/dcr/replications`, then `/{name}/start` for the `PAYMENTS` and `HEARTBEAT` tables), e.g. in DC1: `from_dc2` and `from_dc3`;
source address is the local `wan` service, which forwards to the peer's published client port. Six replications in total.
The agent re-creates any replication that goes `FAILED`/`STOPPED` (e.g. after an outage longer than the source's MVCC
history window) — that triggers a fresh full state transfer. Tables, indexes and the zone are created per cluster by
`k8s/gridgain.yaml` (`schema.sql`); DCR replicates rows only.

## Notes, limits and things learned (read before a live demo)

* **GridGain 9.2.0-b1 is a beta.** Replication lag in this build is typically 0.5–3 s end to end, not milliseconds, and
  DCR apply throughput tops out at roughly 10–15 rows/s per link on this hardware. That is why the default load is a
  modest **8 tx/s per DC** (adjustable on the dashboard). Raise it and you will watch lag build — which is itself a nice demo.
* **Bound the working set.** Everything that degraded the demo in testing traced back to unbounded data growth: payments
  are trimmed after **90 s** (`RETENTION_SEC`), and the MVCC history window is 90 s (`ignite.gc.lowWatermark.dataAvailabilityTimeMillis`),
  which also means DCR resumes after an outage shorter than ~90 s; after a longer one the agent re-creates the replication
  with a full state transfer (cheap here). Don't raise the retention without also giving the nodes more memory/disk.
* **Insert-only data model.** Hot rows updated in place + DCR + MVCC GC corrupted a version chain in testing
  ("Missing next row version", node crash-loop). The ledger model avoids that and avoids write conflicts altogether.
* **Application transactions must not hold locks on replicated rows.** Reference data is read outside any transaction and is
  not replicated at all (every DC seeds the same accounts); DCR streams only `PAYMENTS` and `HEARTBEAT`.
* **Scale down one node at a time**, with the cluster `HEALTHY` and ~75 s between removals (the agent enforces this).
  Removing two replica holders of a 3-replica partition at once loses quorum for it.
* **Failure handling**: the nodes run with `failureHandler.handler.type=stopOrHalt`, so a node that hits a critical engine
  error exits and is restarted by the wrapper instead of lingering half-stopped (it would otherwise look "Running" to Kubernetes).
* The raft-log soft limits must stay large (default is 20 GB; this demo uses 2 GB). With a tiny limit the log GC spins
  ("above the soft limit but no files to compact") and starves the write path.
* `ignite.gc.batchSize` defaults to 5, far too small for delete traffic; `bin/dr up` sets 500.
* The stock arm64 image crashes on Docker Desktop (the columnar-storage JNI library segfaults on load), so
  `images/gridgain/Dockerfile` removes that jar. The demo uses `aipersist` only.
* Never run the GridGain CLI inside a node pod: it is a JVM and OOM-kills the node. Use the `gg-cli` pod (`bin/dr k dc1 exec deploy/gg-cli -- ...`).
* Each node is capped at 3 CPUs / 3.5 GiB (heap 1.5 GiB, 384 MiB page memory). Uncapped, every JVM sizes its thread pools for
  all host cores and a dozen nodes starve each other (raft RPC timeouts). Budget ~12 GB per DC at 3 nodes; max 5 nodes per DC.
  `bin/dr up` refuses to start unless llama has >= 25 GB free disk.
* Lag is measured two ways and the smaller is shown: the age of the newest payment each *other* DC served itself, and the
  age of the newest heartbeat row. Both need the hosts' clocks in sync (NTP).
* Each DC is a single-node kind cluster: "node" failures are GridGain nodes (pods), not Kubernetes workers. A "failed" node
  is a pod whose JVM is killed and held down by a marker file until recovered.
* Ports published on each host: `187xx` load balancer, `188xx` GridGain client port (DCR source), `189xx` dc-agent API
  (no auth — demo only, trusted LAN), `1644x` Kubernetes API. Per-DC `kubectl`: `bin/dr k dc2 get pods`.
* If a cluster ever gets into a bad state, `bin/dr down && bin/dr up` rebuilds everything from scratch in ~8 minutes.

## Layout

```
bin/dr                 orchestration (up / down / status / dashboard / redeploy / link / k)
topology.env           which DC runs where, versions, sizes
kind/                  kind cluster template (publishes the LB / GridGain / agent ports on the host)
k8s/gridgain.yaml      GridGain StatefulSet, config, schema, helper scripts, admin pod
k8s/apps.yaml          payments-api, lb, wan, loadgen, dc-agent
images/gridgain/       GridGain node image (columnar jar removed)
payments-app/          Java payments service
edge/                  Go: lb, wan, loadgen, dc-agent (one binary, stdlib only)
dashboard/             Go + static HTML dashboard (separate application)
```
