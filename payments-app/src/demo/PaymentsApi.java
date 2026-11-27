package demo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Payments API: one stateless instance per pod, talking to the *local* GridGain 9.2 cluster only.
 * Cross-DC data movement is done entirely by GridGain Data Center Replication.
 *
 *   POST /payments        {"id","from","to","amount","origin"}  -> transactional transfer
 *   GET  /payments/{id}   -> payment row (works for payments that originated in another DC)
 *   GET  /accounts/{id}   -> account row
 *   GET  /healthz         -> 200 while the local GridGain cluster is usable
 *   GET  /metrics         -> throughput / latency / replication staleness
 *   GET  /stats           -> row counts per origin DC (shows cross-DC convergence)
 */
public class PaymentsApi {
    static final String DC = env("DC_ID", "dc1");
    static final int DC_INDEX = Integer.parseInt(env("DC_INDEX", "1"));
    static final String POD = env("HOSTNAME", "local");
    static final String ADDRS = env("GG_ADDRS", "gg-ready:10800");
    static final int ACCOUNTS_PER_DC = Integer.parseInt(env("ACCOUNTS_PER_DC", "5000"));
    static final List<String> PEERS = Arrays.stream(env("PEER_DCS", "").split(","))
            .filter(s -> !s.isBlank()).toList();
    static final BigDecimal DAILY_LIMIT = new BigDecimal("10000.00");

    static volatile IgniteClient client;
    static volatile RecordView<Tuple> accounts, payments, heartbeat;
    static volatile long lastOkMs = 0;
    static volatile String lastError = "starting";
    static volatile boolean seeded = false;

    // rolling window of [timestampMs, latencyMicros, ok(1/0)]
    static final ConcurrentLinkedQueue<long[]> events = new ConcurrentLinkedQueue<>();
    static final AtomicLong totalOk = new AtomicLong(), totalErr = new AtomicLong(), rejected = new AtomicLong();
    static final Map<String, long[]> peerHeartbeat = new ConcurrentHashMap<>(); // dc -> [seq, ts, seenAt]

    static String env(String k, String d) { String v = System.getenv(k); return v == null || v.isEmpty() ? d : v; }

    public static void main(String[] args) throws Exception {
        log("starting dc=" + DC + " pod=" + POD + " gg=" + ADDRS);
        ScheduledExecutorService sched = Executors.newScheduledThreadPool(3);
        sched.scheduleWithFixedDelay(PaymentsApi::connectAndSeed, 0, 2, TimeUnit.SECONDS);
        sched.scheduleWithFixedDelay(PaymentsApi::beat, 1, 1, TimeUnit.SECONDS);
        sched.scheduleWithFixedDelay(PaymentsApi::prune, 1, 1, TimeUnit.SECONDS);

        HttpServer http = HttpServer.create(new InetSocketAddress(8080), 1024);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.createContext("/healthz", ex -> {
            boolean ok = seeded && System.currentTimeMillis() - lastOkMs < 4000;
            send(ex, ok ? 200 : 503, "{\"ok\":" + ok + ",\"dc\":\"" + DC + "\",\"error\":" + q(ok ? "" : lastError) + "}");
        });
        http.createContext("/metrics", ex -> send(ex, 200, metrics()));
        http.createContext("/stats", ex -> send(ex, 200, stats()));
        http.createContext("/admin/purge", ex -> send(ex, 200, purge()));
        http.createContext("/lag", ex -> send(ex, 200, lag()));
        http.createContext("/accounts/", ex -> {
            try {
                Tuple t = accounts.get(null, Tuple.create().set("ID", Integer.parseInt(last(ex))));
                send(ex, t == null ? 404 : 200, t == null ? "{}" : accountJson(t));
            } catch (Exception e) { send(ex, 500, err(e)); }
        });
        http.createContext("/payments", ex -> {
            try {
                if ("POST".equals(ex.getRequestMethod())) { transfer(ex); return; }
                Tuple t = payments.get(null, Tuple.create().set("ID", last(ex)));
                send(ex, t == null ? 404 : 200, t == null ? "{}" : paymentJson(t));
            } catch (Exception e) { send(ex, 500, err(e)); }
        });
        http.start();
        log("http listening on :8080");
    }

    // ---------------------------------------------------------------- GridGain connectivity
    static synchronized void connectAndSeed() {
        try {
            if (client == null) {
                client = IgniteClient.builder()
                        .addresses(ADDRS.split(","))
                        .connectTimeout(3000)
                        .operationTimeout(4000)
                        .backgroundReconnectInterval(1000)
                        .backgroundReResolveAddressesInterval(5000)
                        .build();
            }
            if (accounts == null) {
                accounts = client.tables().table("ACCOUNTS").recordView();
                payments = client.tables().table("PAYMENTS").recordView();
                heartbeat = client.tables().table("HEARTBEAT").recordView();
            }
            if (!seeded) {
                seed();
                seeded = true;
                lastOkMs = System.currentTimeMillis();
                log("seeded accounts for " + DC);
            }
        } catch (Exception e) {
            lastError = rootMsg(e);
            log("connect/seed failed: " + lastError);
            if (accounts == null) { try { if (client != null) client.close(); } catch (Exception ignore) {} client = null; }
        }
    }

    /**
     * ACCOUNTS is static reference data, so every DC seeds the full set locally and it is NOT replicated (no DCR traffic
     * for data that never changes). insertAll never overwrites existing rows, so concurrent pods and re-seeds are safe.
     */
    static void seed() {
        List<String> all = new ArrayList<>(PEERS);
        all.add(DC);
        for (String dc : all) {
            int base = Integer.parseInt(dc.replaceAll("\\D", "")) * 100000;
            List<Tuple> batch = new ArrayList<>();
            for (int i = 1; i <= ACCOUNTS_PER_DC; i++) {
                batch.add(Tuple.create().set("ID", base + i).set("OWNER", "cust-" + dc + "-" + i).set("HOME_DC", dc)
                        .set("STATUS", "ACTIVE").set("CURRENCY", "EUR").set("DAILY_LIMIT", DAILY_LIMIT));
                if (batch.size() == 500 || i == ACCOUNTS_PER_DC) { accounts.insertAll(null, batch); batch.clear(); }
            }
        }
    }

    static void beat() {
        if (!seeded) return;
        try {
            long now = System.currentTimeMillis();
            heartbeat.upsert(null, Tuple.create().set("ID", DC + "-" + (now / 1000)).set("DC", DC).set("TS", now));
            lastOkMs = now;
            ResultSet<SqlRow> rs = client.sql().execute((org.apache.ignite.tx.Transaction) null,
                    "SELECT DC, MAX(TS) FROM HEARTBEAT WHERE TS > ? GROUP BY DC", now - 120_000);
            try {
                while (rs.hasNext()) {
                    SqlRow r = rs.next();
                    peerHeartbeat.put(r.stringValue(0), new long[]{0, r.longValue(1), now});
                }
            } finally { rs.close(); }
        } catch (Exception e) {
            String m = rootMsg(e);
            if (!m.equals(lastError)) log("heartbeat failed: " + m);
            lastError = m;
        }
    }

    static void prune() {
        long cutoff = System.currentTimeMillis() - 10_000;
        long[] e;
        while ((e = events.peek()) != null && e[0] < cutoff) events.poll();
    }

    // ---------------------------------------------------------------- business logic
    static void transfer(HttpExchange ex) throws IOException {
        long t0 = System.nanoTime();
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        try {
            String id = jstr(body, "id");
            int from = Integer.parseInt(jstr(body, "from")), to = Integer.parseInt(jstr(body, "to"));
            BigDecimal amount = new BigDecimal(jstr(body, "amount"));
            String origin = jstr(body, "origin");
            if (id == null || origin == null) origin = DC;
            String[] status = new String[1];
            Exception last = null;
            for (int attempt = 0; attempt < 3; attempt++) {
                try {
                    final String fOrigin = origin;
                    // Reference data is read first; the payment itself is one idempotent insert whose key is owned by exactly one DC.
                    Tuple f = accounts.get(null, Tuple.create().set("ID", from));
                    Tuple t = accounts.get(null, Tuple.create().set("ID", to));
                    final String verdict;
                    if (f == null || t == null) verdict = "REJECTED_ACCOUNT";
                    else if (!"ACTIVE".equals(f.stringValue("STATUS")) || !"ACTIVE".equals(t.stringValue("STATUS"))) verdict = "REJECTED_STATUS";
                    else if (amount.compareTo(f.decimalValue("DAILY_LIMIT")) > 0) verdict = "REJECTED_LIMIT";
                    else verdict = "COMPLETED";
                    // single-row implicit transaction: insert() is atomic and returns false when the id already exists
                    boolean inserted = payments.insert(null, Tuple.create().set("ID", id).set("FROM_ACC", from).set("TO_ACC", to)
                            .set("AMOUNT", amount).set("STATUS", verdict).set("ORIGIN_DC", fOrigin)
                            .set("SERVED_DC", DC).set("CREATED_AT", System.currentTimeMillis()));
                    status[0] = inserted ? verdict : "DUPLICATE";   // idempotent retry / failover replay
                    last = null;
                    break;
                } catch (Exception e) { last = e; Thread.sleep(20L * (attempt + 1)); }
            }
            if (last != null) throw last;
            lastOkMs = System.currentTimeMillis();
            boolean rej = status[0].startsWith("REJECTED");
            if (rej) rejected.incrementAndGet();
            record(t0, true);
            send(ex, 200, "{\"id\":" + q(id) + ",\"status\":" + q(status[0]) + ",\"servedBy\":" + q(DC) + ",\"pod\":" + q(POD) + "}");
        } catch (Exception e) {
            record(t0, false);
            lastError = rootMsg(e);
            send(ex, 503, err(e));
        }
    }

    static void record(long t0, boolean ok) {
        events.add(new long[]{System.currentTimeMillis(), (System.nanoTime() - t0) / 1000, ok ? 1 : 0});
        (ok ? totalOk : totalErr).incrementAndGet();
    }

    // ---------------------------------------------------------------- reporting
    static String metrics() {
        long now = System.currentTimeMillis();
        List<Long> lat = new ArrayList<>();
        long ok = 0, bad = 0;
        for (long[] e : events) {
            if (e[0] < now - 5000) continue;
            if (e[2] == 1) { ok++; lat.add(e[1]); } else bad++;
        }
        Collections.sort(lat);
        StringBuilder hb = new StringBuilder("{");
        boolean first = true;
        for (String p : PEERS) {
            long[] h = peerHeartbeat.get(p);
            if (!first) hb.append(',');
            first = false;
            hb.append(q(p)).append(":").append(h == null ? "null" : String.valueOf(Math.max(0, now - h[1])));
        }
        hb.append("}");
        return "{\"dc\":" + q(DC) + ",\"pod\":" + q(POD) + ",\"lastError\":" + q(lastError) + ",\"ready\":" + (seeded && now - lastOkMs < 4000)
                + ",\"tps\":" + String.format(Locale.ROOT, "%.1f", ok / 5.0) + ",\"errPerSec\":" + String.format(Locale.ROOT, "%.1f", bad / 5.0)
                + ",\"p50ms\":" + pct(lat, 0.50) + ",\"p99ms\":" + pct(lat, 0.99)
                + ",\"totalOk\":" + totalOk + ",\"totalErr\":" + totalErr + ",\"rejected\":" + rejected
                + ",\"replicationStalenessMs\":" + hb + "}";
    }

    static volatile long statsAt = 0; static volatile String statsJson = "{}";
    static synchronized String stats() {
        long now = System.currentTimeMillis();
        if (now - statsAt < 4000) return statsJson;
        try {
            StringBuilder sb = new StringBuilder("{\"dc\":" + q(DC) + ",\"paymentsByOrigin\":{");
            ResultSet<SqlRow> rs = client.sql().execute((org.apache.ignite.tx.Transaction) null,
                    "SELECT ORIGIN_DC, COUNT(*) FROM PAYMENTS GROUP BY ORIGIN_DC");
            boolean first = true;
            try {
                while (rs.hasNext()) {
                    SqlRow r = rs.next();
                    if (!first) sb.append(',');
                    first = false;
                    sb.append(q(r.stringValue(0))).append(':').append(r.longValue(1));
                }
            } finally { rs.close(); }
            statsJson = sb.append("}}").toString();
            statsAt = now;
        } catch (Exception e) { statsJson = err(e); }
        return statsJson;
    }

    /** Replication lag as seen by the data: age of the newest payment from each *other* DC that is visible here. */
    static volatile long lagAt = 0; static volatile String lagJson = "{}";
    static synchronized String lag() {
        long now = System.currentTimeMillis();
        if (now - lagAt < 1500) return lagJson;
        try {
            ResultSet<SqlRow> rs = client.sql().execute((org.apache.ignite.tx.Transaction) null,
                    "SELECT ORIGIN_DC, MAX(CREATED_AT) FROM PAYMENTS WHERE CREATED_AT > ? AND SERVED_DC = ORIGIN_DC GROUP BY ORIGIN_DC", now - 30_000);
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            try {
                while (rs.hasNext()) {
                    SqlRow r = rs.next();
                    String o = r.stringValue(0);
                    if (o.equals(DC)) continue;
                    if (!first) sb.append(',');
                    first = false;
                    sb.append(q(o)).append(':').append(Math.max(0, now - r.longValue(1)));
                }
            } finally { rs.close(); }
            lagJson = sb.append('}').toString();
            lagAt = now;
        } catch (Exception e) { lagJson = "{}"; }
        return lagJson;
    }

    /** Hot-store retention: each DC trims the payments it originated. The deletes replicate to the other DCs. */
    static final long RETENTION_MS = Long.parseLong(env("RETENTION_SEC", "600")) * 1000;
    static String purge() {
        long cutoff = System.currentTimeMillis() - RETENTION_MS, total = 0, failed = 0;
        try {
            for (int round = 0; round < 10; round++) {          // small batches: never create a replication storm
                List<Tuple> keys = new ArrayList<>();
                ResultSet<SqlRow> rs = client.sql().execute((org.apache.ignite.tx.Transaction) null,
                        "SELECT ID FROM PAYMENTS WHERE ORIGIN_DC = ? AND CREATED_AT < ? LIMIT 100", DC, cutoff);
                try { while (rs.hasNext()) keys.add(Tuple.create().set("ID", rs.next().stringValue(0))); } finally { rs.close(); }
                if (keys.isEmpty()) break;
                try { payments.deleteAll(null, keys); total += keys.size(); }
                catch (Exception e) {                            // a batch can collide with DCR re-applying our own rows: go row by row
                    for (Tuple k : keys) { try { payments.delete(null, k); total++; } catch (Exception ex) { failed++; } }
                }
                if (keys.size() < 100) break;
            }
            // heartbeat rows older than 2 minutes
            ResultSet<SqlRow> hs = client.sql().execute((org.apache.ignite.tx.Transaction) null,
                    "SELECT ID FROM HEARTBEAT WHERE DC = ? AND TS < ? LIMIT 200", DC, System.currentTimeMillis() - 120_000);
            List<Tuple> hk = new ArrayList<>();
            try { while (hs.hasNext()) hk.add(Tuple.create().set("ID", hs.next().stringValue(0))); } finally { hs.close(); }
            for (Tuple k : hk) { try { heartbeat.delete(null, k); } catch (Exception ex) { failed++; } }
            return "{\"purged\":" + total + ",\"failed\":" + failed + ",\"dc\":" + q(DC) + "}";
        } catch (Exception e) { return err(e); }
    }

    static long pct(List<Long> sorted, double p) {
        if (sorted.isEmpty()) return 0;
        return Math.round(sorted.get(Math.min(sorted.size() - 1, (int) (sorted.size() * p))) / 1000.0);
    }

    static String accountJson(Tuple t) {
        return "{\"id\":" + t.intValue("ID") + ",\"owner\":" + q(t.stringValue("OWNER")) + ",\"homeDc\":" + q(t.stringValue("HOME_DC"))
                + ",\"status\":" + q(t.stringValue("STATUS")) + ",\"dailyLimit\":" + t.decimalValue("DAILY_LIMIT").toPlainString() + ",\"servedBy\":" + q(DC) + "}";
    }

    static String paymentJson(Tuple t) {
        return "{\"id\":" + q(t.stringValue("ID")) + ",\"from\":" + t.intValue("FROM_ACC") + ",\"to\":" + t.intValue("TO_ACC")
                + ",\"amount\":" + t.decimalValue("AMOUNT").toPlainString() + ",\"status\":" + q(t.stringValue("STATUS"))
                + ",\"originDc\":" + q(t.stringValue("ORIGIN_DC")) + ",\"servedDc\":" + q(t.stringValue("SERVED_DC"))
                + ",\"readFrom\":" + q(DC) + "}";
    }

    // ---------------------------------------------------------------- helpers
    static String last(HttpExchange ex) { String p = ex.getRequestURI().getPath(); return p.substring(p.lastIndexOf('/') + 1); }
    static String q(String s) { return s == null ? "null" : "\"" + s.replace("\\", "\\\\").replace("\"", "'").replace("\n", " ") + "\""; }
    static String err(Exception e) { return "{\"error\":" + q(rootMsg(e)) + ",\"dc\":" + q(DC) + "}"; }
    static String rootMsg(Throwable e) { Throwable r = e; while (r.getCause() != null) r = r.getCause(); String m = r.getClass().getSimpleName() + ": " + r.getMessage(); return m.length() > 300 ? m.substring(0, 300) : m; }
    static String jstr(String json, String key) {
        Matcher m = Pattern.compile("\"" + key + "\"\\s*:\\s*(\"([^\"]*)\"|[-0-9.]+)").matcher(json);
        return m.find() ? (m.group(2) != null ? m.group(2) : m.group(1)) : null;
    }
    static void send(HttpExchange ex, int code, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(code, b.length);
        try (var os = ex.getResponseBody()) { os.write(b); }
    }
    static void log(String s) { System.out.println(java.time.Instant.now() + " " + s); }
}
