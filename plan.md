# Fluss + Paimon Tiered Streamhouse — Flink App on Local Kubernetes

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Flink App (your code)                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │  Source:      │  │  Processing  │  │  Sink / Tiering          │  │
│  │  Fluss Log    │  │  Flink SQL   │  │  Fluss → Paimon (auto)   │  │
│  │  Table        │  │  Delta Join  │  │  Union Read (view)       │  │
│  └──────────────┘  └──────────────┘  └──────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐          ┌──────────────────┐                 │
│  │  Apache Fluss     │  tier   │  Apache Paimon    │                 │
│  │  (hot: sub-sec)   │ ──────► │  (warm: minute)   │                 │
│  │  Arrow columnar   │         │  Parquet/ORC      │                 │
│  │  RocksDB backed   │         │  GCS/MinIO (S3)   │                 │
│  └──────────────────┘          └──────────────────┘                 │
├─────────────────────────────────────────────────────────────────────┤
│                    Local Kubernetes (Kind)                          │
│              Flink Operator · Fluss · MinIO · Flink Cluster        │
└─────────────────────────────────────────────────────────────────────┘
```

## Sample App: Real-Time Order Pipeline

Stream of `orders` and `inventory` events → enrich → tier → query.

---

## Phase 1 — Local K8s + Infrastructure

| Step | Action | Tool |
|------|--------|------|
| 1.1 | Create Kind cluster with local volume support + node ports | `kind` |
| 1.2 | Install Flink Kubernetes Operator | Helm chart `flink-operator` |
| 1.3 | Install MinIO (S3-compatible storage for Paimon) | Helm chart `minio` |
| 1.4 | Create MinIO bucket `paimon` + access key secret | `mc` / kubectl |
| 1.5 | Verify all pods running | `kubectl get pods -A` |

**Deliverable:** K8s cluster with Flink Operator + MinIO ready.

---

## Phase 2 — Deploy Apache Fluss

| Step | Action | Tool |
|------|--------|------|
| 2.1 | Add Fluss Helm repo & inspect chart | `helm repo add fluss` |
| 2.2 | Deploy Fluss cluster (coordinator + tablet servers) | Helm values override |
| 2.3 | Create Fluss catalog + tables via `flussctl` or Flink SQL | Flink SQL CLI |
| 2.4 | Verify Fluss endpoints & table creation | `kubectl` / curl |

**Tables to create in Fluss:**
- `orders_log` — append-only log table (raw order stream)
- `inventory_pk` — primary-key table (current stock, upserted)

**Deliverable:** Fluss cluster running with two tables.

---

## Phase 3 — Configure Paimon Catalog

| Step | Action | Tool |
|------|--------|------|
| 3.1 | Create Paimon catalog pointing to MinIO (`s3://paimon/`) | Flink SQL |
| 3.2 | Create Paimon tables matching Fluss schema | Flink SQL |
| 3.3 | Set up auto-tiering config (TTL 5 min) | Flink SQL + Fluss tier property |
| 3.4 | Verify Paimon tables & snapshots visible | Flink SQL `DESCRIBE` |

**Paimon tables:**
- `orders_archive` — bucket-partitioned, changelog-producer = lookup
- `inventory_archive` — primary key, partial-update merge engine

**Deliverable:** Paimon catalog wired to MinIO; tables ready.

---

## Phase 4 — Build Flink App (Core Logic)

| Step | Action | Details |
|------|--------|---------|
| 4.1 | **Data generator** slot | Source connector producing `orders` + `inventory` events at configurable rate |
| 4.2 | **Fluss source** | Read from `orders_log` (log table) using Flink SQL `CREATE TABLE` with Fluss connector |
| 4.3 | **Delta Join enrichment** | Enrich order with inventory data via Fluss primary-key lookup — avoids Flink state bloat |
| 4.4 | **Tiering pipeline** | INSERT INTO `paimon.orders_archive` SELECT ... FROM `fluss.orders_log` (continuous streaming) |
| 4.5 | **Union Read view** | Create a Flink SQL view: `SELECT * FROM fluss.orders_log UNION ALL SELECT * FROM paimon.orders_archive` |
| 4.6 | **Serve layer** | Optional: expose enriched stream to Kafka / WebSocket for real-time dashboard |

**Key Flink SQL patterns:**
```sql
-- Fluss source
CREATE TABLE fluss_orders (
  order_id BIGINT,
  item_id STRING,
  quantity INT,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'fluss',
  'table.name' = 'orders_log'
);

-- Delta Join: enrich via Fluss PK table instead of Flink state
CREATE TABLE fluss_inventory (
  item_id STRING PRIMARY KEY NOT ENFORCED,
  stock INT,
  last_updated TIMESTAMP(3)
) WITH (
  'connector' = 'fluss',
  'table.name' = 'inventory_pk'
);

-- Enriched view (zero Flink state for the dimension)
CREATE VIEW enriched_orders AS
SELECT o.*, i.stock AS current_stock, i.last_updated
FROM fluss_orders o
LEFT JOIN fluss_inventory FOR SYSTEM_TIME AS OF o.proctime i
ON o.item_id = i.item_id;

-- Tier to Paimon
INSERT INTO paimon.orders_archive
SELECT * FROM enriched_orders;
```

**Deliverable:** Runnable Flink SQL job (standalone or via FlinkDeployment CRD).

---

## Phase 5 — Package & Deploy to K8s

| Step | Action | Tool |
|------|--------|------|
| 5.1 | Write `FlinkDeployment` YAML CRD | YAML |
| 5.2 | Containerize init SQL / dependencies | Docker + Flink base image |
| 5.3 | Push Flink job JAR (if DataStream API) or SQL script | Docker registry |
| 5.4 | Deploy via `kubectl apply -f flink-deployment.yaml` | `kubectl` |
| 5.5 | Monitor deployment with Flink UI (port-forward) | `kubectl port-forward` |

**FlinkDeployment CRD structure:**
```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
spec:
  image: my-flink-app:latest
  flinkVersion: v1_20
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    state.savepoints.dir: s3://paimon/savepoints
  serviceAccount: flink
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom: ...
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom: ...
  job:
    jarURI: local:///opt/flink/usrlib/flink-app.jar
    entryClass: com.example.StreamhouseApp
    args: []
    parallelism: 2
    upgradeMode: stateless
```

**Deliverable:** Flink app running on K8s, tiering Fluss → Paimon continuously.

---

## Phase 6 — Observability & Validation

| Step | Action | Details |
|------|--------|---------|
| 6.1 | Flink dashboard | Port-forward `8081` — verify job latency, checkpoint size |
| 6.2 | Fluss metrics | Fluss UI / JMX — verify table throughput, storage |
| 6.3 | Paimon snapshots | `SELECT * FROM paimon.orders_archive$snapshots` |
| 6.4 | End-to-end latency test | Insert event, measure time to appear in Fluss vs Paimon |
| 6.5 | Simulate inventory upsert | Update `inventory_pk` in Fluss, observe Delta Join reflect changes |
| 6.6 | Union Read query | `SELECT * FROM union_view WHERE order_id = X` — verify Fluss + Paimon combined result |

**Deliverable:** Verified pipeline with latency numbers.

---

## Phase 7 — Cleanup & Teardown

| Step | Action |
|------|--------|
| 7.1 | Cancel Flink job (savepoint optional) |
| 7.2 | Uninstall Helm releases (Fluss, MinIO, Flink Operator) |
| 7.3 | Delete Kind cluster |

---

## Dependencies & Versions (recommended)

| Component | Version | Notes |
|-----------|---------|-------|
| Apache Flink | 1.20+ | Fluss runs as Flink connector |
| Flink K8s Operator | 1.10+ | Helm chart `flink-operator` |
| Apache Fluss | 0.8+ | Incubating; Helm chart TBD |
| Apache Paimon | 1.1+ | Flink connector bundled |
| MinIO | latest | S3-compatible for Paimon |
| Kind | 0.25+ | local K8s cluster |
| Helm | 3.x | charts |

---

## Risk & Mitigation

| Risk | Mitigation |
|------|------------|
| Fluss Helm chart immature / missing | Run Fluss as StatefulSet manually via provided manifests |
| Flink Operator CRD version mismatch | Pin `flink-operator` chart version to match Flink minor |
| Paimon S3 connectivity | Use MinIO `s3://` with path-style access; verify endpoint in `flink-conf.yaml` |
| Fluss connector not in Flink dist | Add `fluss-flink-*.jar` to `lib/` in custom Docker image |
| Flink ↔ Fluss network on K8s | Use headless services; same namespace |

---

## Milestones (Definition of Done)

- [ ] **M1** — Kind cluster up; MinIO reachable; Operator installed
- [ ] **M2** — Fluss tables created; data can be written & read
- [ ] **M3** — Paimon tables visible; Flink can INSERT into them
- [ ] **M4** — Tiering pipeline running; events flow Fluss → Paimon automatically
- [ ] **M5** — Union Read view returns combined hot + cold data
- [ ] **M6** — End-to-end latency verified (< 1 s for Fluss, < 1 min for Paimon)
