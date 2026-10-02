# M0 runbook — baseline stack on Floci

Goal: the inherited lakehouse-anywhere stack runs on Floci-emulated EKS, and
a dbt model writes a day-partitioned Iceberg table through Trino and Nessie.

Run on a **Linux box** (the homelab mini PC, or Ubuntu under WSL2). The
scripts use bash, Docker, kind, kubectl, helm and OpenTofu, none of which
run natively in Windows `cmd`.

Run phases one at a time and stop at the first failure — upstream never ran
this end to end, so expect a few fixes. Paste the failing output back.

## 1. Get the branch

```bash
git clone -b feature/m0-baseline https://github.com/satya-cloud9/iceberg-table-health
cd iceberg-table-health
```

## 2. Bring the stack up, phase by phase

`PROVIDER` is pinned to `aws` in this fork; only `TENANT` can be overridden.

```bash
make preflight        # phase 0: box sizing / OS checks
make install          # phase 1: docker, kind, kubectl, helm, tofu
make emulator-up      # phase 2: Floci (nightly image, needed for EKS OIDC)
make provider-apply   # phase 3: emulated EKS cluster + contract outputs
make platform-apply   # phase 4: Nessie, Kestra, shared OLTP, observability
make tenant-apply     # phase 5: tenant-a Trino / MinIO / Postgres / Grafana
make flows            # phase 6: register the example Kestra flow
make status           # endpoints and port-forward commands
```

After each phase, a quick health check:

```bash
kubectl get pods -A | grep -v Running | grep -v Completed
```

## 3. Smoke test A — Kestra → Trino → Iceberg (upstream flow)

Port-forward Kestra (command from `make status`), open the UI, and run the
`lakehouse / ingest-to-iceberg` flow. The `verify_count` task should return
`row_count = 3`.

## 4. Smoke test B — dbt → Trino → Iceberg (new in this fork)

```bash
kubectl -n tenant-a port-forward svc/trino 8080:8080 &

python3 -m venv .venv && . .venv/bin/activate
pip install -r dbt/requirements.txt
dbt run --project-dir dbt --profiles-dir dbt --target local
```

Check the result (any Trino client, or `kubectl exec` into the Trino pod and
run `trino`):

```sql
SELECT count(*) FROM iceberg.bronze.events;                -- 7000
SELECT partition, record_count, file_count
FROM iceberg.bronze."events$partitions" ORDER BY 1;         -- 7 day partitions
```

The `$partitions` query is the same metadata the Advisor will score in M1/M3.

## 5. Optional — dbt as a Kestra-launched pod

Build the runner image (bakes in `dbt/`):

```bash
docker build -f kestra-plugins/plugin-dbt-k8s-runner/docker/dbt-runner.Dockerfile -t dbt-runner:local .
```

Loading it into the emulated cluster depends on how Floci runs the EKS nodes
(check `docker ps` / `kind get clusters` after phase 3); we'll settle the exact
load command once phase 3 is up. Then run
`kestra-plugins/plugin-dbt-k8s-runner/examples/02-dbt-run.yaml`.

## M0 done when

- [ ] Phases 0–6 complete with all pods Running
- [ ] Smoke test A returns 3 rows
- [ ] Smoke test B writes `iceberg.bronze.events` with 7 day partitions
