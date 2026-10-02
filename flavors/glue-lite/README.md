# Glue-Lite flavor

A slim stack for showing Spark compaction and metrics-driven automation early:
Kestra, Spark Operator, Glue Data Catalog, S3 and Iceberg. No Trino, Nessie,
dbt, tenants or observability. Built on Floci first, then promoted to real EKS.

## GL0 step 1 — Glue spike (go/no-go)

Checks whether Iceberg's `GlueCatalog` can create and commit to a table
against Floci's Glue emulation. Needs only the Floci emulator running, not the
Kubernetes cluster.

```bash
make emulator-up                                 # if lakehouse-floci isn't running
bash flavors/glue-lite/spike/run-glue-spike.sh
```

The first run downloads the Spark image and the Iceberg jars, which takes a
few minutes.

- **Pass:** 5 rows, 2 `append` snapshots, 3 partitions, and `get-table` shows
  a `metadata_location` in S3. Continue GL0–GL3 on Floci.
- **Fail:** an error on `CREATE TABLE` or the second `INSERT`. Move GL0 to
  real EKS; the Spark image and jobs stay the same.

Overrides (environment variables): `FLOCI_ENDPOINT`, `WAREHOUSE_BUCKET`,
`AWS_REGION`, `SPARK_IMAGE` (default `apache/spark:3.5.6`), `ICEBERG_VERSION`
(default `1.10.0`).

**Result (2026-10-02): PASS** on Floci nightly — 2 snapshots, 3 partitions,
Glue `metadata_location` advanced `00001` → `00002`. GL0–GL3 run on Floci.

## GL0 step 2 — Spark in the cluster

Brings up only the emulated EKS cluster (no platform or tenant layers), installs
the Spark Operator, loads the Spark image and runs the same checks as the spike
as a `SparkApplication`. Proves pod → Floci networking, the image and the
operator together.

```bash
make gl-cluster          # Floci + emulated EKS (same as emulator-up + provider-apply)
make gl-spark-operator   # spark-jobs namespace, quota, aws-creds Secret, operator 2.5.2
make gl-image            # build glue-lite-spark:local, import into the nodes
make gl-smoke            # run jobs/sql/smoke.sql, print the driver output
```

Pass: the smoke job ends `COMPLETED` and prints 5 rows, 2 `append` snapshots
and 3 partitions for `glue.demo.smoke_events`.

### How it fits together

| Path | What it is |
|---|---|
| `docker/spark.Dockerfile` | `apache/spark:3.5.6` + Iceberg runtime and AWS bundle 1.10.0 + `jobs/` |
| `jobs/run_sql.py` | runs every statement in one SQL file and prints results |
| `jobs/sql/*.sql` | one file per job (`smoke.sql` now; generator, metrics, compaction next) |
| `k8s/sparkapp-sql.tmpl.yaml` | the one SparkApplication template every job renders |
| `k8s/spark-jobs.yaml` | job namespace + ResourceQuota |
| `helm/spark-operator-values.yaml` | operator watches `spark-jobs`, creates the `spark` service account |
| `scripts/env.sh` | shared settings; overrides via env vars |
| `scripts/run-sql-job.sh <name>` | render, submit, wait, print driver log for `jobs/sql/<name>.sql` |

Pods reach Floci at `http://172.17.0.1:4566` (Docker bridge gateway, the same
address the base repo's aws provider uses). Override with `AWS_ENDPOINT` if
your Docker network differs.

Every change to `jobs/` needs `make gl-image` again before the next run.
