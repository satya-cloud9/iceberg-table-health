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
