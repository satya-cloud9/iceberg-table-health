"""Run every statement in a SQL file with Spark and print each result.

Generic entry point for Glue-Lite SQL jobs (smoke test, metrics, compaction),
so one image serves them all and a job is just a SQL file plus catalog config
(supplied as sparkConf by the SparkApplication).

Usage: run_sql.py <path-to-sql-file>
"""
import re
import sys

from pyspark.sql import SparkSession


def statements(sql_text):
    # Drop "--" line comments, then split on ";". Good enough for the job
    # SQL in this repo (no semicolons inside string literals).
    no_comments = re.sub(r"--[^\n]*", "", sql_text)
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: run_sql.py <path-to-sql-file>")
    path = sys.argv[1]
    with open(path, encoding="utf-8") as f:
        stmts = statements(f.read())

    spark = SparkSession.builder.appName(f"run_sql:{path}").getOrCreate()
    for i, stmt in enumerate(stmts, 1):
        first_line = stmt.splitlines()[0]
        print(f"\n=== [{i}/{len(stmts)}] {first_line}", flush=True)
        df = spark.sql(stmt)
        if df.columns:
            df.show(50, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
