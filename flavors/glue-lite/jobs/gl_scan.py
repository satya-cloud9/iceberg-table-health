"""GL2.5d: the catalog scan in one Spark job: metrics -> symptoms -> scorecard.

One job instead of three keeps it to a single pod start, and s3 is measured
first so a freshly built hot partition is still inside the hot window.

Usage (via scripts/run-job.sh py gl_scan.py ...):
  gl_scan.py [--namespace glue.demo] [--no-scorecard]
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession

import gl_common as gl
from detect_symptoms import run_detect
from scan_metrics import run_scan
from scorecard import run_scorecard

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--namespace", default="glue.demo")
    p.add_argument("--config", default=os.path.join(HERE, "config", "health.json"))
    p.add_argument("--expectations", default=os.path.join(HERE, "config", "expectations.json"))
    p.add_argument("--priority", default="s3_hot_partition",
                   help="comma-separated tables to measure first")
    p.add_argument("--no-scorecard", action="store_true")
    p.add_argument("--full", action="store_true", help="measure every table, even unchanged ones")
    a = p.parse_args()

    config = gl.load_config(a.config)
    spark = SparkSession.builder.appName("gl25-scan").getOrCreate()
    scan_id = run_scan(spark, a.namespace, config, report=False, full=a.full,
                       priority=[t.strip() for t in a.priority.split(",") if t.strip()])
    run_detect(spark, scan_id, config)
    if not a.no_scorecard:
        run_scorecard(spark, scan_id, config, gl.load_config(a.expectations))
    spark.stop()


if __name__ == "__main__":
    main()
