#!/usr/bin/env bash
# GL0: spark-jobs namespace + quota, the aws-creds Secret Spark jobs read,
# the warehouse bucket, and the Spark Operator (pinned chart version).
# Needs the emulated EKS cluster from `make provider-apply`.
set -euo pipefail
source "$(dirname "$0")/env.sh"

echo "=== Cluster: $(kubectl config current-context) (KUBECONFIG=$KUBECONFIG) ==="
kubectl get nodes

echo "=== Warehouse bucket s3://$WAREHOUSE_BUCKET (ok if it already exists) ==="
docker run --rm --network host \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_REGION="$AWS_REGION" \
  amazon/aws-cli --endpoint-url "$FLOCI_ENDPOINT" s3 mb "s3://$WAREHOUSE_BUCKET" 2>/dev/null || true

echo "=== spark-jobs namespace and quota ==="
kubectl apply -f "$GL_ROOT/k8s/spark-jobs.yaml"

echo "=== aws-creds Secret (Floci accepts any static keys; GL4 replaces this with IRSA) ==="
kubectl -n spark-jobs create secret generic aws-creds \
  --from-literal=AWS_ACCESS_KEY_ID=test \
  --from-literal=AWS_SECRET_ACCESS_KEY=test \
  --dry-run=client -o yaml | kubectl apply -f -

echo "=== Spark Operator $SPARK_OPERATOR_VERSION ==="
helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
helm repo update spark-operator >/dev/null
helm upgrade --install spark-operator spark-operator/spark-operator \
  --version "$SPARK_OPERATOR_VERSION" \
  --namespace spark-operator --create-namespace \
  -f "$GL_ROOT/helm/spark-operator-values.yaml" \
  --wait --timeout 10m

kubectl -n spark-operator get pods
kubectl -n spark-jobs get serviceaccount spark
echo ""
echo "Next: bash flavors/glue-lite/scripts/03-build-load-image.sh"
