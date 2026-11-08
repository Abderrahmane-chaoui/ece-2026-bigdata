#!/usr/bin/env bash
# Usage: polaris_lab/run.sh STEP PRINCIPAL  -> runs one step of lab_polaris.py, output in polaris_lab/output_STEP.txt
cd /home/onyxia/work/ece-2026-bigdata
eval "$(aws configure export-credentials --profile default --format env)"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE" POLARIS_URI="http://polaris:8181"
export POLARIS_CATALOG="$LAB_BUCKET_NAME" AWS_REGION="us-east-1"
set -a; . /home/onyxia/.polaris-credentials.env; set +a
export STEP="$1" POLARIS_PRINCIPAL="$2"
export METADATA_VERSION=$(aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/warehouse/silver/orders/metadata/version-hint.text" - 2>/dev/null)
spark-submit --driver-memory 480m --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  polaris_lab/lab_polaris.py > "polaris_lab/output_$1.txt" 2> "polaris_lab/stderr_$1.log"
echo "exit $? -> polaris_lab/output_$1.txt"
