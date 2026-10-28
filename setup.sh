#!/usr/bin/env bash
# Creates the table with a stream and writes lab.env.
set -euo pipefail
cd "$(dirname "$0")"

REGION=${AWS_REGION:-us-east-1}
TABLE=${TABLE:-ddb-at-timestamp-lab}

aws dynamodb create-table --region "$REGION" --table-name "$TABLE" \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE >/dev/null
aws dynamodb wait table-exists --region "$REGION" --table-name "$TABLE"

STREAM_ARN=$(aws dynamodb describe-table --region "$REGION" --table-name "$TABLE" \
  --query Table.LatestStreamArn --output text)

cat > lab.env <<EOF
REGION=$REGION
TABLE=$TABLE
STREAM_ARN=$STREAM_ARN
EOF
cat lab.env
