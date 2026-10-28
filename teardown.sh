#!/usr/bin/env bash
# Deletes what setup.sh and lambda_check.py created.
set -uo pipefail
cd "$(dirname "$0")"
source lab.env

FN=ddb-at-timestamp-lab-fn
ROLE=ddb-at-timestamp-lab-role

for uuid in $(aws lambda list-event-source-mappings --region "$REGION" --function-name "$FN" \
    --query 'EventSourceMappings[].UUID' --output text 2>/dev/null); do
  aws lambda delete-event-source-mapping --region "$REGION" --uuid "$uuid" >/dev/null
done
aws lambda delete-function --region "$REGION" --function-name "$FN"
aws iam detach-role-policy --role-name "$ROLE" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaDynamoDBExecutionRole
aws iam delete-role --role-name "$ROLE"
aws dynamodb delete-table --region "$REGION" --table-name "$TABLE" >/dev/null
aws dynamodb wait table-not-exists --region "$REGION" --table-name "$TABLE"
echo "deleted $TABLE, $FN, $ROLE"
