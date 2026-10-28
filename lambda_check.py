"""Tries to attach a Lambda function to the table's stream with each starting position."""
import io
import json
import time
import zipfile
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

from common import RESULTS, env

e = env()
iam = boto3.client("iam")
lam = boto3.client("lambda", region_name=e["REGION"])
ROLE, FN = "ddb-at-timestamp-lab-role", "ddb-at-timestamp-lab-fn"

try:
    role_arn = iam.create_role(RoleName=ROLE, AssumeRolePolicyDocument=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    }))["Role"]["Arn"]
    iam.attach_role_policy(RoleName=ROLE, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaDynamoDBExecutionRole")
    time.sleep(10)
except iam.exceptions.EntityAlreadyExistsException:
    role_arn = iam.get_role(RoleName=ROLE)["Role"]["Arn"]

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w") as z:
    z.writestr("app.py", "def handler(event, context):\n    return len(event['Records'])\n")
try:
    lam.create_function(FunctionName=FN, Runtime="python3.13", Role=role_arn, Handler="app.handler",
                        Code={"ZipFile": buf.getvalue()}, Architectures=["arm64"])
    lam.get_waiter("function_active_v2").wait(FunctionName=FN)
except lam.exceptions.ResourceConflictException:
    pass

out = []
for pos in ("AT_TIMESTAMP", "TRIM_HORIZON"):
    kw = dict(EventSourceArn=e["STREAM_ARN"], FunctionName=FN, StartingPosition=pos, Enabled=False)
    if pos == "AT_TIMESTAMP":
        kw["StartingPositionTimestamp"] = datetime.now(timezone.utc) - timedelta(minutes=10)
    try:
        uuid = lam.create_event_source_mapping(**kw)["UUID"]
        line = f"{pos}: accepted (mapping {uuid}, created disabled)"
        lam.delete_event_source_mapping(UUID=uuid)
    except ClientError as ex:
        line = f"{pos}: {ex.response['Error']['Code']}: {ex.response['Error']['Message']}"
    print(line)
    out.append(line)
(RESULTS / "lambda_check.txt").write_text("\n".join(out) + "\n")
