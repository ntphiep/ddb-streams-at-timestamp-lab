"""Shared helpers: lab.env, boto3 clients, shard listing and stream record parsing."""
import json
import time
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).parent
RESULTS = HERE / "results"


def env():
    out = {}
    for line in (HERE / "lab.env").read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k] = v
    return out


def clients():
    e = env()
    cfg = Config(retries={"mode": "standard", "max_attempts": 10}, max_pool_connections=32)
    ddb = boto3.client("dynamodb", region_name=e["REGION"], config=cfg)
    streams = boto3.client("dynamodbstreams", region_name=e["REGION"], config=cfg)
    return e, ddb, streams


def now_ms():
    return int(time.time() * 1000)


def list_shards(streams, arn):
    shards, start = [], None
    while True:
        kw = {"StreamArn": arn}
        if start:
            kw["ExclusiveStartShardId"] = start
        d = streams.describe_stream(**kw)["StreamDescription"]
        shards += d["Shards"]
        start = d.get("LastEvaluatedShardId")
        if not start:
            return shards


def record_row(r):
    sr = r["dynamodb"]
    return {
        "run": sr["NewImage"].get("run", {}).get("S"),
        "i": int(sr["NewImage"]["i"]["N"]),
        "seq": sr["SequenceNumber"],
        "acdt": sr["ApproximateCreationDateTime"].timestamp(),
    }


def read_jsonl(path):
    return [json.loads(l) for l in Path(path).read_text().splitlines() if l.strip()]
