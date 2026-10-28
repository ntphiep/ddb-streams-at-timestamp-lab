"""Polls every shard once per second, then exits CRASH_AFTER seconds after start without saving a checkpoint."""
import json
import os
import sys
import time

from common import RESULTS, clients, list_shards, now_ms, record_row

CRASH_AFTER = float(sys.argv[1]) if len(sys.argv) > 1 else 90
POLL_SECONDS = 1.0

e, _, streams = clients()
arn = e["STREAM_ARN"]
iters = {}
start = time.time()
last_refresh = 0
out = open(RESULTS / "processed.jsonl", "w")

while True:
    if time.time() - last_refresh > 10:
        for s in list_shards(streams, arn):
            sid = s["ShardId"]
            if sid not in iters:
                iters[sid] = streams.get_shard_iterator(
                    StreamArn=arn, ShardId=sid, ShardIteratorType="TRIM_HORIZON"
                )["ShardIterator"]
        last_refresh = time.time()

    for sid, it in list(iters.items()):
        if it is None:
            continue
        resp = streams.get_records(ShardIterator=it, Limit=1000)
        for r in resp["Records"]:
            row = record_row(r)
            row["shard"] = sid
            row["processed_ms"] = now_ms()
            out.write(json.dumps(row) + "\n")
        out.flush()
        iters[sid] = resp.get("NextShardIterator")

    if time.time() - start >= CRASH_AFTER:
        crash_ms = now_ms()
        (RESULTS / "crash.json").write_text(json.dumps({"crash_ms": crash_ms, "shards": list(iters)}))
        print(f"consumer died at {crash_ms}")
        os._exit(1)
    time.sleep(POLL_SECONDS)
