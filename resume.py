"""Restarts the crashed consumer seven ways at the same moment and reads until the writer is done."""
import json
import time
from datetime import datetime, timezone

from common import RESULTS, clients, list_shards, now_ms, read_jsonl, record_row

e, _, streams = clients()
arn = e["STREAM_ARN"]
RUN = (RESULTS / "run_id").read_text().strip()
processed = [p for p in read_jsonl(RESULTS / "processed.jsonl") if p["run"] == RUN]
crash = json.loads((RESULTS / "crash.json").read_text())
last = max(processed, key=lambda r: int(r["seq"]))


def ts(ms_or_s, unit="ms"):
    s = ms_or_s / 1000 if unit == "ms" else ms_or_s
    return datetime.fromtimestamp(s, tz=timezone.utc)


last_by_shard = {}
for p in processed:
    if p["shard"] not in last_by_shard or int(p["seq"]) > int(last_by_shard[p["shard"]]["seq"]):
        last_by_shard[p["shard"]] = p
newest_in_sink = max(p["acdt"] for p in processed)


def at(t):
    return {"ShardIteratorType": "AT_TIMESTAMP", "Timestamp": t}


# Each strategy maps a shard id to GetShardIterator arguments.
strategies = {
    "checkpoint per shard (AFTER_SEQUENCE_NUMBER)": lambda sid: (
        {"ShardIteratorType": "AFTER_SEQUENCE_NUMBER", "SequenceNumber": last_by_shard[sid]["seq"]}
        if sid in last_by_shard else {"ShardIteratorType": "TRIM_HORIZON"}),
    "TRIM_HORIZON": lambda sid: {"ShardIteratorType": "TRIM_HORIZON"},
    "LATEST": lambda sid: {"ShardIteratorType": "LATEST"},
    "AT_TIMESTAMP time of crash": lambda sid: at(ts(crash["crash_ms"])),
    "AT_TIMESTAMP newest record in sink": lambda sid: at(ts(newest_in_sink, "s")),
    "AT_TIMESTAMP newest record per shard": lambda sid: (
        at(ts(last_by_shard[sid]["acdt"], "s")) if sid in last_by_shard else {"ShardIteratorType": "TRIM_HORIZON"}),
    "AT_TIMESTAMP time of crash minus 5 s": lambda sid: at(ts(crash["crash_ms"] - 5000)),
}

restart_ms = now_ms()
iters = {}
used = {}
for name, args_for in strategies.items():
    for s in list_shards(streams, arn):
        sid = s["ShardId"]
        args = args_for(sid)
        used.setdefault(name, {})[sid] = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in args.items()}
        iters[(name, sid)] = streams.get_shard_iterator(StreamArn=arn, ShardId=sid, **args)["ShardIterator"]

seen = {name: [] for name in strategies}
idle = 0
while idle < 5:
    got = 0
    for key, it in list(iters.items()):
        if it is None:
            continue
        resp = streams.get_records(ShardIterator=it, Limit=1000)
        seen[key[0]] += [row["i"] for row in map(record_row, resp["Records"]) if row["run"] == RUN]
        got += len(resp["Records"])
        iters[key] = resp.get("NextShardIterator")
    writer_done = (RESULTS / "writer.done").exists()
    idle = idle + 1 if (writer_done and got == 0) else 0
    time.sleep(1)

(RESULTS / "resume.json").write_text(json.dumps({
    "restart_ms": restart_ms,
    "crash_ms": crash["crash_ms"],
    "last_processed": last,
    "iterator_args": used,
    "seen": seen,
}))
print("resume done")
