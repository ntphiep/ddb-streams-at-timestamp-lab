"""Asks for AT_TIMESTAMP at several points inside the same second and shows which record comes back first."""
import json
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

from common import RESULTS as BASE, clients, list_shards, read_jsonl, record_row

RESULTS = BASE / sys.argv[1] if len(sys.argv) > 1 else BASE

e, _, streams = clients()
arn = e["STREAM_ARN"]
RUN = (RESULTS / "run_id").read_text().strip()
writes = {w["i"]: w for w in read_jsonl(RESULTS / "writes.jsonl")}
shards = list_shards(streams, arn)


def read_from(sid, **kw):
    it = streams.get_shard_iterator(StreamArn=arn, ShardId=sid, **kw)["ShardIterator"]
    for _ in range(20):
        resp = streams.get_records(ShardIterator=it, Limit=1000)
        if resp["Records"]:
            return [record_row(r) for r in resp["Records"]]
        it = resp.get("NextShardIterator")
        if it is None:
            return []
    return []


# Full stream once, to see what ApproximateCreationDateTime looks like.
stream = []
for s in shards:
    it = streams.get_shard_iterator(StreamArn=arn, ShardId=s["ShardId"], ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
    empty = 0
    while it and empty < 3:
        resp = streams.get_records(ShardIterator=it, Limit=1000)
        stream += [dict(record_row(r), shard=s["ShardId"]) for r in resp["Records"]]
        empty = 0 if resp["Records"] else empty + 1
        it = resp.get("NextShardIterator")
stream = [r for r in stream if r["run"] == RUN]
fractions = Counter(round(r["acdt"] % 1, 3) for r in stream)
print(f"stream records: {len(stream)}, distinct fractional parts of ApproximateCreationDateTime: {dict(fractions.most_common(5))}")

by_i = {r["i"]: r for r in stream}
seconds = sorted({int(r["acdt"]) for r in stream})
picked = seconds[len(seconds) // 4: len(seconds) // 4 + 10]
rows = []
for sec in picked:
    in_sec = sorted(i for i, r in by_i.items() if int(r["acdt"]) == sec)
    sid = by_i[in_sec[0]]["shard"]
    for off in (0, 250, 500, 750, 999):
        t = datetime.fromtimestamp(sec, tz=timezone.utc) + timedelta(milliseconds=off)
        recs = read_from(sid, ShardIteratorType="AT_TIMESTAMP", Timestamp=t)
        first = recs[0]["i"] if recs else None
        skipped = sum(1 for i in in_sec if first is not None and i < first)
        rows.append({"second": sec, "offset_ms": off, "first_i": first, "items_in_second": len(in_sec),
                     "first_item_of_second": in_sec[0], "skipped_in_second": skipped,
                     "first_written_ms": writes[first]["before_ms"] if first is not None else None})
        print(f"{t.strftime('%H:%M:%S.%f')[:-3]}  first i={first}  items in that second={len(in_sec)}  skipped={skipped}  "
              f"first item written at {datetime.fromtimestamp(writes[first]['before_ms']/1000, tz=timezone.utc).strftime('%H:%M:%S.%f')[:-3]}")

(RESULTS / "precision.json").write_text(json.dumps(rows))

for label, t in (("future", datetime.now(timezone.utc) + timedelta(minutes=5)),
                 ("before stream existed", datetime.now(timezone.utc) - timedelta(hours=23))):
    try:
        recs = read_from(shards[0]["ShardId"], ShardIteratorType="AT_TIMESTAMP", Timestamp=t)
        print(f"{label}: first i={recs[0]['i'] if recs else None}")
    except Exception as ex:
        print(f"{label}: {type(ex).__name__}: {ex}")
