"""Counts, for each restart strategy, records processed twice and records never processed."""
import json
import sys
from datetime import datetime, timezone

from common import RESULTS as BASE, read_jsonl

RESULTS = BASE / sys.argv[1] if len(sys.argv) > 1 else BASE

writes = read_jsonl(RESULTS / "writes.jsonl")
RUN = (RESULTS / "run_id").read_text().strip()
processed = [p for p in read_jsonl(RESULTS / "processed.jsonl") if p["run"] == RUN]
res = json.loads((RESULTS / "resume.json").read_text())

all_i = {w["i"] for w in writes}
done = {p["i"] for p in processed}
crash_ms = res["crash_ms"]
acked_before_crash = {w["i"] for w in writes if w["ack_ms"] < crash_ms}
last = res["last_processed"]


def hms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]


print(f"items written:                  {len(all_i)}")
print(f"processed before the crash:     {len(done)} (last i={last['i']}, its ApproximateCreationDateTime {hms(last['acdt'] * 1000)})")
print(f"consumer died at:               {hms(crash_ms)}")
print(f"acked before crash, never read: {len(acked_before_crash - done)}")
print(f"restart at:                     {hms(res['restart_ms'])}")
print()
print(f"{'restart from':46} {'read':>6} {'twice':>6} {'lost':>6}")
for name, seen in res["seen"].items():
    s = set(seen)
    twice = len(s & done)
    lost = len(all_i - done - s)
    print(f"{name:46} {len(seen):>6} {twice:>6} {lost:>6}")
