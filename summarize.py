"""Puts the restart counts of every run in results/ side by side."""
import json

from common import RESULTS, read_jsonl

runs = sorted(p for p in RESULTS.iterdir() if p.is_dir() and (p / "resume.json").exists())
labels = {
    "checkpoint per shard (AFTER_SEQUENCE_NUMBER)": "checkpoint per shard",
    "AT_TIMESTAMP time of crash minus 5 s": "AT_TIMESTAMP crash time - 5 s",
    "AT_TIMESTAMP time of crash": "AT_TIMESTAMP crash time",
}
table = {}
for run in runs:
    writes = {w["i"] for w in read_jsonl(run / "writes.jsonl")}
    rid = (run / "run_id").read_text().strip()
    done = {p["i"] for p in read_jsonl(run / "processed.jsonl") if p["run"] == rid}
    for name, seen in json.loads((run / "resume.json").read_text())["seen"].items():
        s = set(seen)
        table.setdefault(labels.get(name, name), []).append((len(s & done), len(writes - done - s)))

print(f"{len(runs)} runs, {len(writes):,} writes each, consumer killed at 90 s, restarted 60 s later\n")
print(f"{'':38}{'read twice':>18}{'never read':>20}")
runs_hdr = " ".join(f"{'run ' + str(k + 1):>5}" for k in range(len(runs)))
print(f"{'restart from':38}{runs_hdr:>18}{runs_hdr:>20}")
for name, cells in table.items():
    twice = " ".join(f"{t:>5}" for t, _ in cells)
    lost = " ".join(f"{l:>5}" for _, l in cells)
    print(f"{name:38}{twice:>18}{lost:>20}")
