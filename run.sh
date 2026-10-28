#!/usr/bin/env bash
# One run: writer for 240 s at 20 items/s, consumer dies at 90 s, restart at 150 s.
# Files of the run end up in results/<run id>/.
set -euo pipefail
cd "$(dirname "$0")"
PY=${PY:-.venv/bin/python}
RUN=$(date -u +r%H%M%S)
echo "$RUN" > results/run_id
rm -f results/writes.jsonl results/processed.jsonl results/crash.json results/resume.json results/writer.done

$PY writer.py 20 240 &
WRITER=$!
sleep 2
$PY consumer.py 90 || true
sleep 60
$PY resume.py
wait $WRITER
$PY analyze.py | tee results/summary.txt
mkdir -p "results/$RUN"
mv results/run_id results/writes.jsonl results/processed.jsonl results/crash.json results/resume.json \
   results/writer.done results/summary.txt "results/$RUN/"
