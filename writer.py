"""Writes RATE items per second for SECONDS seconds and logs when each write started and was acknowledged."""
import json
import sys
import threading
import time

from common import RESULTS, clients, now_ms

RATE = float(sys.argv[1]) if len(sys.argv) > 1 else 20
SECONDS = float(sys.argv[2]) if len(sys.argv) > 2 else 240
THREADS = 16  # one put_item from my laptop to us-east-1 takes about 240 ms

e, ddb, _ = clients()
RUN = (RESULTS / "run_id").read_text().strip()
n = int(RATE * SECONDS)
start = time.time()
lock = threading.Lock()
log = open(RESULTS / "writes.jsonl", "w")


def worker(k):
    for i in range(k, n, THREADS):
        delay = start + i / RATE - time.time()
        if delay > 0:
            time.sleep(delay)
        before = now_ms()
        ddb.put_item(
            TableName=e["TABLE"],
            Item={"pk": {"S": f"{RUN}-{i:06d}"}, "run": {"S": RUN}, "i": {"N": str(i)}, "written_ms": {"N": str(before)}},
        )
        with lock:
            log.write(json.dumps({"i": i, "before_ms": before, "ack_ms": now_ms()}) + "\n")
            log.flush()


threads = [threading.Thread(target=worker, args=(k,)) for k in range(THREADS)]
for t in threads:
    t.start()
for t in threads:
    t.join()
(RESULTS / "writer.done").write_text(str(now_ms()))
print(f"wrote {n} items in {time.time() - start:.1f} s")
