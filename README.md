# ddb-streams-at-timestamp-lab

On 2026-10-15 DynamoDB Streams added the `AT_TIMESTAMP` shard iterator
([announcement](https://aws.amazon.com/about-aws/whats-new/2026/10/amazon-dynamodb-streams-timestamp-processing),
[`GetShardIterator` API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetShardIterator.html)).
Before that, a consumer that lost its checkpoint could only restart from `TRIM_HORIZON` or `LATEST`.

This lab crashes a stream consumer without a checkpoint and restarts it in seven ways at the same moment.
For each restart it counts the records that were read twice and the records that were never read.

## Results

3 runs, 4,800 writes each, consumer stopped at 90 s and restarted 60 s later
(`python summarize.py`, raw files in `results/r*/`):

| Restart from | Read twice | Never read |
|---|---|---|
| Checkpoint per shard (`AFTER_SEQUENCE_NUMBER`) | 0, 0, 0 | 0, 0, 0 |
| `TRIM_HORIZON` | 1,867, 1,854, 1,845 | 0, 0, 0 |
| `LATEST` | 0, 0, 0 | 1,301, 1,296, 1,322 |
| `AT_TIMESTAMP` at the crash time | 0, 0, 0 | 19, 8, 36 |
| `AT_TIMESTAMP` at the newest record processed, one value for all shards | 5, 6, 1 | 0, 2, 14 |
| `AT_TIMESTAMP` at the newest record processed in each shard | 11, 13, 10 | 0, 0, 0 |
| `AT_TIMESTAMP` at the crash time minus 5 s | 80, 92, 64 | 0, 0, 0 |

Other checks:

| Check | Result | File |
|---|---|---|
| Fractional part of `ApproximateCreationDateTime` on 4,800 records | always `.000` | `results/r151250/precision.txt` |
| `AT_TIMESTAMP` at 250 ms steps inside one second | starts near that millisecond, not at the start of the second | `results/r151250/precision.txt` |
| `AT_TIMESTAMP` 5 minutes in the future | `ValidationException` | `results/r151250/precision.txt` |
| `AT_TIMESTAMP` 23 hours in the past, before the stream existed | accepted, no error | `results/r151250/precision.txt` |
| Lambda event source mapping with `StartingPosition=AT_TIMESTAMP` | `InvalidParameterValueException: Unsupported starting position for arn type` | `results/lambda_check.txt` |

## What the numbers say

The crash time is the wrong restart point when the consumer is behind. Every record the crash-time restart
missed had been written before the crash, the oldest 2.5 s before it, but the consumer had not polled it yet.

The newest record a shard has processed is a safe restart point for that shard, at the cost of a few
duplicates. `ApproximateCreationDateTime` is rounded down to the second
([`StreamRecord`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_StreamRecord.html)),
while the iterator seeks to the millisecond. A timestamp copied from a record therefore rewinds by up to one second.

Shards are read independently. One timestamp for the whole stream is only as good as the slowest shard,
which is why the single "newest record processed" value still lost records in 2 of 3 runs.

Lambda triggers cannot use the new iterator yet. The console offers only Latest and Trim horizon, and the
API rejects `AT_TIMESTAMP` for a DynamoDB stream. The announcement says the Kinesis Client Library (KCL) supports
it through the DynamoDB Streams Kinesis adapter. This lab does not test KCL.

Scope: one on-demand table in us-east-1, 4 shards, 20 writes per second, 3 runs. The client ran on a laptop
in Vietnam with about 240 ms per API call. Read the counts as relative, not as rates you should expect.

## How a run works

1. `writer.py` puts 20 items per second for 240 s. Each item carries a run id and a sequence number `i`.
2. `consumer.py` reads every shard from `TRIM_HORIZON` once per second, logs each record, and exits at 90 s
   without saving a checkpoint.
3. 60 s later, `resume.py` opens seven sets of shard iterators, one per restart strategy, and reads until
   the writer has finished and the stream is idle.
4. `analyze.py` compares what each strategy read with what the consumer had already processed.

## Running it

Requirements: AWS CLI v2 and credentials with access to DynamoDB, DynamoDB Streams, IAM and Lambda,
Python 3.13 and boto3 1.43.120. The scripts call the API through boto3 because AWS CLI 2.35.23 has no
`--timestamp` option for `get-shard-iterator` yet.

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./setup.sh                                # creates the table and stream, writes lab.env
./run.sh                                  # one run, about 6 minutes, output in results/<run id>/
.venv/bin/python summarize.py             # all runs side by side
.venv/bin/python precision.py <run id>    # AT_TIMESTAMP inside one second
.venv/bin/python lambda_check.py          # Lambda event source mapping with each starting position
./teardown.sh                             # deletes the table, the Lambda function and its IAM role
```

The 3 runs cost well under 0.10 USD at us-east-1 on-demand prices, by my estimate.

## Files

| File | Purpose |
|---|---|
| `setup.sh`, `teardown.sh` | Create and delete the AWS resources |
| `common.py` | Shared helpers: config, boto3 clients, shard listing, record parsing |
| `writer.py` | Writes items at a fixed rate and logs send and acknowledge times |
| `consumer.py` | Reads all shards and exits without a checkpoint |
| `resume.py` | Restarts the consumer seven ways at the same moment |
| `analyze.py` | Read-twice and never-read counts for one run |
| `summarize.py` | All runs side by side |
| `precision.py` | `AT_TIMESTAMP` at 250 ms steps, plus future and old timestamps |
| `lambda_check.py` | Lambda event source mapping with `AT_TIMESTAMP` and `TRIM_HORIZON` |
| `run.sh` | One full run |
| `results/` | Raw output of the 3 runs (`r150302`, `r150800`, `r151250`) |

The account ID in `results/lambda_check.txt` is replaced with `123456789012`.

## License

[MIT](LICENSE)
