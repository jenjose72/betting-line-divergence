# streaming/ — Role B (Analytics / Spark Engineer)

`divergence_job.py` — the Spark Structured Streaming job:

1. Reads both `odds-ticks` and `match-events` from Kafka.
2. Windowed join on match id + time window.
3. Computes the expected-odds-movement baseline from events in that window.
4. Scores actual vs. expected movement and flags divergence.
5. Writes results (raw scores + flags) to a Hive external table backed by HDFS.

Submit with:

```bash
docker exec linewatch-spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
  /workspace/streaming/divergence_job.py
```

See [PROJECT_PLAN.md](../PROJECT_PLAN.md) for the divergence-scoring model design.
