# dashboard/ — Role C (Serving / Integration & Reporting Engineer)

- `hdfs_reader.py` — pulls the streaming job's output tables straight off HDFS via `docker exec ... hdfs dfs -get` + `docker cp`, then reads them with pandas. Not via Hive JDBC/Thrift (HiveServer2 2.3.2's SASL handshake doesn't match modern Python Thrift clients — same class of old-library-vs-new-client mismatch that already hit Kafka and `kafka-python` earlier in this project) and not via WebHDFS's HTTP API (its file-read redirects point at the datanode container, which isn't port-published to the host). See the module docstring for the full reasoning.
- `app.py` — the Streamlit dashboard: per-match odds lines with event markers overlaid, the divergence score per window as a bar chart (red = flagged), and the underlying tables.

## Running it

```bash
streamlit run dashboard/app.py
```

Needs the Docker stack up, at least one producer run (real data in Kafka), and `streaming/divergence_job.py` to have run at least once (so HDFS has something to read). Data refreshes every 10s (or click Refresh to force it) via `st.cache_data`.

## Verified

Ran against the live stack with two real matches replayed concurrently (Burnley 0-3 Man City, Tottenham 2-0 Man United) — screenshotted in the browser via the in-app preview. Confirmed for both: odds lines render correctly, event markers land at the right minutes (visually matches `events_producer.py`'s verified output — e.g. Tottenham's 3 yellow cards clustered at minutes 33/38/39 show up exactly there), divergence bars render per window, match selector switches cleanly between matches, and the raw data tables match what `streaming/divergence_job.py` produced.

See [PROJECT_PLAN.md](../PROJECT_PLAN.md) for the full architecture. Phase 3 (dashboard + multi-match integration) is done — next is Phase 4 (final report + packaging).
