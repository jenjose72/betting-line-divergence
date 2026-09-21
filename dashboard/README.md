# dashboard/ — Role C (Serving / Integration & Reporting Engineer)

- `hdfs_reader.py` — pulls the streaming job's output tables straight off HDFS via `docker exec ... hdfs dfs -get` + `docker cp`, then reads them with pandas. Not via Hive JDBC/Thrift (HiveServer2 2.3.2's SASL handshake doesn't match modern Python Thrift clients — same class of old-library-vs-new-client mismatch that already hit Kafka and `kafka-python` earlier in this project) and not via WebHDFS's HTTP API (its file-read redirects point at the datanode container, which isn't port-published to the host). See the module docstring for the full reasoning.
- `app.py` — the Streamlit dashboard: per-match odds lines with event markers overlaid, the divergence score per window as a bar chart (red = flagged), and the underlying tables.

## Running it

```bash
streamlit run dashboard/app.py
```

Needs the Docker stack up, at least one producer run (real data in Kafka), and `streaming/divergence_job.py` to have run at least once (so HDFS has something to read). Data refreshes every 10s (or click Refresh to force it) via `st.cache_data`.

## Verified

Ran against the live stack with three matches replayed concurrently — screenshotted in the browser via the in-app preview:

- **Burnley 0-3 Man City** and **Tottenham 2-0 Man United** (both real, via `events_producer.py`/`odds_producer.py`): odds lines render correctly, event markers land at the right minutes (e.g. Tottenham's 3 yellow cards clustered at minutes 33/38/39 show up exactly there), 0 flagged windows in both — an honest result, not a bug. Since our "actual" odds come from interpolating between real opening/closing lines rather than true tick-level data, they're structurally smoother than a live feed — local volatility right around events is inherently underrepresented. Disclosed limitation of the data source, not evidence the detector doesn't work.
- **`SYNTH-LEAK-DEMO`** (via `producers/synthetic_leak_demo.py`, clearly-labeled synthetic test case, not real data): a deliberate "leaked information" pattern — odds crash with no explaining event, then the goal that would explain it happens 11 minutes later. The dashboard shows exactly what it should: home odds visibly crash well before the dotted Goal marker, the divergence chart shows one clean red (flagged) bar at that window, and the goal's own window stays unflagged (blue, since the market had already priced it in by then). This is the actual proof the detector works — the two real matches above only prove it doesn't cry wolf, not that it can catch anything. Both checks matter.

See [PROJECT_PLAN.md](../PROJECT_PLAN.md) for the full architecture. Phase 3 (dashboard + multi-match integration) is done — next is Phase 4 (final report + packaging).
