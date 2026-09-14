# dashboard/ — Role C (Serving / Integration & Reporting Engineer)

`app.py` — a Streamlit dashboard that queries the Hive table `divergence_job.py` writes to and renders:

- The live odds line vs. the expected-movement baseline, ticking as a match replays.
- Divergence flags overlaid at the minute they fire.

Run with:

```bash
streamlit run dashboard/app.py
```

Also owns: README top-level polish, demo recording, and pulling Role A/B's technical write-ups into the final report. See [PROJECT_PLAN.md](../PROJECT_PLAN.md) for the team role breakdown.
