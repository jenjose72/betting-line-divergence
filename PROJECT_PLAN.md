# Line Watch — Real-Time Betting-Line Divergence Detection

**CSE412 Big Data & Large-Scale Computing — Applied Project**
Team size: 3 · Timeline: 2026-09-13 → 2026-09-27 (2 weeks)

---

## 1. The Idea

**Line Watch** replays a football match's real timeline — goals, cards, substitutions — through Kafka, while simultaneously replaying that same match's real historical odds movements through a second Kafka topic. Spark Structured Streaming consumes both streams in windowed micro-batches, builds a live "expected odds movement" baseline from the events actually happening, and flags moments where the real odds moved in a way the events don't explain — i.e. the market moved before, or without, a visible trigger. Results land in Hive/HDFS and drive a live dashboard: both lines ticking in sync with the match, divergence flags lighting up in real time.

### The problem it solves

Betting markets are supposed to move only on public information. A goal, a red card, a confirmed injury — these are legitimate reasons for odds to shift, because they change the real probability of who wins. When odds move *before* such an event, or move by *more* than the event justifies, that gap is a signal: early leaks, insider knowledge, coordinated betting activity, or a bookmaker front-running suspicious volume. This is a documented pattern in sports-betting integrity work — match-fixing investigations often start exactly this way, with an analyst noticing a line move that has no news behind it.

Concretely, the system answers: *at this exact point in the match, does the market's behavior match what's actually happening on the pitch — and if not, how unusual is the gap?*

This is useful in three concrete ways:
- **Integrity monitoring** — leagues and regulators already run manual/vendor-driven surveillance for exactly this; this is a cheap, transparent, open-source version of the same idea.
- **Market inefficiency detection** — even absent any wrongdoing, a lagging or overreacting market creates value-betting opportunities, a legitimate quant-finance angle.
- **Genuine analytical depth** — this isn't static classification. It's scoring whether one live time series reacted appropriately to a second, independent time series, in real time. That's real substance to defend in Q&A, versus "we trained a model that got 80% accuracy."

### Why this genuinely needs 3 tools, not superficially

No single tool naturally handles "ingest two live streams, join them in time windows, and score deviation from expected behavior." That's precisely the batch-would-struggle, streaming-and-windowing-is-required case: Kafka has to carry two independently-timed live feeds, Spark Streaming has to join and score them per window as they arrive, and Hive/HDFS has to serve the results for querying and the dashboard. Each stage's output is genuinely the next stage's input — not three tools bolted on for the sake of the requirement.

---

## 2. Architecture

This maps directly onto **Track D** from the course guidelines: *Kafka → Spark Streaming → Hive/HDFS*.

```
football-data.co.uk (odds CSV)      API-Football (event timeline JSON)
            │                                    │
            ▼                                    ▼
     Odds producer script              Event producer script
   (interpolates + replays               (replays goals/cards/
    opening→closing odds,                 subs at real match
    weighted toward event                      minutes)
    minutes, at real-time pace)
            │                                    │
            ▼                                    ▼
   Kafka topic: "odds-ticks"          Kafka topic: "match-events"
            │                                    │
            └───────────────┬────────────────────┘
                             ▼
              Spark Structured Streaming
        (windowed join of both topics; computes
         expected-odds-movement baseline from
         events; scores actual vs. expected →
                divergence flag + score)
                             │
                             ▼
                  Hive external table / HDFS
                (queryable, persisted results)
                             │
                             ▼
              Live dashboard (both lines ticking,
                divergence flags in real time)
```

**Tool-by-tool role:**
- **Kafka** — dual-topic real-time replay/ingestion of two independent match timelines.
- **Spark Structured Streaming** — the streaming technique the other tools can't provide: windowed joins across two live topics, an expected-movement model, and anomaly scoring per window.
- **Hive / HDFS** — the queryable serving and persistence layer, feeding both the dashboard and the final report's results tables.

**Stated modeling assumption:** true tick-by-tick odds aren't available from the free sources, so intra-match odds movement is modeled by interpolating between opening and closing odds, weighted toward event minutes (a goal moves the line sharply near that minute; a card moves it slightly). This is declared explicitly in the proposal and report — not a hidden gap — and is exactly the kind of design choice the *Presentation & Q&A* rubric line rewards defending.

**Datasets:**
- **Odds** — [football-data.co.uk](https://football-data.co.uk): free CSV, no signup, opening (`B365H/D/A`, `PSH/PSD/PSA`, …) and closing (`B365CH`, …) odds per match across major bookmakers, seasons back to 2000/01.
- **Events** — [API-Football](https://api-sports.io): free tier, ~100 requests/day, full event timelines (goals with exact minute, cards, subs, lineups) for completed matches. 1–2 calls per match, so the free tier comfortably covers a handful of replay matches.
- **Fallback** — Apify `football-odds-historical` actor, for closer-to-tick odds snapshots if opening/closing interpolation proves too coarse.

---

## 3. Final Output

**The working system:** an end-to-end pipeline that replays N historical matches through Kafka → Spark Streaming → Hive/HDFS, with a live dashboard showing both lines ticking and divergence flags firing in sync with the replayed match.

**Submission artifacts (per course guidelines):**
1. **Proposal** — problem statement, dataset sources, 3-tool combination, expected outcome.
2. **Progress check-in 1** — working ingestion stage (Kafka topics receiving real data).
3. **Progress check-in 2** — draft analytical results (divergence scores being produced).
4. **Code + README** — full pipeline, setup/run instructions, tools and versions used.
5. **Final report** (6–10 pages) — architecture, tool-by-tool justification, results, challenges, individual contribution breakdown.
6. **Demo** — live walkthrough or a ≤10-minute video showing the full pipeline running end to end.

---

## 4. Team Roles (3-way split, by tool)

Development so far happened collaboratively; this is the team's agreed division of ownership by tool for the remainder of the project (review, testing, and each person's share of the final report), matching the breakdown in the final report's Individual Contribution section.

- **Role A — Ingestion / Streaming Engineer — Jen Jose Jeeson**
  Kafka topic design; producer scripts for both feeds with realistic replay timing; API-Football and football-data.co.uk data pulls; interpolation logic for intra-match odds movement. Owns `producers/`.

- **Role B — Analytics / Spark Engineer — Praneetha Pillai**
  Spark Structured Streaming job; windowed join of the two topics; expected-odds-movement baseline model; divergence scoring; writes to Hive/HDFS. Owns `streaming/`.

- **Role C — Serving / Integration & Reporting Engineer — Rupsika Thipparthi**
  Hive schema and queries; live dashboard (lines + divergence flags); README; demo recording; report assembly. Owns `dashboard/` and the final report.

Roles intentionally overlap near the end — integration testing, report writing, and demo rehearsal are shared work in Phases 3–5.

---

## 5. Phase-by-Phase Schedule (2 weeks)

| Dates | Phase | Work | Deliverable |
|---|---|---|---|
| **Sep 13–14** | Phase 0 — Setup & Proposal | Finalize scope; stand up Kafka/Spark/Hive; pull sample data from both sources; lock in the interpolation modeling approach | **Proposal** |
| **Sep 15–18** | Phase 1 — Ingestion | Build dual Kafka producers (`match-events`, `odds-ticks`) replaying a chosen match's real timeline with real-time-paced timing; validate both topics receiving data | **Progress check-in 1** (~Sep 18) |
| **Sep 19–22** | Phase 2 — Streaming Analytics | Spark Structured Streaming consumer: windowed join of both topics, expected-odds-movement baseline, divergence scoring; write results to Hive/HDFS | **Progress check-in 2** (~Sep 22) |
| **Sep 23–24** | Phase 3 — Dashboard & Multi-Match Integration | Build live dashboard (both lines + flags); run the full pipeline across a handful of matches end to end; fix integration issues | Demo-ready pipeline |
| **Sep 25–26** | Phase 4 — Report & Packaging | Write the 6–10 page final report (architecture, tool justification, results, challenges, contribution breakdown); finish README; clean up code | **Final report + Code/README** |
| **Sep 27** | Phase 5 — Demo | Rehearse and deliver/record the live walkthrough (≤10 min if recorded) | **Demo** |

---

## 6. Guideline Adherence Mapping

| Guideline requirement | How Line Watch satisfies it |
|---|---|
| Minimum 3 tools, genuine data flow | Kafka → Spark Streaming → Hive/HDFS; each stage's output is literally the next stage's input, not three tools used independently |
| Team size 3 | Role A / B / C split above |
| Problem scope substantial enough | Requires a live windowed join across two independent time series plus real-time anomaly scoring — batch alone can't do this, which is the guideline's explicit bar |
| Proposal & Planning (2 pts) | Phase 0 deliverable: problem statement, datasets, tool combo, expected outcome |
| Implementation & Integration (7 pts) | Phases 1–3: real Kafka ingestion, real Spark windowed processing, real Hive/HDFS persistence, genuinely chained |
| Analytical Quality (3 pts) | The divergence-scoring model itself — validity of "expected vs. actual" odds movement, not just that a Spark job ran |
| Demo (3 pts) | Phase 5: live pipeline run end to end across multiple matches |
| Documentation & Report (3 pts) | Phase 4: architecture diagram (Section 2 above), tool-by-tool justification, reproducible setup instructions |
| Presentation & Q&A (2 pts) | The interpolation assumption is stated and defensible by design — exactly what this rubric line rewards |

**Extra credit (optional, not required for the 2-week core scope):** publication (+3), major open-source contribution (+2), minor open-source contribution (+1). Not planned for in this timeline — flag as a stretch goal only if the core deliverables land early.

---

## 7. Risks / Challenges to Flag in the Report

- **Kafka setup overhead** — the course guidelines explicitly warn to budget extra time here; Phase 0 is scheduled first for exactly this reason and has zero slack in a 2-week window.
- **API-Football's 100 req/day limit** — plan the match list in advance so pulls stay well under it (1–2 calls/match).
- **The interpolation assumption is a modeling limitation, not a bug** — must be stated and defended explicitly in the report and demo, not glossed over.
- **Compressed timeline** — 2 weeks is tight against the guideline's implied pacing (proposal + 2 check-ins + report + demo). Phase 0 (environment setup) is the single highest-risk item; if Kafka/Spark/Hive aren't reliably running by Sep 14, every later phase slips.
