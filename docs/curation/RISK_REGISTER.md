# Risk Register

A risk register is the data steward's working list of "things that could go wrong" with the dataset over its lifecycle. Each entry records the risk, the likelihood and impact, the trigger conditions, the mitigations already in place, and the residual risk after mitigation. This register directly responds to the instructor's prompt about lab/research-context risk for this F1 dataset.

Likelihood / Impact rubric:
- **L** = Low (unlikely / rare)
- **M** = Medium (plausible / occasional)
- **H** = High (likely / common)

## 1. Legal and licensing risks

| ID | Risk | L | I | Trigger | Mitigation | Residual |
|---|---|---|---|---|---|---|
| L-1 | Formula One Management issues a takedown over redistributed timing data | M | H | Publishing raw bronze NDJSON publicly; commercial reuse | Ship derived/aggregated data only (`LICENSING.md`); attribution boilerplate; CC-BY-4.0 with explicit non-commercial-of-raw notice | L |
| L-2 | A downstream user re-publishes the curated dataset as if it were primary timing data | M | M | Imprecise attribution by a derivative work | Mandatory attribution clause; clear statement in `LICENSE-DATA.md`; provenance log shows the derivation chain | L |
| L-3 | FastF1 itself is taken down or relicensed | L | H | Upstream rights-holder action against FastF1 | Pin FastF1 version; preserve local FastF1 cache; fallback path documented (re-run code against any future API client) | M |
| L-4 | Jolpica-F1 deprecates without succession | L | M | Same fate as Ergast | Schedule data is currently low-volatility; circuits and races dim can be re-derived from any historical schedule source | L |
| L-5 | Institutional review (UIUC) flags the dataset for hosting concerns | L | M | IDEALS deposit review | Zenodo is primary archive; IDEALS is secondary; LICENSING.md addresses every concern preemptively | L |

## 2. Technical risks

| ID | Risk | L | I | Trigger | Mitigation | Residual |
|---|---|---|---|---|---|---|
| T-1 | FastF1 backend schema changes (e.g., `Result.Position` column renamed) | M | H | Upstream silently changes a field name | Defensive `getattr(row, "...", None)` in producer/backfill; pinned version in `requirements.txt`; dbt `not_null` and `relationships` tests would catch most schema breaks via failing build | M |
| T-2 | dbt build silently produces empty silver/gold | L | M | A backfill run produces 0 records | dbt `not_null` and `unique` tests fail the build immediately; backfill manifest records error status | L |
| T-3 | DuckDB OOM on `read_json` with `union_by_name=true` over 1000+ files | **OBSERVED** | H | Full-season build with the original `bronze_read` macro | **Resolved** by removing `union_by_name=true` from `dbt/macros/bronze_read.sql` (commented inline) and setting `maximum_object_size=64MB`. Bronze writers emit a stable schema per data type, so `union_by_name` is unnecessary in practice. Documented in `CLEANING_LOG.md` and `PROVENANCE.md`. | L |
| T-4 | Bronze append-only design causes duplication on rerun | M | L | `BACKFILL_FORCE=true` rerun without manifest reset | dbt `qualify row_number() over (partition by record_id order by event_timestamp desc) = 1` absorbs duplicates (latest wins); storage waste only | L |
| T-5 | Parser drift in `duration_to_ms` if a future bronze format is introduced | L | M | Producer changes `_format_timedelta_hhmmssmmmm` shape | Macro at `dbt/macros/duration_to_ms.sql` is regex-based; future formats can add new branches | L |
| T-6 | Cache poisoning if a partial FastF1 download is treated as authoritative | L | M | Network interruption mid-download | FastF1's own cache validation; `record_id` collisions on rerun would be absorbed by silver dedup | L |
| T-7 | FastF1 500-call/hour rate limit interrupts full-season backfill | **OBSERVED** | M | Aggressive backfill (sleep < 8s) | Resumable manifest; rerun skips already-completed sessions; default `BACKFILL_SLEEP_SECONDS=8` keeps under the limit | L |
| T-8 | Schedule seeds get out of sync with FastF1 schedule | L | L | Schedule changes without re-running `load_schedules.py` | `dim_circuits` / `dim_races` filter to seasons present in silver, so stale seeds for unobserved years are invisible. Re-run is a one-liner. | L |

## 3. Reproducibility risks

| ID | Risk | L | I | Trigger | Mitigation | Residual |
|---|---|---|---|---|---|---|
| R-1 | A reviewer cannot reproduce the build a year from now | M | H | FastF1 backend changes; Python 3.10 deprecation; unpinned transitive deps | `REPRODUCE.md` covers steps; `requirements.txt` is pinned; deposit ships *outputs* so consumption is decoupled from rebuild | M |
| R-2 | Wall-clock columns (`_updated_at`) cause spurious "differences" in dim Parquet | H | L | Re-running gold dimensions | Documented in `PROVENANCE.md` §7 as expected non-determinism | L |
| R-3 | DuckDB / dbt-duckdb version drift produces different warehouse internals | M | L | New DuckDB or dbt-duckdb major version | Logical schema and SQL queries are stable; sample bundle ships Parquet+CSV which are version-independent | L |
| R-4 | Python version drift breaks `from datetime import timezone` style imports | L | L | Python 4 someday | Listed Python 3.10+ baseline in `REPRODUCE.md` | L |

## 4. Data-quality risks

| ID | Risk | L | I | Trigger | Mitigation | Residual |
|---|---|---|---|---|---|---|
| Q-1 | Missing telemetry / aborted laps inflate or deflate `avg_lap_time_ms` | H | M | Always present; this is a feature of F1 timing | Documented in data dictionary's interpretive notes and `CLEANING_LOG.md`. Analysts who want pure pace should filter `main_silver.stg_laps` directly. | M |
| Q-2 | Reserve drivers create `dim_drivers` rows with "stale" team affiliations | M | L | A driver appears in one session as a reserve | Type-1 SCD by design; advised in dim_drivers note that for mid-season analysis, join `main_silver.stg_race_results` directly | L |
| Q-3 | `position` is null for unclassified drivers and could be misread as 0 | M | M | Analyst defaults nulls to 0 | The `status` column carries the DNF/DNS reason; documented in `CLEANING_LOG.md`. Future work: add a dbt `accepted_values` test for `position ∈ [1, 24]` when not null. | L |
| Q-4 | Sprint sessions counted alongside main race in aggregates | L | M | Analyst forgets `session = 'R'` filter | Aggregates are *per session*, so sprint and main race appear as separate rows; documented in data dictionary | L |
| Q-5 | `pit_out_ms` null when stop straddles a lap boundary | M | L | Common in long pit cycles | Documented in `CLEANING_LOG.md` (S-PS-1); analysts can join the next lap's `PitOutTime_ms` for completeness | L |

## 5. Research-lab risks (instructor's question)

This section answers the instructor's specific prompt: *"if you are part of a research lab working with this dataset, what are the risks?"*

| ID | Risk | L | I | Mitigation in this project |
|---|---|---|---|---|
| RL-1 | Lab publishes a paper using the dataset; reviewer asks about data licensing | H | M | `LICENSING.md` is publication-ready; CITATION.cff is provided; attribution boilerplate is one paragraph |
| RL-2 | Lab puts raw bronze on a public S3 bucket "for collaborators"; gets a takedown | M | H | Documented red-line: raw bronze is **not** deposited, not even in the curated bundle (only truncated excerpts). Collaborators run the pipeline themselves. |
| RL-3 | Lab's findings depend on a transient FastF1 backend behavior; results don't reproduce a year later | M | M | Pinned dependencies; FastF1 cache retention recommended; documented in `RISK_REGISTER` (T-1) |
| RL-4 | Funding application reviewer flags F1 as a commercial-data dependency | M | L | The dataset depends on a commercial source but does not redistribute it; this is a normal posture for academic motorsport analytics. Disclose openly. |
| RL-5 | Grad student copies the bronze NDJSON into a new lab repo and inadvertently re-publishes it | M | M | Documentation is verbose; cleaning log makes the bronze/derived distinction explicit; new uses should be reviewed against `LICENSING.md` |
| RL-6 | Industry collaborator wants to use the dataset in a commercial product | L | H | Refuse or require the collaborator to obtain a license from FOM directly. The MIT license on the *code* does not extend to the *data*. Documented in `LICENSING.md` §3. |

## 6. Top-3 priorities

If only three things from this register can be addressed before final submission, they are:

1. **L-1 / RL-2**, confirm that the deposited bundle does not include the full raw bronze. Verified by `scripts/build_sample_bundle.py` truncating bronze NDJSON to 200 lines per file.
2. **R-1**, confirm `REPRODUCE.md` works against a fresh venv. Manually run through it before depositing.
3. **T-1**, pin `fastf1==<exact>` in `requirements.txt` (currently unpinned). One-line fix; high reproducibility return.

Lower-priority items remain valuable to track but are acceptable as residual risks.
