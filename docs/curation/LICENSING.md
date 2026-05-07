# Licensing, Terms of Use, and Responsible Curation

This memo explains my legal and ethical posture for the curated F1 telemetry dataset. It is a direct response to the instructor's review feedback and to the open question raised in [Fast-F1 Discussion #634](https://github.com/theOehrly/Fast-F1/discussions/634#discussioncomment-10720812) about whether F1 timing data scraped through FastF1 can be redistributed.

The short answer is: **no, raw timing data cannot be redistributed**. I ship derived and aggregated outputs and reproducible extraction code, not raw F1 API responses. My reasoning is below.

---

## 1. Upstream sources and their terms

| Source | Role in this pipeline | Stated terms |
|---|---|---|
| **Formula 1 live timing service** | Underlying source of lap times, sector splits, weather, results. Accessed indirectly via FastF1. | Governed by the [Formula 1 Brand Guidelines](https://www.formula1.com/en/information/guidelines.4EOKE9RRqevL4niTK9kWyt). The "Timing Data" section restricts use of timing-derived information; it is not licensed for republication or commercial reuse without permission from Formula One Management. |
| **FastF1** ([theoehrly/Fast-F1](https://github.com/theOehrly/Fast-F1)) | Python client used to retrieve session data. | Licensed MIT (the *library code*). The library author has been clear (see the linked discussion) that the **data accessed through it is not licensed by the F1 timing service**. FastF1 only provides programmatic access; it does not re-license the underlying data. |
| **Jolpica-F1** ([jolpica/jolpica-f1](https://github.com/jolpica/jolpica-f1)) | Successor to the deprecated Ergast API. Used for season schedule and historical reference data. | Free for non-commercial use; attribution requested. Project README and docs at <https://docs.fastf1.dev/api_reference/jolpica.html> describe the interface. |
| **Pandas / dbt-duckdb / DuckDB / Kafka / Airflow** | Tooling. No data hosted here. | Each under permissive open-source licenses (BSD-3, Apache-2.0, MIT). |

### Re-reading the F1 Timing Data guidelines

The Formula1.com guidelines page says timing data and timing-derived statistics are the property of Formula One Management and that "any unauthorised use," including republication, redistribution, or commercial exploitation, is prohibited. The guidelines are not a permissive license. They are a notice that any use beyond personal viewing requires explicit permission.

FastF1 itself is a reverse-engineered client. The library author has acknowledged in [Discussion #634](https://github.com/theOehrly/Fast-F1/discussions/634#discussioncomment-10720812) that this places the *data*, not the library, in a legally ambiguous space. That ambiguity is the central compliance fact for this project.

---

## 2. What is actually being shipped, and why

I distinguish between five categories:

| Category | Ship in the curated bundle? | Why |
|---|---|---|
| **Raw bronze NDJSON** (full coverage) | **No.** | This is essentially a re-encoded copy of FOM's timing data. Republishing it is the exact behavior the F1 guidelines prohibit. |
| **Bronze NDJSON sample** (single race, ≤200 lines per file, included for transparency only) | Yes, **with attribution**. | A small excerpt is necessary so a reviewer can see what the raw layer looks like. It is not a usable redistribution of timing data. |
| **Derived Silver/Gold tables** (deduplicated, typed, aggregated) | Yes, **with attribution**. | Aggregated and transformed statistics are several steps removed from the underlying timing feed. This is consistent with how academic motorsport publications cite the timing source without redistributing it. |
| **Extraction & transformation code** | Yes, MIT-licensed. | Code is my own work. |
| **Dimensions** (`dim_drivers`, `dim_circuits`, `dim_races`) | Yes, **with Jolpica attribution**. | Schedule and reference data is sourced from Jolpica-F1, not the F1 timing service. |

My decision rule is: **derived outputs and reproducible code go out; raw timing redistribution stays in.** A user who wants the full raw bronze runs the pipeline themselves and assumes their own compliance posture by doing so.

---

## 3. Decision matrix (what users may do with the curated bundle)

| Action | Permitted | Conditions |
|---|---|---|
| Use Silver/Gold tables for academic / non-commercial research | Yes | Cite this dataset (see `CITATION.cff`) and Formula One Management as the underlying data source. Cite FastF1 and Jolpica-F1 as access tools. |
| Include the curated tables in a paper or thesis | Yes | Same attribution. Disclose any further transformations. |
| Re-distribute the curated tables verbatim | Yes, under CC-BY-4.0 | Preserve attribution, do not represent it as official F1 data, do not strip lineage. |
| Re-distribute as raw F1 timing data, claim it as official, or sell it | **No** | This violates the F1 Brand Guidelines and likely the implicit terms of FOM's timing service. |
| Use for commercial products (betting, dashboards-as-a-service, and similar) | **Not from this dataset alone** | Commercial use of F1 timing data requires a license from Formula One Management. The MIT license on the code does not grant rights to the data. |

---

## 4. Required attribution boilerplate

Any reuse of the curated dataset must include the following note (or a close paraphrase) in any publication, dashboard, or downstream redistribution:

> Data ultimately derived from Formula 1 timing services (© Formula One World Championship Ltd.), accessed via FastF1 (Oehrly, theoehrly/Fast-F1, MIT) and Jolpica-F1. Curated and transformed by Anand Marepalli (IS547, Spring 2026). Raw timing data is not redistributed; aggregated and derived statistics are released under CC-BY-4.0.

This text is also embedded in `docs/curation/metadata.json` (`rights` field) and in the `LICENSE-DATA.md` file at the repository root.

---

## 5. Research-lab risk discussion

The instructor asked specifically: *what are the risks if you are part of a research lab working with this dataset?*

The risks I see are:

1. **Takedown or cease-and-desist.** A research lab that publishes raw F1 timing dumps online, even for "academic" purposes, can receive a DMCA-style takedown or a direct legal request from Formula One Management. The F1 Brand Guidelines do not carve out academic use. **Mitigation:** ship derived data and code, not raw timing. Keep raw bronze inside the lab's internal storage.
2. **Reputational risk to the lab and the institution.** Universities are usually careful about hosting third-party content of contested provenance, especially from a globally visible commercial rights-holder. **Mitigation:** route the data through institutional repository review (for example, IDEALS at UIUC) before public deposit, and document the compliance reasoning (this memo).
3. **Funding and research-compliance friction.** If the lab seeks NSF or industry funding, reviewers may flag commercial-data dependence. **Mitigation:** declare the source dependency openly. Show that the workflow is reproducible against the live source so the published artifacts are not a redistribution.
4. **Backend changes invalidate prior analyses.** FastF1 is reverse-engineered, and the F1 timing backends and Jolpica change without notice. A reproducibility claim made today may not hold in 12 months. **Mitigation:** version-pin FastF1 in `requirements.txt`, record retrieval dates and library versions in the provenance log, retain the local FastF1 cache when feasible.
5. **Re-identification is not a concern.** No personal data beyond publicly known driver names and team affiliations. This dataset does not raise GDPR, FERPA, or HIPAA issues.
6. **Derivative-work scope creep.** A graduate student using this dataset might copy the bronze layer into a new pipeline, blurring what is "raw" versus "derived." **Mitigation:** clear directory naming, the cleaning log, and this memo establish what counts as derived. New uses should be reviewed against this framing.

---

## 6. Project-applied license summary

| Asset class | License | File |
|---|---|---|
| Source code (Python ingestion, dbt project, Airflow DAGs, helper scripts) | MIT | `LICENSE` |
| Curated derived data (Silver/Gold Parquet, sample bundle) | Creative Commons BY 4.0, with the attribution requirements above | `LICENSE-DATA.md` |
| Documentation (this file, REPORT.md, data dictionary, and so on) | Creative Commons BY 4.0 | `LICENSE-DATA.md` |
| Raw bronze (full coverage) | **Not redistributed.** Recreatable via `ingestion/backfill.py` against the live source. | n/a |

This split-license model is consistent with the FAIR data principles' requirement that licenses be both clear and applied at appropriate granularity (Wilkinson et al., 2016).

---

## 7. References

- Formula One Management. (n.d.). *Brand guidelines and use of Formula 1 information*. <https://www.formula1.com/en/information/guidelines.4EOKE9RRqevL4niTK9kWyt>
- Oehrly, T. (n.d.). *FastF1*. <https://docs.fastf1.dev/>
- Oehrly, T. (n.d.). *Discussion #634: Permission to use F1 timing data*. <https://github.com/theOehrly/Fast-F1/discussions/634#discussioncomment-10720812>
- Jolpica-F1. (n.d.). *Jolpica-F1 API*. <https://github.com/jolpica/jolpica-f1>
- Wilkinson, M. D., Dumontier, M., Aalbersberg, I. J., et al. (2016). The FAIR Guiding Principles for scientific data management and stewardship. *Scientific Data*, 3, 160018. <https://doi.org/10.1038/sdata.2016.18>
