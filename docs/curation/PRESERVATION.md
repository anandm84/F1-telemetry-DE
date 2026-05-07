# Archiving and Preservation Plan

## Goals

This plan describes how the curated F1 telemetry dataset will be archived for long-term access and integrity. The goals, in priority order, are:

1. **Reproducibility over time.** A reviewer two years from now should be able to obtain the same artifacts, including the documentation, even if FastF1 backends or Jolpica-F1 change.
2. **Discoverability.** The deposit must be findable by DOI, indexed in major aggregators, and cited correctly when reused.
3. **License clarity.** The split-license model (MIT for code, CC-BY-4.0 for derived data, no redistribution of raw timing data) must be unambiguous to anyone landing on the deposit page.
4. **Cost stability.** Free, institutional-grade archiving is preferred over self-hosting.

## Repository selection

| Repository | Pros | Cons | Decision |
|---|---|---|---|
| **Zenodo** | Free, persistent DOI, GitHub-integration for one-click releases, EU-hosted with CERN-backed long-term commitment, supports versioning, accepts code+data bundles up to 50 GB. Aligned with FAIR (Wilkinson et al., 2016). | Generalist (no F1-specific community); discovery from outside Zenodo depends on indexing. | **Selected.** |
| IDEALS (UIUC institutional) | Institutional context matches the university-research-lab framing of this project; long-term institutional commitment. | Stricter ingest review; less integration with external tools; smaller global discovery surface. | Considered; a secondary deposit may be made if time permits. |
| Figshare | Similar to Zenodo; widely used. | Commercial backing (Digital Science) rather than non-profit/CERN. | Rejected for this submission. |
| OSF (Open Science Framework) | Strong for active research project workflows; integrates with versioning tools. | Better suited for ongoing projects than archival snapshots; DOI flow is less direct than Zenodo. | Rejected. |
| Self-hosted (GitHub release only) | Easiest. | No DOI, no preservation guarantees, GitHub itself is not an archival institution. | Rejected. |

**Primary archive: Zenodo. Secondary (optional): IDEALS as institutional deposit.**

## What is deposited

A single Zenodo record will hold:

| Item | Form | Approximate size |
|---|---|---|
| Source code (this repository at the submission tag) | Zip + linked GitHub release | < 5 MB |
| `docs/curation/` (all documentation in this folder) | Included in the source zip | < 1 MB |
| `docs/curation/sample_output/` bundle | Parquet + CSV + small NDJSON excerpt | ~5-20 MB |
| `REPORT.pdf` | PDF, exported from `REPORT.md` | < 2 MB |
| `prov.jsonld` | JSON-LD, machine-readable provenance | < 100 KB |
| `metadata.xml` and `metadata.json` | Dublin Core | < 50 KB |
| `checksums.sha256` | SHA-256 over every file in the deposit | < 10 KB |

**Not deposited:** the full bronze NDJSON for non-trivial coverage, for the licensing reasons in [LICENSING.md](LICENSING.md). A user who wants the full bronze runs `ingestion/backfill.py` themselves.

**Not deposited (intentional):** the DuckDB warehouse file `data/warehouse.duckdb`. DuckDB is a stable file format but locking the deposit to one specific database engine reduces long-term accessibility. Reviewers reproduce the warehouse from the deposited code and the live source; the *outputs* of analytical queries (Parquet + CSV) are what gets archived.

## Format choices and rationale

| Layer | Primary format | Why this format |
|---|---|---|
| Bronze (sample only) | NDJSON | Plain text, line-oriented, language-agnostic. Trivially readable in any language. The sample is small enough that compression is unnecessary. |
| Silver (working store) | DuckDB tables in `data/warehouse.duckdb` | Engine-managed, fast analytical queries, single-file portable. Not deposited (engine-locked); reviewers regenerate from code + bronze. |
| Silver (sample bundle export) | Apache Parquet | Self-describing schema, columnar (efficient for analytics), broad ecosystem support, non-proprietary, ASF-governed. Listed in the Library of Congress Sustainability of Digital Formats guidance. |
| Silver (CSV derivatives in sample bundle) | UTF-8 CSV | Last-resort accessibility if Parquet tooling is unavailable. Loses type fidelity; documented in the data dictionary. |
| Gold (working store) | DuckDB tables in `data/warehouse.duckdb` | Same as silver. |
| Gold (sample bundle export) | Apache Parquet + CSV | Reviewer-friendly; CSV readable in Excel/LibreOffice without specialized tooling. |
| Documentation | Markdown + PDF | Markdown is the source of truth (plain text, version-controlled). PDF is the rendered narrative for the final report. |
| Metadata | Dublin Core in XML + JSON | Standardized, widely supported, machine-readable. |
| Provenance | W3C PROV-O JSON-LD | Standardized, machine-readable, FAIR-aligned. |

The Parquet + CSV pair satisfies the Library of Congress's recommendation for both *primary* (rich, efficient) and *secondary* (broadly accessible) preservation formats.

## Integrity and fixity

A SHA-256 manifest (`checksums.sha256`) is generated by [`scripts/checksums.py`](../../scripts/checksums.py) and included in every deposit. It covers every file in the bundle, including the manifest's siblings. A reviewer can verify integrity with:

```bash
sha256sum --check checksums.sha256
```

Zenodo additionally records an MD5 per file in its own metadata; the SHA-256 in our manifest is the project's own fixity signal and survives any future migration off Zenodo.

## Versioning strategy

- Each substantive update gets a new Zenodo version (Zenodo handles parent + child DOIs automatically).
- Versions correspond to git tags in the source repo. The submission for IS547 final is tagged `v1.0.0-is547-final`.
- Backwards-incompatible schema changes (e.g., renaming a silver column) increment the *major* version.
- Adding new races or new gold tables increments the *minor* version.
- Pure documentation or metadata fixes increment the *patch* version.

## Long-term access risks and mitigations

| Risk | Mitigation |
|---|---|
| Zenodo policy changes / acquisition | Secondary IDEALS deposit; SHA-256 manifest survives migration; data dictionary describes formats so artifacts remain interpretable without Zenodo's metadata. |
| Parquet tooling deprecation | CSV derivatives included for every gold table in the sample bundle. |
| FastF1 / Jolpica backends change | The deposit ships *outputs and code*, not a live dependency. Re-running may fail in the future, but consuming the deposit will not. |
| Source-of-truth drift on GitHub | The deposit is an immutable snapshot at a tag. The source repo can move or disappear; the deposit remains. |
| F1 timing licensing escalation | Because raw timing data is *not* redistributed, a future enforcement action would not affect deposited derivatives. |
| Author unavailability | Documentation is verbose enough that another data steward can interpret and extend the dataset without contacting the author. |

## Succession plan

If the author becomes unavailable:

1. The Zenodo record remains accessible and citable indefinitely.
2. The CITATION.cff at the repo root names a contact path through the IS547 / UIUC iSchool advising channel.
3. The data dictionary, provenance log, and cleaning log together form a complete handover packet.

## Pre-deposit checklist

Before the Zenodo deposit, confirm:

- [ ] All files in `docs/curation/` are committed.
- [ ] `scripts/build_sample_bundle.py` has been run successfully and `docs/curation/sample_output/` contains the expected files.
- [ ] `scripts/checksums.py` has been run and `checksums.sha256` is current.
- [ ] `scripts/cleaning_log.py` has been run and `CLEANING_LOG.md` has its automated row-count section populated.
- [ ] `REPORT.pdf` is current and matches `REPORT.md`.
- [ ] `requirements.txt` is pinned (no `>=`).
- [ ] `metadata.xml` / `metadata.json` carry the final retrieval/issued dates.
- [ ] No `.env`, credentials, or large `cache/` artifacts are included.
- [ ] LICENSE and LICENSE-DATA.md are at the repo root.

## Optional: Zenodo Sandbox dry-run

For class submission, a Zenodo Sandbox deposit (https://sandbox.zenodo.org) provides a free DOI on a non-production server. The DOI looks identical and lets reviewers see the deposit page without committing to a permanent record. This is recommended for the final submission so the report can reference an actual deposit URL. Sandbox records expire on a rolling basis, so the *production* Zenodo deposit remains the canonical archive.

## References

- Library of Congress. (n.d.). *Sustainability of Digital Formats, Apache Parquet*. <https://www.loc.gov/preservation/digital/formats/>
- Wilkinson, M. D., Dumontier, M., Aalbersberg, I. J., et al. (2016). The FAIR Guiding Principles for scientific data management and stewardship. *Scientific Data*, 3, 160018. <https://doi.org/10.1038/sdata.2016.18>
- Zenodo. (n.d.). *Zenodo policies*. <https://about.zenodo.org/policies/>
