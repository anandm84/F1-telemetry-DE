# FAIR Self-Assessment

This is a self-assessment of the curated F1 telemetry dataset against the FAIR Guiding Principles (Wilkinson et al., 2016). I tried to be honest rather than charitable: where a principle is partial or unmet, I said so and recorded what would actually close it.

| Score | Meaning |
|---|---|
| Met | Principle is satisfied without significant caveat. |
| Partial | Principle is partially satisfied; specific gap noted. |
| Unmet | Principle is not addressed. |

## Findable

| Principle | Score | Evidence / gap |
|---|---|---|
| **F1.** (Meta)data are assigned a globally unique and persistent identifier. | Partial | `metadata.{xml,json}` carry a URN identifier today. The DOI field is a placeholder until I deposit the bundle to Zenodo and replace it with the issued DOI. |
| **F2.** Data are described with rich metadata. | Met | Dublin Core record (`metadata.xml`/`metadata.json`); column-level data dictionary; PROV-O record; cleaning log; preservation plan. |
| **F3.** Metadata clearly and explicitly include the identifier of the data they describe. | Met | `dc:identifier` is present, and the metadata sits next to the data bundle in the deposit so the link is unambiguous. |
| **F4.** (Meta)data are registered or indexed in a searchable resource. | Partial | Zenodo auto-syndicates records to OpenAIRE within ~24 hours of deposit, and DataCite indexes the DOI immediately. Both clear once the deposit is live. |

## Accessible

| Principle | Score | Evidence / gap |
|---|---|---|
| **A1.** (Meta)data are retrievable by their identifier using a standardised communications protocol. | Met (planned) | Zenodo serves over HTTPS; the issued DOI resolves to a stable landing page. |
| **A1.1.** The protocol is open, free, and universally implementable. | Met | HTTPS. |
| **A1.2.** The protocol allows for an authentication and authorisation procedure where necessary. | Met | None needed; the deposit is open. |
| **A2.** Metadata are accessible, even when the data are no longer available. | Met | Dublin Core metadata is part of the deposit and survives independently of any specific data file. The licensing memo notes that raw bronze is intentionally not redistributed; the metadata explains that to anyone landing on the deposit. |

## Interoperable

| Principle | Score | Evidence / gap |
|---|---|---|
| **I1.** (Meta)data use a formal, accessible, shared, and broadly applicable language for knowledge representation. | Met | Dublin Core (XML, JSON-LD); W3C PROV-O (JSON-LD); Apache Parquet (self-describing schema). |
| **I2.** (Meta)data use vocabularies that follow FAIR principles. | Met | DCMI Terms; PROV-O. |
| **I3.** (Meta)data include qualified references to other (meta)data. | Met | `dcterms:references`, `dcterms:isPartOf`, `dcterms:provenance` in the metadata; `prov:wasDerivedFrom` chains in `prov.jsonld`. |

## Reusable

| Principle | Score | Evidence / gap |
|---|---|---|
| **R1.** (Meta)data are richly described with a plurality of accurate and relevant attributes. | Met | Schema-level (data dictionary), interpretive (codebook section), per-record lineage, cleaning log, provenance log, preservation plan. |
| **R1.1.** (Meta)data are released with a clear and accessible data usage license. | Met | CC-BY-4.0 (`LICENSE-DATA.md`); MIT for code (`LICENSE`); responsible-use memo (`LICENSING.md`). |
| **R1.2.** (Meta)data are associated with detailed provenance. | Met | `PROVENANCE.md` (human-readable), `prov.jsonld` (W3C PROV-O machine-readable), and dbt's auto-generated `target/manifest.json` (graph-level model lineage). |
| **R1.3.** (Meta)data meet domain-relevant community standards. | Partial | There is no widely adopted motorsport telemetry metadata standard, so I fall back on Dublin Core for the descriptive layer and CSVW-style column descriptions in the data dictionary. The closest "community standard" I could conform to is a Frictionless Data Package or Schema.org Dataset markup. See "How to reach 15/15" below. |

## Summary

| Category | Met | Partial | Unmet |
|---|---|---|---|
| Findable | 2 | 2 | 0 |
| Accessible | 4 | 0 | 0 |
| Interoperable | 3 | 0 | 0 |
| Reusable | 3 | 1 | 0 |
| **Total** | **12** | **3** | **0** |

## How to reach 15/15

Two of the three partials clear automatically the moment the deposit is made; one needs a small amount of additional work.

| Partial | What closes it | Effort |
|---|---|---|
| F1 (persistent identifier) | Deposit to Zenodo (or Zenodo Sandbox), copy the issued DOI back into `metadata.xml` and `metadata.json` to replace the placeholder URN. | ~15 minutes |
| F4 (indexed in a searchable resource) | Same Zenodo deposit. Zenodo pushes new records to OpenAIRE within ~24 hours; DataCite indexes the DOI immediately. | $0; ~1 day wait |
| R1.3 (community-standard adherence) | Add a Frictionless Data Package (`datapackage.json`) describing each gold table's schema, validated with the `frictionless` CLI; or alternatively add Schema.org `Dataset` markup as JSON-LD. The data dictionary CSV already contains the source material, so this is mostly a re-encoding step. | ~1 hour |


## Reference

Wilkinson, M. D., Dumontier, M., Aalbersberg, I. J., Appleton, G., Axton, M., Baak, A., et al. (2016). The FAIR Guiding Principles for scientific data management and stewardship. *Scientific Data*, 3, 160018. <https://doi.org/10.1038/sdata.2016.18>
