# Data and Documentation License

The curated derived data (Silver and Gold Parquet tables, the sample bundle in
`docs/curation/sample_output/`, and CSV derivatives) and the project
documentation (everything in `docs/curation/`, plus this repository's
`README.md`) are released under the **Creative Commons Attribution 4.0
International License (CC-BY-4.0)**.

License text: <https://creativecommons.org/licenses/by/4.0/legalcode>
Summary: <https://creativecommons.org/licenses/by/4.0/>

## What CC-BY-4.0 means here

You are free to:

- **Share** — copy and redistribute the material in any medium or format.
- **Adapt** — remix, transform, and build upon the material for any purpose, even commercially.

Under the following terms:

- **Attribution** — You must give appropriate credit, provide a link to the license, and indicate if changes were made.
- **No additional restrictions** — You may not apply legal terms or technological measures that legally restrict others from doing anything the license permits.

## Required attribution boilerplate

Any reuse must include the following note (or close paraphrase):

> Data ultimately derived from Formula 1 timing services
> (© Formula One World Championship Ltd.), accessed via FastF1
> (Oehrly, theoehrly/Fast-F1, MIT) and Jolpica-F1. Curated and
> transformed by Anand Marepalli (IS547, Spring 2026). Raw timing
> data is not redistributed; aggregated and derived statistics are
> released under CC-BY-4.0.

A `CITATION.cff` file in the repository root provides a structured citation.

## What is NOT covered

- **Raw F1 timing data** is not redistributed by this project. The underlying timing data accessed through FastF1 remains the property of Formula One World Championship Ltd. and is governed by the [Formula 1 Brand Guidelines](https://www.formula1.com/en/information/guidelines.4EOKE9RRqevL4niTK9kWyt). The `CC-BY-4.0` license here applies to *transformations and aggregations* of timing data, not to a license over the timing data itself.
- **Source code** is licensed separately under the MIT License (see `LICENSE`).
- **Third-party logos, team identities, and trademarks** referenced incidentally in the dataset (driver/team names) remain the property of their respective owners.

## Detailed reasoning

See [`docs/curation/LICENSING.md`](docs/curation/LICENSING.md) for the
full responsible-use memo, including the redistribution decision matrix and
research-lab risk discussion.
