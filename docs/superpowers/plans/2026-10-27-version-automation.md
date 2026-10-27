# gluecat version automation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Discovery keeps Spark, Delta, Scala and hadoop-aws versions current in module POMs and `matrix.json` through auto-merging PRs; Renovate keeps the rest current; README/docs artifact tables are generated.

**Architecture:** stdlib Python scripts under `scripts/` (pure functions + thin Maven Central I/O), unit-tested with `python -m unittest`; a `discover.yml` workflow modelled on sdaberdaku/spark-images; a `renovate.json`; a check-only CI job for generated tables and the script tests.

**Tech Stack:** Python 3.12 stdlib, GitHub Actions, Renovate (Mend app), Maven.

**Spec:** `docs/superpowers/specs/2026-10-27-version-automation-design.md`

## Global Constraints

- Discovery and the table renderer are stdlib-only Python 3.12; network I/O isolated in one module function each; everything else pure and unit-tested offline.
- `matrix.json` layout byte-stable: `[` line, one row object per line with keys in the existing order (`module`, `spark`, `delta`, optional `deltaArtifact`, `jdk`, `smoke`), `]` line, trailing newline; JSON spacing as today (`{"module": "...", ...}`).
- POM edits are single-line text replacements of `<prop>value</prop>`; never re-serialise XML.
- `main` is protected (PR, `ci-ok` strict, linear history); squash merges; auto-merge allowed; workflow token stays read-only by default.
- No AI-tool attribution anywhere. Plain prose in docs and PR bodies. Commit messages `type(scope): subject`.
- Latest action majors: `actions/checkout@v7`, `actions/setup-python@v7`, `actions/setup-java@v6`.

## Review Focus

1. A discovery run must never change the Spark version of a pinned row (3.5.3 rows, the 4.1.1 rows); Delta patches still move their Delta. Test in Task 2.
2. A new Spark line without a matching `delta-spark_<minor>_2.13` artifact adds nothing. Test in Task 2.
3. A Delta minor that only exists for some Spark lines adds rows only to those modules. Test in Task 2.
4. Writer output for the current files is byte-identical when there is nothing to change. Test in Task 2.
5. The generated tables reproduce today's information (including the "(on Spark 4.1.1)" note). Test in Task 1.

---

### Task 1: Generated artifact tables and the CI check

**Files:** Create `scripts/render_tables.py`, `tests/scripts/test_render_tables.py`; Modify `README.md`, `docs/site/index.md` (wrap the existing Artifacts tables in `<!-- artifacts:start -->`/`<!-- artifacts:end -->` and replace them with the rendered output), `.github/workflows/ci.yml` (job `scripts`: setup-python 3.12, `python -m unittest discover -s tests/scripts -v`, `python scripts/render_tables.py --check`; `ci-ok` needs it).

**Interfaces:**
- `read_modules(root) -> list[dict]`: parse each `modules/*/pom.xml` property lines (`spark.version`, `spark.minor`, `delta.version`, `scala.binary.version`, `java.release`) with a regex; module order = parent POM `<module>` order.
- `table(modules, rows) -> str`: markdown table, one line per module: artifact `gluecat-spark-<minor>_<scala>`, Spark (`<minor>.x`, or `<min> and later <minor>` when the lowest row Spark is not `.0`/ not the module's... use: if any row has a Spark patch lower than the module's, show "`<lowest>` and later `<minor>`", else "`<minor>.x`"), Scala, bytecode (`java.release`), JDKs (sorted distinct row JDKs), Delta (distinct row Delta versions in version order; versions only present on pinned Spark patches get " (on Spark x.y.z)" grouped like today: "4.1.0 and 4.2.0 (on Spark 4.1.1), 4.3.1, 4.4.1").
- `main()`: `--check` exits 1 when either file differs; without it rewrites both files.
- The rendered tables must equal today's hand-written tables (compare and adapt the renderer, not the data; if today's table has an inconsistency with matrix.json, report it).

- [ ] Tests first (fake modules + rows covering the 3.5 "and later" case and the 4.1 pinned-Delta note), then the script, then wrap the tables, `--check` passes, CI job, commit `build: generate the artifacts tables from the module POMs and matrix.json`.

### Task 2: Discovery script

**Files:** Create `scripts/discover.py`, `tests/scripts/test_discover.py`, `tests/scripts/fixtures/` (small fake metadata if useful).

**Interfaces:**
- I/O: `fetch_versions(group, artifact) -> list[str]` from `https://repo1.maven.org/maven2/<group path>/<artifact>/maven-metadata.xml` (one retry, 5 s), `fetch_spark_parent(version) -> {"hadoop": str, "scala": str}` from `org/apache/spark/spark-parent_2.13/<v>/spark-parent_2.13-<v>.pom` (`<hadoop.version>`, `<scala.version>`; for Scala 2.12 modules read `spark-parent_2.12`).
- Pure: `plan_updates(modules, rows, spark_versions, delta_versions_by_artifact, spark_parent) -> (modules', rows', additions: list[str])` applying spec rules 2–6 in order; stable releases only (`^\d+\.\d+\.\d+$`); numeric version comparison.
- Writers: `write_matrix(rows) -> str` (byte-stable layout), `edit_pom(text, prop, value) -> str`, `new_module_pom(newest_pom_text, minor, spark, delta, scala, hadoop) -> str` (also replaces artifactId, name, description), `add_module(parent_pom_text, module_dir) -> str`, `changelog_unreleased(text, line) -> str`.
- CLI: `--dry-run` prints additions; without it writes files; `--help` documents `DISCOVER_FAKE_MAVEN` (JSON with fake version lists, test-only).
- Exit non-zero on network failure after the retry.

- [ ] Tests first for each rule and every Review Focus item (pinned rows untouched; no Delta artifact → no new line; per-module Delta minor; byte-identical no-op on the real files; Spark patch moves only current rows and updates scala/hadoop from spark-parent; new line module POM content; ordering), then the script; `python scripts/discover.py --dry-run` against Maven Central today (report the output; it may legitimately propose updates); commit `build: discovery of new Spark and Delta versions`.

### Task 3: Discovery workflow and repository setting

**Files:** Create `.github/workflows/discover.yml`, `scripts/ensure_ci.sh` (copy from sdaberdaku/spark-images, adapted: `ci.yml` here also needs `full`? no: pull_request runs the full matrix).

**Interfaces:** daily `30 5 * * *` + dispatch; `concurrency: discover`; `permissions: contents: write, pull-requests: write, actions: write`; steps: checkout, setup-python 3.12, setup-java 17 not needed; open-PR repair loop as in spark-images (BEHIND → `gh pr update-branch --rebase` + wait for new head + ensure-CI + re-arm; DIRTY → close `--delete-branch` and continue; else ensure-CI + re-arm); stale `discover/*` branch without PR deleted; run `scripts/discover.py`; if additions: `scripts/render_tables.py`, branch `discover/<yyyymmdd>`, commit as `github-actions[bot]`, push, `gh pr create` (plain-prose body listing additions; a new module adds "a release is needed to publish the new artifact"), ensure-CI, `gh pr merge --auto --squash --delete-branch`. Every run step `set -euo pipefail`.
- The repository setting "Allow GitHub Actions to create and approve pull requests" is applied by the controller, not the implementer.
- Rehearsal: from a throwaway branch, dispatch with `DISCOVER_FAKE_MAVEN` pretending Spark 4.3.0 and `delta-spark_4.3_2.13:4.5.0` exist; confirm the PR, CI dispatch and auto-merge arming; close the PR unmerged; delete the branches.

- [ ] Workflow, actionlint, PR, ci-ok, squash-merge, rehearsal, no-op dispatch on main, commit `ci: daily discovery of new Spark and Delta versions`.

### Task 4: Renovate

**Files:** Create `renovate.json`; Modify `docs/site/development.md` or README development section (one paragraph: what Renovate updates and automerges, what discovery owns).

**Interfaces:** per spec Decided 11. Validate with `docker run --rm -v "$PWD":/w -w /w renovate/renovate renovate-config-validator renovate.json`; dry extraction with `RENOVATE_CONFIG_FILE=/w/renovate.json renovate --platform=local --dry-run=extract` and report the extracted dependencies (none of the disabled groups may produce updates; check the rules, since extraction lists them anyway).

- [ ] Config, validation, extraction, PR, ci-ok, squash-merge, commit `ci: Renovate configuration`.
