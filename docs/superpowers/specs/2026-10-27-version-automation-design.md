# gluecat version automation: discovery and Renovate

- Status: decided 2026-10-27 (owner asked for Renovate and discovery; the controller made the calls below)
- Scope: keep Spark, Delta and tooling versions current through pull requests that CI judges, without losing the
  deliberate compatibility rows in `matrix.json`.

## Facts this design rests on

- Each artifact is a module `modules/spark-<minor>_<scala>/` holding only a `pom.xml` with properties
  `spark.version`, `spark.minor`, `delta.version`, `delta.artifact` (optional; the parent POM holds the default
  `delta-spark_${scala.binary.version}`), `scala.version`, `java.release`, `hadoop.aws.version`; sources are shared (`src/main/scala`, optional `src/main/spark-<minor>/scala`).
- `matrix.json` records the compatibility that CI proves:
  - rows on the module's `spark.version` are the "current" rows;
  - rows on another Spark patch are deliberate pins (3.5.3 is the minimum supported 3.5; Delta 4.1.0 and 4.2.0 stay
    on Spark 4.1.1 because they fail `ALTER TABLE` on 4.1.2+);
  - the newest Delta runs on every supported JDK; older Delta versions run on one JDK (the module's lowest).
- Delta publishes one artifact per Spark minor (`io.delta:delta-spark_<minor>_<scala>`), except older lines
  listed in `deltaArtifact` (`delta-spark_2.13`).
- Spark's `org.apache.spark:spark-parent_<scala>:<version>` POM on Maven Central carries `hadoop.version` and
  `scala.version` for that Spark release.

## Decided

1. Discovery (`scripts/discover.py`, stdlib only, pure functions over fetched Maven metadata) owns every Spark,
   Delta, Scala and hadoop-aws version in the module POMs and in `matrix.json`. Renovate never touches them.
2. New Spark patch in a module's line (e.g. 4.2.1 > module `spark.version` 4.2.0):
   - module `spark.version` → new patch; `scala.version` and `hadoop.aws.version` follow the `spark-parent` POM only
     when its value differs between the old and the new Spark patch, and only upward (Spark's parent POM can carry
     an older Scala or Hadoop than the module pins, e.g. Scala 2.13.8 for Spark 3.5); the addition line names the
     change, e.g. `spark 4.2.1 (spark-4.2_2.13: hadoop-aws 3.5.1)`, with several modules
     `spark 3.5.10 (spark-3.5_2.12: scala 2.12.22, hadoop-aws 3.3.6; spark-3.5_2.13: hadoop-aws 3.3.6)`;
   - every row of that module whose `spark` equals the old module `spark.version` moves to the new patch; pinned
     rows stay.
3. New Delta patch for a line (e.g. 4.4.2 when rows use 4.4.1):
   - every row and the module `delta.version` on 4.4.1 move to 4.4.2 (a patch supersedes its predecessor),
     including rows on a pinned Spark patch (the 3.5.3 floor rows follow 3.3.3 → 3.3.4); the pin is the Spark
     patch, and discovery never changes the Spark version of a pinned row;
   - a Delta version counts for a module only if its per-Spark artifact `io.delta:delta-spark_<spark.minor>_<scala>`
     carries it. The unsuffixed `delta-spark_<scala>` (or the artifact a row names) is consulted only for a Spark
     minor that has no per-Spark artifact at all (the 3.5 modules), and then only within the module's Delta major;
   - a moved row that names an explicit `deltaArtifact` keeps it when that artifact carries the new version,
     otherwise it takes the per-Spark artifact; `deltaArtifact` is written after `delta` and omitted when it is
     the module's default artifact.
4. New Delta minor (e.g. 4.5.0), under the artifact rule of 3 (a per-Spark artifact may bring a new Delta major):
   - copy that module's current rows (module `spark.version`, current Delta; one per JDK) with the new Delta; the
     copied rows keep their `smoke` flags;
   - the previous current-Delta rows shrink to the lowest-JDK row with `smoke: false` (matches the existing
     pattern for older Delta versions);
   - module `delta.version` → the new Delta. Whenever a module's `delta.version` moves to a version its
     per-Spark artifact carries, that artifact becomes the module's default `delta.artifact` (the property line
     is added after `delta.version` if the module inherits the parent's default), so the published POM resolves
     what the current rows test and those rows carry no `deltaArtifact`; rows that inherited the old default and
     whose Delta the new one lacks name the old artifact explicitly.
5. New Spark minor or major line (e.g. 4.3, 5.0) for Scala 2.13, only once `delta-spark_<minor>_2.13` exists;
   every Spark line without a module above the oldest 4.x module that has a per-Spark Delta artifact qualifies,
   and modules are inserted in version order:
   - new module directory from the POM of the module just below it: artifactId, name, description,
     `spark.version`, `spark.minor`, `delta.version` (newest Delta with that artifact), `delta.artifact` =
     `delta-spark_<minor>_2.13` (the module default, so its rows carry no `deltaArtifact`), `scala.version` and
     `hadoop.aws.version` from `spark-parent` (only upward of the template's), `java.release` unchanged;
   - `<module>` entry in the parent POM after that module;
   - matrix rows copied from that module's current-Spark, current-Delta rows (same JDKs and smoke flags);
   - a `CHANGELOG.md` "Unreleased" line naming the new artifact.
6. Order inside one run: Spark patches, Delta patches, Delta minors, new Spark lines.
7. Writers keep layouts byte-stable: `matrix.json` one row per line in the existing key order; POM edits are
   targeted text replacements of single property lines (no XML re-serialisation).
8. README and `docs/site/index.md` "Artifacts" tables are generated from the module POMs and `matrix.json`
   (`scripts/render_tables.py`, between `<!-- artifacts:start -->` / `<!-- artifacts:end -->` markers); the Spark 4.1
   example command in the README and quickstart is rendered the same way between `example` markers, with the Delta
   version from the module POM and the gluecat version from the newest released heading of `CHANGELOG.md` (the
   parent POM carries the next SNAPSHOT, so the changelog is the robust source); hadoop-aws is a placeholder there.
   CI checks they are current (check-only, no pushes). The discovery workflow renders them before committing.
9. Workflow `discover.yml`: daily at 05:30 UTC and on dispatch; the self-healing open-PR handling, ensure-CI helper
   and concurrency group of sdaberdaku/spark-images (rebase a BEHIND PR, close and redo a DIRTY one, re-dispatch CI
   only if no run exists for the head SHA, `gh pr merge --auto --squash --delete-branch`). One branch
   `discover/<yyyymmdd>` per run, commit `build: <additions>` by `github-actions[bot]`. Added after the final review:
   only same-repository pull requests count (`isCrossRepository`); a DIRTY pull request, or a BEHIND one whose
   `update-branch --rebase` is refused, is closed only when every commit is by `github-actions[bot]`, else the run
   fails with `::error::`; a failed `ci.yml` run on the head is rerun once (`gh run rerun --failed`), and a second
   failure fails the run with `::error::` naming the pull request (a failed scheduled run emails the owner); at the
   start of the job `pages.yml` is dispatched when no Pages run on `main` followed the latest commit touching
   `README.md`, `docs/site/**` or `CHANGELOG.md` (the publish guard of sdaberdaku/spark-images).
10. A merged discovery PR changes what is built; publishing stays manual (a `vX.Y.Z` tag). The PR body says so when
    it adds a module.
11. Renovate (`renovate.json`, Mend app installed): `github-actions`, `maven` (plugins and test-scope dependencies
    in the parent POM), Docker images in `ci.yml`, `docker/docker-compose.yml` and the `Makefile` (MiniStack, Trino)
    via the `docker-compose` manager plus a regex manager; `packageRules` disable `org.apache.spark:*`, `io.delta:*`,
    `org.scala-lang:*`, `org.apache.hadoop:*` and `software.amazon.awssdk:*` (SDK 2.24.6 is a deliberate `provided`
    floor) and the `spark-with-glue` legacy image (its tag is the Spark version of the legacy suite, not semver);
    patch, pin and digest updates form one "patch updates" pull request that automerges after `ci-ok`; minor updates
    form one "minor updates" pull request that is reviewed; GitHub Actions updates are their own pull request and
    automerge; major updates (every Trino release) are reviewed; weekly schedule.
12. Invariant: every module has at least one `matrix.json` row on its own `spark.version` and `delta.version`, and
    every row names an existing module. `scripts/render_tables.py` enforces it (CI runs `--check` and the message
    names the module), rules 4 and 5 raise instead of skipping a module without such rows, and `discover.py`
    validates its plan before writing.
13. `scripts/discover-ignore.txt` declines updates: one entry per line, `#` comments, in the form of the leading part
    of an addition line: `spark 4.3`, `spark 4.2.1`, `delta 4.5.0`, `delta 4.5.0 spark-4.0_2.13`,
    `module spark-4.3_2.13`.

## Non-goals

- Automatic releases. New Scala binary versions (2.12 lines stay as they are; no Scala 3). Source shims for a new
  Spark line (if a new line needs code under `src/main/spark-<minor>/scala`, the discovery PR fails CI and waits).
- Pruning old compatibility rows.

## Left to the builder

- Maven Central access details (metadata XML parsing, retries), exact table columns as long as they carry the
  information of the current hand-written tables (artifact, Spark, Scala, bytecode, JDKs, Delta versions with the
  "(on Spark x.y.z)" note for pinned rows).

## Rulings during execution

- Pinned rows move with Delta patches; the pin is the Spark patch only.
- Delta 4.x modules use the per-Spark artifact `delta-spark_<minor>_<scala>` whenever it carries the version, and a
  module's default `delta.artifact` switches to it with the module's Delta; there is no unsuffixed fallback for 4.x.
- Scala and hadoop-aws change on a Spark patch only when `spark-parent` changed them between the two patches, and
  only upward; the addition line names them per module.
- Pull requests created with the workflow token fire `pull_request` in practice (seen in sdaberdaku/spark-images);
  `ensure_ci.sh` dispatches `ci.yml` as the fallback when no run appears.
- The `spark-with-glue` legacy image is disabled in Renovate.
- The final review moved the end-to-end tests to a frozen fixture tree (`tests/scripts/fixtures/repo`), added the
  module/row invariant, `discover-ignore.txt`, the CI rerun and alert, the Pages guard, the fork filter, bot-only
  closing, and the patch and minor Renovate groups.
