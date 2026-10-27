# gluecat version automation: discovery and Renovate

- Status: decided 2026-10-27 (owner asked for Renovate and discovery; the controller made the calls below)
- Scope: keep Spark, Delta and tooling versions current through pull requests that CI judges, without losing the
  deliberate compatibility rows in `matrix.json`.

## Facts this design rests on

- Each artifact is a module `modules/spark-<minor>_<scala>/` holding only a `pom.xml` with properties
  `spark.version`, `spark.minor`, `delta.version`, `scala.version`, `java.release`, `hadoop.aws.version`;
  sources are shared (`src/main/scala`, optional `src/main/spark-<minor>/scala`).
- `matrix.json` is a compatibility record, not a list of latest versions:
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
   - module `spark.version` → new patch; `scala.version` and `hadoop.aws.version` → the values in that
     release's `spark-parent` POM, but only when that value is numerically higher than the module's (Spark's
     parent POM can carry an older Scala or Hadoop than the module pins, e.g. Scala 2.13.8 for Spark 3.5);
   - every row of that module whose `spark` equals the old module `spark.version` moves to the new patch; pinned
     rows stay.
3. New Delta patch for a line (e.g. 4.4.2 when rows use 4.4.1, artifact available for the module's Spark minor):
   - every row and the module `delta.version` on 4.4.1 move to 4.4.2 (a patch supersedes its predecessor),
     including rows on a pinned Spark patch (the 3.5.3 floor rows follow 3.3.3 → 3.3.4); the pin is the Spark
     patch, and discovery never changes the Spark version of a pinned row;
   - the Delta artifact is checked per Spark minor first (`io.delta:delta-spark_<spark.minor>_<scala>`); moved
     rows get `"deltaArtifact": "<that artifact>"` (after `delta` in the key order) unless it is the module's
     default artifact, and fall back to the artifact they already use only when the per-Spark artifact lacks the
     version.
   - when the module's default `delta.artifact` lacks the version it moves to and the per-Spark artifact has it,
     the module's default becomes the per-Spark artifact (the property line is added after `delta.version` if the
     module inherits the parent's default); rows that inherited the old default and whose Delta the new one lacks
     name the old artifact explicitly.
4. New Delta minor (e.g. 4.5.0) with an artifact for a module's Spark minor:
   - copy that module's current rows (module `spark.version`, current Delta; one per JDK) with the new Delta; the
     copied rows keep their `smoke` flags and get `deltaArtifact` under the same per-Spark-first rule as rule 3;
   - the previous current-Delta rows shrink to the lowest-JDK row with `smoke: false` (matches the existing
     pattern for older Delta versions);
   - module `delta.version` → the new Delta.
5. New Spark minor or major line (e.g. 4.3, 5.0) for Scala 2.13, only once `delta-spark_<minor>_2.13` exists:
   - new module directory from the newest module's POM: artifactId, name, description, `spark.version`,
     `spark.minor`, `delta.version` (newest Delta with that artifact), `delta.artifact` = `delta-spark_<minor>_2.13` (the module
     default, so its rows carry no `deltaArtifact`), `scala.version` and `hadoop.aws.version`
     from `spark-parent`, `java.release` unchanged;
   - `<module>` entry in the parent POM after the newest one;
   - matrix rows copied from the newest module's current-Spark, current-Delta rows (same JDKs and smoke flags);
   - a `CHANGELOG.md` "Unreleased" line naming the new artifact.
6. Order inside one run: Spark patches, Delta patches, Delta minors, new Spark lines.
7. Writers keep layouts byte-stable: `matrix.json` one row per line in the existing key order; POM edits are
   targeted text replacements of single property lines (no XML re-serialisation).
8. README and `docs/site/index.md` "Artifacts" tables are generated from the module POMs and `matrix.json`
   (`scripts/render_tables.py`, between `<!-- artifacts:start -->` / `<!-- artifacts:end -->` markers); CI checks
   they are current (check-only, no pushes). The discovery workflow renders them before committing.
9. Workflow `discover.yml`: daily at 05:30 UTC and on dispatch; the self-healing open-PR handling, ensure-CI helper
   and concurrency group of sdaberdaku/spark-images (rebase a BEHIND PR, close and redo a DIRTY one, re-dispatch CI
   only if no run exists for the head SHA, `gh pr merge --auto --squash --delete-branch`). One branch
   `discover/<yyyymmdd>` per run, commit `build: <additions>` by `github-actions[bot]`.
10. A merged discovery PR changes what is built; publishing stays manual (a `vX.Y.Z` tag). The PR body says so when
    it adds a module.
11. Renovate (`renovate.json`, Mend app installed): `github-actions`, `maven` (plugins and test-scope dependencies
    in the parent POM), Docker images in `ci.yml`, `docker/docker-compose.yml` and the `Makefile` (MiniStack, Trino,
    the legacy Spark image) via the `docker-compose` manager plus regex managers; `packageRules` disable
    `org.apache.spark:*`, `io.delta:*`, `org.scala-lang:*`, `org.apache.hadoop:*` and `software.amazon.awssdk:*`
    (SDK 2.24.6 is a deliberate `provided` floor); patch, pin and digest updates and all action updates automerge
    after `ci-ok`; `group:allNonMajor`; weekly schedule.

## Non-goals

- Automatic releases. New Scala binary versions (2.12 lines stay as they are; no Scala 3). Source shims for a new
  Spark line (if a new line needs code under `src/main/spark-<minor>/scala`, the discovery PR fails CI and waits).
- Pruning old compatibility rows.

## Left to the builder

- Maven Central access details (metadata XML parsing, retries), exact table columns as long as they carry the
  information of the current hand-written tables (artifact, Spark, Scala, bytecode, JDKs, Delta versions with the
  "(on Spark x.y.z)" note for pinned rows).
