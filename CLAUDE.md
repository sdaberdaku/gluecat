# gluecat

AWS Glue Data Catalog client for Apache Spark: a Spark catalog plugin (`io.github.sdaberdaku.gluecat.GlueCatalog`)
that gives each named Spark catalog its own Glue catalog ID and IAM role, loads and creates Delta tables there, and
reads other data-source tables, without the Hive metastore client. Works as a named catalog and as `spark_catalog`.

## Documents

- `docs/superpowers/specs/` holds the design. Its "Decided", "Anti-goals" and "Glue entry format" sections are binding;
  change the design before changing behaviour they cover.
- `docs/superpowers/plans/` holds implementation plans with pinned versions, signatures and tests. When code proves a
  plan wrong, update the plan, then the code.
- `docs/logo/`: `gluecat-transparent.png` (README header), `gluecat-banner.jpg` (full scene), `gluecat-social.png`
  (GitHub social preview; `gluecat-social.jpg` is the same image under GitHub's 1 MB upload limit), `gluecat-icon.png` (avatar).

## Layout

- Shared Scala sources in `src/main/scala` and `src/test/scala`, compiled once per artifact.
- One Maven module per artifact under `modules/spark-<minor>_<scala>/` (Spark 3.5 on Scala 2.12 and 2.13, Spark
  4.x on 2.13). Each module pins its Spark, Delta and Scala versions as properties; CI overrides them per matrix row.
- `matrix.json` is the single source of the Spark × Delta × JDK test matrix.
- The AWS SDK v2 (`glue`, `sts`) is `provided` at version 2.24.6; the jar contains no SDK classes.

## Commands

- `make` wraps the common commands; `MODULE` (default `spark-4.1_2.13`) picks the module and `JAVA_HOME` is used as set:
  `make test`, `make verify`, `make package` (every module), `make ministack-up` / `make ministack-down`,
  `make compat-up` / `make compat-down` / `make compat` (Trino and legacy Spark suites, `docker/docker-compose.yml`),
  `make matrix` (full CI matrix on GitHub Actions for the current, pushed branch), `make clean`.
- Glue/STS/IAM emulator for tests: `docker run -d --rm --name ministack -p 4566:4566 ministackorg/ministack:1.5.31`
  (the version pinned in `ci.yml` and the Makefile)
- Unit tests: `./mvnw -pl modules/spark-4.1_2.13 -DskipITs test`
- Unit tests, jar and integration tests against the jar: `./mvnw -pl modules/spark-4.1_2.13 verify`
- Other Spark lines: replace the module directory; override versions with `-Dspark.version=... -Ddelta.version=...`, and the Delta artifact with `-Ddelta.artifact=...` when it differs from the module default (Spark 4.0 and 4.1 modules default to `delta-spark_4.0_2.13` / `delta-spark_4.1_2.13`; use `delta-spark_2.13` for Delta 4.0.1 on 4.0 and 4.1.0 to 4.3.1 on 4.1).
- JDK 17 builds every module; CI runs each module on every JDK its Spark line supports. `ci.yml` runs every
  `matrix.json` row on pull requests (plus compat and docs; the `ci-ok` job is the required check of the `main`
  branch protection) and on dispatch with `full=true`; nothing runs on pushes to `main`.
- Release: pushing a tag `vX.Y.Z` runs `release.yml` (full matrix, then one Central deployment of every module and
  a GitHub Release). Local check without publishing: `./mvnw -Prelease -DskipTests -DskipITs -Dgpg.skip=true package`
  (sources and scaladoc jars per module).

## Conventions

- Unit tests are `*Suite` classes (surefire, run against `target/classes`); integration tests are `*IT` classes
  (failsafe, run against the module's jar).
- Never reference Spark's `private[sql]` objects (`CatalogV2Util`, `V1Table`, `IdentityTransform`); use reflection
  bridges and the public `Transform` / `Expressions` APIs.
- Delta creates and replaces run with path identifiers (``delta.`<location>` ``), never with a catalog identifier,
  and never by naming anything `io.unitycatalog.*`.
- Database and table names are lowercased in both directions; every Glue `UpdateTable` passes the `VersionId` read
  before the change; DROP never deletes data.
- Code and tests must compile for Scala 2.12 and 2.13 and for Java 8 (`scala.collection.JavaConverters`, no Java 9+
  APIs in shared code).

## Rules

- Public project: generic names only in code, docs, tests, fixtures and commit messages (`acct_a`, `111111111111`,
  `s3://bucket-a/...`). No real account IDs, bucket names or organisation names.
- Plain commit messages, no AI attribution lines.
- Artifact names, configuration keys and the Glue entry format are public API from 1.0.0; changing any of them is a
  design change first.
- A Maven Central release cannot be deleted or replaced: the full CI matrix must be green on the tagged commit.
- Show drafts of user-facing text (README, release notes) before pushing.

## Standing consent (owner, 2026-10-22)

- Push feature branches and open pull requests without asking; merge a pull request into `main` without asking once
  `ci-ok` is green (`main` is protected: pull requests only, `ci-ok` required, branch up to date, no force pushes,
  administrators included).
- Commit and push README, CHANGELOG and docs without showing a draft first (this overrides the "show drafts" rule
  above).
- Run GitHub Actions (`gh workflow run`), start and stop local Docker containers, and rewrite unpublished branch
  history as needed.
- Still ask before: creating a release tag (`v*`), flipping the repository public, deleting anything outside the
  working tree, or changing the design (spec "Decided", "Anti-goals", "Glue entry format").
