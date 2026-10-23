# Development

JDK 17 or later builds every module (CI builds the Scaladoc on JDK 17; only the compatibility suites use JDK 21).

## Layout

- Shared Scala sources in `src/main/scala` and `src/test/scala`, compiled once per artifact.
- One Maven module per artifact under `modules/spark-<minor>_<scala>/`. Each module pins its Spark, Delta and Scala
  versions as properties; CI overrides them per matrix row.
- `matrix.json` is the single source of the Spark x Delta x JDK test matrix. Pull requests that are ready for review
  run every row, the compatibility suites and the docs build; the `ci-ok` job summarises them and is the required
  status check of the `main` branch protection, so nothing merges without a green matrix. On a draft nothing runs and
  `ci-ok` is red; marking the pull request ready for review starts the run. Pushes to `main` run no tests (the merged
  tree is the one the pull request tested; `pages.yml` publishes the site). `workflow_dispatch` with `full=false` runs
  the smoke rows only.
- The AWS SDK v2 modules `glue` and `sts` are `provided` dependencies: the jar contains no SDK classes, and the
  published POM (the module's own `pom.xml`) lists them as `provided` and nothing from the SDK at compile or runtime
  scope (`PackagingIT` and `PublishedPomSuite` check both).

## Make targets

```
make ministack-up    # Glue/STS/IAM emulator on localhost:4566
make test            # unit tests of one module (MODULE=spark-4.1_2.13 by default)
make verify          # unit tests, the jar, and integration tests against the jar
make package         # every module's jar
make matrix          # full CI matrix on GitHub Actions for the current branch
make compat-up       # MiniStack, Trino and Spark 4.1 with the Glue Hive client (replaces ministack-up's)
make compat          # Trino and that Spark read gluecat's tables, and gluecat reads theirs
make compat-down     # stops them and empties /tmp/gluecat-compat; make ministack-up brings back the emulator
make docs            # serve this site locally with mkdocs
```

Other Spark lines: `make verify MODULE=spark-3.5_2.12`; override versions with `-Dspark.version=...
-Ddelta.version=...`.

## Tests

Unit tests are `*Suite` classes (surefire, run against `target/classes`); integration tests are `*IT` classes
(failsafe, run against the module's jar) and use MiniStack, a local AWS emulator for Glue, STS and IAM
(`ministackorg/ministack`, version pinned in the `Makefile` and `ci.yml`). Real AWS is never used.

## Compatibility containers

`docker/docker-compose.yml` starts MiniStack, Trino 484 and a legacy Spark 4.1 (Spark 4.1.3 with the Glue Hive
client for Spark 4; Delta 4.4.1 is downloaded on its first run) for the `TrinoCompat*IT` and `LegacyHiveGlueCompat*IT`
suites. `make compat-up` creates `/tmp/gluecat-compat` (mode 0777), where the tests write table data that both
readers see at the same path, and starts the stack; `make compat` runs the suites; `make compat-down` stops it.
Without the `TRINO_URL` and `LEGACY_SPARK_CONTAINER` environment variables the suites are skipped.

## This site

`make docs` copies the logo and `CHANGELOG.md` into `docs/site/` (both copies are git-ignored), installs the pinned
`mkdocs` and `mkdocs-material` and serves the site. `mkdocs build --strict` must pass with no warnings; CI runs it on
every pull request. The Scaladoc is built from the Spark 4.1 module in `pages.yml` and published under `/api/`.
