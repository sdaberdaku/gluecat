# Development

JDK 17 or later builds every module (JDK 21 for Scaladoc and the compatibility suites in CI).

## Layout

- Shared Scala sources in `src/main/scala` and `src/test/scala`, compiled once per artifact.
- One Maven module per artifact under `modules/spark-<minor>_<scala>/`. Each module pins its Spark, Delta and Scala
  versions as properties; CI overrides them per matrix row.
- `matrix.json` is the single source of the Spark x Delta x JDK test matrix. Pushes to `main` run every row; pull
  requests run the smoke rows.
- The AWS SDK v2 is relocated inside the shaded jar under `io.github.sdaberdaku.gluecat.shaded.awssdk`; the
  published POM declares no SDK dependency.

## Make targets

```
make ministack-up    # Glue/STS/IAM emulator on localhost:4566
make test            # unit tests of one module (MODULE=spark-4.1_2.13 by default)
make verify          # unit tests, the jar, and integration tests against the jar
make package         # every module's jar
make matrix          # full CI matrix on GitHub Actions for the current branch
make compat-up       # MiniStack, Trino and Spark 3.5 with the Glue Hive client (replaces ministack-up's)
make compat          # Trino and that Spark read gluecat's tables, and gluecat reads theirs
make compat-down     # stops them and empties /tmp/gluecat-compat; make ministack-up brings back the emulator
make docs            # serve this site locally with mkdocs
```

Other Spark lines: `make verify MODULE=spark-3.5_2.12`; override versions with `-Dspark.version=...
-Ddelta.version=...`.

## Tests

Unit tests are `*Suite` classes (surefire, run against `target/classes`); integration tests are `*IT` classes
(failsafe, run against the shaded jar) and use MiniStack, a local AWS emulator for Glue, STS and IAM
(`ministackorg/ministack`, version pinned in the `Makefile` and `ci.yml`). Real AWS is never used.

## Compatibility containers

`docker/docker-compose.yml` starts MiniStack, Trino 484 and a legacy Spark 3.5 (Spark 3.5.1 with the Glue Hive
client and Delta 3.2.0) for the `TrinoCompat*IT` and `LegacyHiveGlueCompat*IT` suites. `make compat-up` creates
`/tmp/gluecat-compat` (mode 0777), where the tests write table data that both readers see at the same path, and
starts the stack; `make compat` runs the suites; `make compat-down` stops it. Without the `TRINO_URL` and
`LEGACY_SPARK_CONTAINER` environment variables the suites are skipped.

## This site

`make docs` copies the logo and `CHANGELOG.md` into `docs/site/` (both copies are git-ignored), installs the pinned
`mkdocs` and `mkdocs-material` and serves the site. `mkdocs build --strict` must pass with no warnings; CI runs it on
every pull request. The Scaladoc is built from the Spark 4.1 module in `pages.yml` and published under `/api/`.
