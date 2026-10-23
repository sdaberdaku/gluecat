# Release

## Maven Central and GitHub Release

Pushing a tag `vX.Y.Z` runs `release.yml`: the full CI matrix on the tagged commit, then every artifact is published
to Maven Central as one deployment and the jars are attached to a GitHub Release. A Maven Central release cannot be
deleted or replaced, so the full matrix must be green on the tagged commit.

Artifact names, configuration keys and the Glue entry format are public API from 1.0.0; changing any of them is a
design change first.

## Documentation site

`release.yml` runs `ci.yml` before publishing, which includes the `docs` job, so a documentation build failure blocks
a publish.

The site is deployed by `pages.yml` on pushes to `main` that touch the documentation files, or by running the
workflow manually (`workflow_dispatch`). It is not deployed from the tag: the `github-pages` environment only accepts
the default branch.
