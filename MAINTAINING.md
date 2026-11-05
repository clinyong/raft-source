# Maintaining this fork

This repository is a fork of the Raft release mirror. Upstream still publishes
one snapshot commit per release and does not accept pull requests; everything
below applies to our own fork only.

## Baseline

- Baseline is the latest upstream snapshot we have taken, plus our own patches
  on top of it.
- Current baseline: `v1.21.23-source.1` (Server 1.21.23, Computer 1.0.49).
  Newer upstream Computer builds are not part of the baseline until we decide
  they are.
- Record the baseline commit in the release/verification report of the work
  that depends on it, not only in this file.

## Merging

One rule per kind of change:

1. **Our own changes** (patches, build workflows, docs): merge with **squash**.
   One pull request is one commit on `main`.
2. **Upstream snapshot sync** (taking a new release mirror commit): merge with
   **fast-forward**, never squash. Fast-forward keeps the upstream commit SHA
   identical to upstream, so later syncs do not turn into conflicts.

Do not rewrite history that is already on `main`.

## Review

- Every pull request needs an architecture review before merge. The reviewer
  is the architecture role on the team; the executor of the change is not the
  reviewer of it.
- One person executes; the reviewer decides and reviews. The same person
  should not both approve and merge their own change.
- A review comment about deployment, migration or production timing is a
  decision for the deployment owner, not something a pull request body can
  authorize.

## Upstream automation

The upstream snapshot ships `.github/workflows/close-external-prs.yml`, which
closes every pull request as soon as it is opened. It is disabled in this
fork's Actions settings; the file itself is kept so `main` stays identical to
upstream.

After every upstream sync, check that this workflow is still disabled before
opening a pull request. A sync restores or re-adds the file, and the Actions
setting is the only thing that keeps it from closing our pull requests.

## Builds

- Local builds are for verification only. Do not build on the production VPS.
- The manual Linux build workflow (`.github/workflows/manual-linux-computer.yml`)
  is dispatched by hand and only proves that the tree compiles and produces an
  artifact. It does not deploy anything.
- For each built artifact, record the version the binary reports and the
  SHA256 of the binary. Verify the hash against the published `SHA256SUMS`
  before using it.
- Run a new Computer binary once in a separate `RAFT_HOME` before it is used
  anywhere real. Never point an unverified binary at a live server.
- Archive every binary that goes live in a GitHub Release of this fork, with
  the build commit, the reported version and the SHA256 in the release notes.
  The build artifact expires; the release does not.

## Deployment

- Nothing in this repository is deployed to production by a merge.
- Switching the production Computer or Server to a fork build needs a separate
  approval from the deployment owner, with a named time window, a backup of
  the running binary and written rollback steps.
- Keep the previous binary until the new one has run long enough to be accepted.

## Generated files

Build outputs stay out of git. `packages/computer/dist-native/` is a local
build directory and is ignored.
