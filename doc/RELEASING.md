# Releasing DozenDB

A release starts with a version tag pushed to GitHub. The release workflow
(`.github/workflows/release.yml`) then tests, builds and publishes it; nothing
is published from a laptop.

- [Versions](#versions)
- [Neo4j versions](#neo4j-versions)
- [Dependency updates](#dependency-updates)
- [Making a release](#making-a-release)
- [What a release publishes](#what-a-release-publishes)
- [When a release fails](#when-a-release-fails)
- [Setting up the repository](#setting-up-the-repository)

## Versions

DozenDB's version is its own, in [Semantic Versioning](https://semver.org):
`0.2.0`, or `0.3.0-rc.1` for a pre-release. Each release is built for exactly
one Neo4j version, which the names of its jar and image carry, as in
`dozendb-0.2.0-neo4j-5.26.27.jar` and
`ghcr.io/dozendb/dozendb:0.2.0-neo4j-5.26.27`.

What a number says to operators:

- A patch release (`0.2.1`) fixes bugs, updates dependencies, or moves to a
  newer Neo4j 5.26 patch.
- A minor release (`0.3.0`) adds features. It may raise the format level
  ([`FORMATS.md`](FORMATS.md)), which
  [`finishUpgrade`](OPERATOR_GUIDE.md#upgrading-dozendb) then switches to.
- A major release, from `1.0.0` on, asks operators for more than replacing
  the jar: a setting or procedure removed, or an upgrade from the previous
  release that is not a rolling one.
- Until `1.0.0`, while DozenDB is not ready for production, a minor release
  can do what a major one does. Its notes say so.

Each release runs next to the one before it, as [`FORMATS.md`, "Changing a
format"](FORMATS.md#changing-a-format) requires, so that upgrading from the
previous release is a rolling upgrade, one member at a time, unless the
release moves to another Neo4j version: every member must run the same Neo4j
version, and such a release is installed by shutting down every member, as
for a Neo4j upgrade ([Upgrading DozenDB](OPERATOR_GUIDE.md#upgrading-dozendb)).
The release notes say which, and from which earlier releases a rolling upgrade
works.

## Neo4j versions

Each release is built for the newest Neo4j 5.26 patch, and only for it. When
Neo4j publishes a patch, Dependabot opens a pull request that raises
`neo4j.version` (the `neo4j` group in `.github/dependabot.yml`), and CI runs
every test on it. Before merging it:

1. Read Neo4j's changelog for the patch, and recheck the Neo4j classes in
   [`UPSTREAM_TOUCHPOINTS.md`](UPSTREAM_TOUCHPOINTS.md) for changes between
   the two versions.
2. Write the new version wherever the documents name the old one: `git grep
   -n 5.26.27 -- '*.md'`.
3. Raise the versions of what DozenDB takes from Neo4j's `lib/` to those the
   new Neo4j ships, if they changed: slf4j, JNA, Jackson and the JAX-RS API,
   which Dependabot leaves alone.

Then release, normally as a patch release, so that operators get Neo4j's fixes.
Neo4j's security fixes come in its patches, which is why Dependabot proposes
them at once instead of a week later as other updates.

Moving to another Neo4j line, such as Neo4j's calendar releases (2026.x), is a
decision of its own, not an update: Dependabot ignores them.

## Dependency updates

Dependabot opens a pull request a week for GitHub Actions and one for the
Maven dependencies, grouped, each for versions out for a week at least.
Security updates come at once, in pull requests of their own. CI runs every
test on each. Before merging one, update what records the versions it
changes: [`COMPATIBILITY.md`, "Pinned
versions"](COMPATIBILITY.md#pinned-versions), and `NOTICE` when the jar
bundles a new library or a library's license changed.

## Making a release

1. Pick the version (see [Versions](#versions)), and check that CI passed on
   the commit of `main` to release.
2. Write the release notes in a file, in Markdown. The first line is the
   title, `DozenDB 0.2.0`; after a blank line:

   ```markdown
   ## Highlights

   What changed for operators and users, with links to the documents.

   ## Upgrading

   From which releases a rolling upgrade works, or that every member must be
   shut down (a new Neo4j version); whether finishUpgrade raises the format
   level; settings or procedures renamed or removed; anything to do before or
   after.
   ```

   An agent can draft them from `git log v0.1.0..main`; read and correct
   them before tagging.
3. Tag the commit with the notes, and push the tag:

   ```sh
   git tag -s --cleanup=whitespace -F notes.md v0.2.0 main
   git push origin v0.2.0
   ```

   `-s` signs the tag, `-a` instead makes it without a signature.
   `--cleanup=whitespace` keeps the notes' `##` headings, which git otherwise
   drops as comments.
4. Follow the Release workflow in the repository's Actions tab. It refuses a
   tag that is not `v` and a version, or that carries no notes, at once. It
   then runs CI on the tagged commit, packaging that version (about half an
   hour), pushes the image, and publishes the GitHub Release.
5. Set `main` to the next version's snapshot, normally the next minor
   version, and commit it as "Start 0.3.0":

   ```sh
   mise exec -- ./mvnw org.codehaus.mojo:versions-maven-plugin:2.25.1:set \
     -DnewVersion=0.3.0-SNAPSHOT -DgenerateBackupPoms=false -DprocessAllModules=true
   ```

The pom of the tagged commit keeps its `-SNAPSHOT` version: the workflow sets
the release's version only in what it builds.

A pre-release, such as `v0.3.0-rc.1`, is published the same way, as a GitHub
pre-release, and its image gets only its own tags.

## What a release publishes

| What | Where | Names |
|---|---|---|
| The jar | The GitHub Release | `dozendb-<version>-neo4j-<neo4j version>.jar`, with `SHA256SUMS` |
| The image | `ghcr.io/dozendb/dozendb`, for `linux/amd64` and `linux/arm64` | `<version>`, `<version>-neo4j-<neo4j version>`, `<major>.<minor>`, and `latest` for the highest release; a pre-release only the first two |
| Attestations | GitHub's attestations, and the image's registry | One for the jar, one for the image |

The image is Neo4j's official Community image of that Neo4j version, with the
jar in its `lib/` (`docker/Dockerfile`). It is built at releases only: fixes
of Neo4j's image arrive with the next release.

The attestations prove that the release workflow built the jar and the image
from the tagged commit. The release notes give the commands that check them:

```sh
gh attestation verify dozendb-0.2.0-neo4j-5.26.27.jar --repo dozendb/dozendb
gh attestation verify oci://ghcr.io/dozendb/dozendb:0.2.0 --repo dozendb/dozendb
```

## When a release fails

Until the GitHub Release is published, nothing was announced. When a step
failed for a passing reason, such as a registry timeout, re-run the failed
jobs of the workflow run. When the tagged commit is at fault, fix it on
`main`, delete the tag, and tag again:

```sh
git push origin :refs/tags/v0.2.0
git tag -d v0.2.0
```

An image the failed run pushed is replaced when the tag is released again.

A published release stays as it is: never move or delete its tag. Fix it in
the next patch release.

## Setting up the repository

Once, in the settings of `dozendb/dozendb`:

- Rules: on `main`, require the status check `CI`, which passes only when
  every CI job did, and block force pushes and deletion. On tags `v*`, allow
  only maintainers to create them, and block updates.
- Releases: enable immutable releases, so that a published release's tag
  and files cannot change.
- Actions: require actions to be pinned to a full commit SHA, as the
  workflows are and Dependabot keeps them.
- Security: enable private vulnerability reporting, which
  [`SECURITY.md`](../SECURITY.md) relies on, and Dependabot alerts and
  security updates.
- Packages: after the first release, check that the package
  `ghcr.io/dozendb/dozendb` is public and linked to the repository.
