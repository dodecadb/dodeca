# Contributing to Dodeca

## Clean room

Dodeca reimplements what Neo4j keeps for its Enterprise Edition from Neo4j's
public documentation alone: the operations manual and the Cypher manual of
Neo4j Enterprise 2026. Never read, copy or decompile Neo4j Enterprise's code,
binaries or anything derived from them, not even to compare behaviour, and do
not contribute what you wrote after reading it. Neo4j Community's code is
GPL-licensed and may be read, and built on as
[`doc/UPSTREAM_TOUCHPOINTS.md`](doc/UPSTREAM_TOUCHPOINTS.md) records.

Where Enterprise's documentation says nothing, Dodeca decides, and
[`doc/COMPATIBILITY.md`](doc/COMPATIBILITY.md) says what it decided.

## Building and testing

The README says how to build and run the tests. Before sending a change:

```sh
mise exec -- ./mvnw spotless:apply        # format the Kotlin code with ktfmt
mise exec -- ./mvnw verify -Pit           # every test, the Docker ones too
```

- A bug fix starts with a test that reproduces the bug as a user meets it,
  end to end where possible, and fails before the fix.
- Tests are deterministic: wait for the state a step depends on, such as a
  write replicated to the member read next, never for a fixed time.
- A failing or flaky test, even one the change did not touch, is fixed rather
  than retried or skipped.

CI on GitHub Actions (`.github/workflows/ci.yml`) runs the same on each pull
request and each push to `main`: it packages the jar, runs the unit tests, and
runs the Docker tests against the packaged jar, split over six jobs. Its check
`CI` passes once every job did. A failed job keeps its test reports, and for
the Docker tests the members' logs, as an artifact of the run.

## Documentation

Update the documents a change affects in the same change:

| Document | When |
|---|---|
| [`doc/OPERATOR_GUIDE.md`](doc/OPERATOR_GUIDE.md) | Operators do something new or differently: a command, a setting, a log line to look for. |
| [`doc/COMPATIBILITY.md`](doc/COMPATIBILITY.md) | Dodeca supports another Enterprise feature, behaves differently from Enterprise's documentation, or decides what it leaves open. |
| [`doc/FORMATS.md`](doc/FORMATS.md) | What members send each other or keep changes. Follow its rules for changing a format, so that rolling upgrades keep working. |
| [`doc/UPSTREAM_TOUCHPOINTS.md`](doc/UPSTREAM_TOUCHPOINTS.md) | The code builds on another Neo4j or Apache Ratis class, or relies on more of their behaviour. |
| [`doc/RELEASING.md`](doc/RELEASING.md) | How releases are numbered, made or published changes, as in the workflows under `.github/`. |

Code that ports or copies Neo4j Community, DozerDB or Apache Ratis code keeps
their copyright notices and says what Dodeca changed and when.

## Commits

Each commit is one change that builds and passes its tests on its own. Its
subject is a short imperative sentence, such as "Log once when a member stops
answering"; its body, when needed, says why and what the reader would not see
in the diff.

## License

Dodeca is licensed under the GNU General Public License, version 3 or later
(`LICENSE`). By contributing, you agree that your contribution is licensed the
same way.

The Dodeca Authors, in its copyright notices, are everyone who contributed to
Dodeca, as its git history records; each keeps the copyright of what they
contributed.
