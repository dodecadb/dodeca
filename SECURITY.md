# Security policy

## Reporting a vulnerability

Report a vulnerability in DozenDB privately, on GitHub: on the repository's
[Security tab](https://github.com/dozendb/dozendb/security), choose "Report a
vulnerability". Please do not open a public issue, pull request or discussion
about it.

Say which DozenDB version and setup it affects, how to reproduce it, and what
an attacker can do with it. The report stays between you and the maintainers
until a release fixes it; the release notes then describe it, and credit you
if you wish.

## Supported versions

Fixes go into the next release, which is built for the newest Neo4j 5.26 patch
(see [`doc/RELEASING.md`](doc/RELEASING.md)); earlier releases are not patched.
DozenDB is under development and not ready for production use.

## Vulnerabilities in Neo4j

DozenDB runs on Neo4j Community, unmodified. Report a vulnerability in Neo4j
itself to Neo4j, as [its security policy](https://github.com/neo4j/neo4j/security/policy)
says. When a Neo4j 5.26 patch fixes one, the next DozenDB release is built for
that patch.
