# DozenDB operator guide

## Cluster members

The members of a cluster are the voting members of the Raft group of the
`system` database. Every other database has its own Raft group, whose members
follow the cluster's: the leader of `system` adds members to each database's
group and removes them from it until they match.

Two procedures show the members, on any member:

- `CALL dozendb.cluster.members()` lists the cluster's members with their Raft
  and Bolt addresses, which one leads `system`, and in `databases` the
  databases whose groups each member votes in.
- `CALL dozendb.cluster.status()` lists, for every database running on the
  member asked, the member's role, the leader, Raft indexes, and in `members`
  the ids of the members that vote in the database's group.

Changing the members needs an administrator. The calls below work on any
member and change one member at a time. A change needs a majority of the
members before it and a majority after it: in a cluster of three, one member
may be down while another is added or removed, and in a cluster of two both
must be up to remove one.

### Adding a member

1. Configure the new server like the others: the same Neo4j and DozenDB
   versions, `dbms.dozendb.cluster.enabled=true`, the same TLS settings with a
   certificate of its own (see [TLS between members](#tls-between-members)),
   its own `server.dozendb.cluster.advertised_address`, and
   `dbms.dozendb.cluster.initial_members` listing the Raft address of at least
   one current member. Its data directory must be empty.
2. Start it. It copies `system` and the default database from a member, starts
   them without taking part in the cluster yet, and logs:

   ```
   This member is not part of the cluster yet. To add it, run
   CALL dozendb.cluster.addMember('<its Raft address>') on a member of the cluster.
   ```

3. Run that call on any member. It returns once the new member votes in the
   group of `system`, which brings it up to date first. The new member then
   copies every other database and joins the database's group; the database's
   leader brings it up to date before it votes. `dozendb.cluster.members()`
   shows each database it joined, and routing tables list it for a database
   once it votes there. Reads and writes go on throughout.

A database copied by the new member before it is added is not copied again
once it is added, unless the cluster discarded the Raft log since. A database
that is offline while a member is added is joined once it is started again.

`addMember` refuses a server that is not reachable, runs another Neo4j
version, still starts, belongs to another cluster (its databases came from
elsewhere) or was removed from this one. A server whose certificate the
members do not trust is not reachable: the error says why, for example
`unable to find valid certification path`.

### Removing a member

```
CALL dozendb.cluster.removeMember('<member id>')
```

with the id `dozendb.cluster.members()` shows. The call returns once the member
no longer votes in the group of `system`; the member then leaves the group of
every database, also of offline ones. A member removed while it is down learns
it when it starts again.

A removed member stops its databases, hands over any leadership it held, logs
an error saying that it was removed, and takes no further part in the cluster.
Shut it down. It cannot be added again as it is: to bring the server back,
delete its whole data directory, `data/server_id` included, and start it; it
then waits to be added as a new member.

### Replacing a failed member

Remove the failed member, then add its replacement as above. Adding the
replacement first works as well, at the cost of more members to reach a
majority while both are in the cluster.

### Changing an address

A member's Raft address is part of its membership. To change it, remove the
member, delete its data directory, and add it again with the new address. With
hostname verification, its certificate must name the new address.

## TLS between members

Members talk to each other on their Raft port: Raft itself, membership
changes, and copies of databases all go through it. A clustered member does
not start until `dbms.ssl.policy.cluster.enabled` is set, on every member
alike:

- `true` (recommended): every connection between members uses TLS, and each
  end presents a certificate that the other end trusts (mutual TLS).
- `false`: plaintext. Any host that reaches a Raft port can read what members
  send and act as a member. Members log a warning at every start. Use it only
  on a network that nothing but the members can reach.

The settings are Neo4j's own SSL policy settings of the `cluster` scope, as in
Neo4j Enterprise. A typical member has:

```
dbms.ssl.policy.cluster.enabled=true
dbms.ssl.policy.cluster.verify_hostname=true
```

and these files in `certificates/cluster` under the Neo4j home (or the
directory `dbms.ssl.policy.cluster.base_directory` names):

| File | Content |
|---|---|
| `private.key` | The member's private key, in PEM. |
| `public.crt` | The member's certificate, in PEM, followed by any intermediate certificates. |
| `trusted/` | The certificates of the authorities whose certificates members accept, in PEM, one per file. |
| `revoked/` | Optional: certificate revocation lists of those authorities. |

Every member's certificate must come from an authority in every member's
`trusted/` directory, and serve both ends of a connection: if it lists
extended key usages, it needs both `serverAuth` and `clientAuth`.

- `verify_hostname=true` makes a member check that the certificate of the
  member it connects to names the address it dials: the host of that member's
  `server.dozendb.cluster.advertised_address`, as a DNS name or IP address
  among the certificate's subject alternative names. Neo4j's default is
  `false`, which only checks that a trusted authority issued the certificate.
- `client_auth` keeps its default, `REQUIRE`, for members to refuse
  connections without a trusted certificate. `NONE` leaves only the member
  connected to checked, and members log a warning; `OPTIONAL` is refused.
- `tls_versions` (default TLS 1.2 and 1.3) and `ciphers` restrict the protocol
  versions and cipher suites.
- A certificate is not tied to a member id: members accept every certificate
  from a trusted authority. Use an authority, or an intermediate authority,
  that issues certificates for the cluster alone.

Each member logs how it talks to the others when it starts, in `debug.log`,
for example `Raft member ... listening on 0.0.0.0:7700 (TLS, client
certificates required, hostnames verified)`.

For example, with OpenSSL 3, an authority and a certificate for a member that
others reach as `core1`:

```
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -days 3650 -subj "/CN=DozenDB cluster CA" -keyout ca.key -out ca.crt
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=core1" -keyout private.key -out core1.csr
printf "subjectAltName=DNS:core1\nextendedKeyUsage=serverAuth,clientAuth\n" \
  > core1.ext
openssl x509 -req -in core1.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 825 -extfile core1.ext -out public.crt
```

Copy `private.key` and `public.crt` to the member, `ca.crt` to `trusted/` on
every member, and keep `ca.key` elsewhere.

### Replacing certificates

Members read their certificates when they start. Replace a member's files and
restart it, one member at a time. To move to another authority, first add its
certificate to `trusted/` on every member, restarting each; then give each
member a certificate from the new authority, restarting each; then remove the
old authority from `trusted/`, restarting each.

### Switching between TLS and plaintext

Members with TLS and members without cannot reach each other. Shut down every
member, change `dbms.ssl.policy.cluster.enabled` on each, and start them all
again.
