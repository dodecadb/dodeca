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
   versions, `dbms.dozendb.cluster.enabled=true`, its own
   `server.dozendb.cluster.advertised_address`, and
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
elsewhere) or was removed from this one.

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
member, delete its data directory, and add it again with the new address.
