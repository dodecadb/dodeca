package org.dozendb.consensus

import java.util.UUID

/**
 * Stable identity of a cluster member.
 *
 * The identity is bound to the member's data directory, so a member whose data is wiped comes back
 * as a new member. That keeps a member that lost its Raft log from voting under its old identity.
 */
@JvmInline
public value class MemberId(public val uuid: UUID) : Comparable<MemberId> {
    override fun compareTo(other: MemberId): Int = uuid.compareTo(other.uuid)

    override fun toString(): String = uuid.toString()

    public companion object {
        public fun parse(text: String): MemberId = MemberId(UUID.fromString(text))
    }
}

/** A network endpoint. IPv6 literals are written in brackets, for example `[::1]:7700`. */
public data class HostPort(val host: String, val port: Int) {
    init {
        require(host.isNotBlank()) { "host must not be blank" }
        require(port in 1..65535) { "port must be in 1..65535, was $port" }
    }

    override fun toString(): String = if (':' in host) "[$host]:$port" else "$host:$port"

    public companion object {
        /** Parses `host:port`, `[ipv6]:port`. */
        public fun parse(text: String): HostPort {
            val value = text.trim()
            val separator = value.lastIndexOf(':')
            require(separator > 0 && separator < value.length - 1) {
                "expected host:port, was '$text'"
            }
            val rawHost = value.substring(0, separator)
            val host =
                if (rawHost.startsWith('[') && rawHost.endsWith(']')) {
                    rawHost.substring(1, rawHost.length - 1)
                } else {
                    require(':' !in rawHost) { "IPv6 addresses must be bracketed, was '$text'" }
                    rawHost
                }
            val port =
                requireNotNull(value.substring(separator + 1).toIntOrNull()) {
                    "invalid port in '$text'"
                }
            return HostPort(host, port)
        }
    }
}

/** A voting member of a consensus group and the address other members reach it on. */
public data class Member(val id: MemberId, val raftAddress: HostPort) {
    /** `id@host:port`, as [parse] reads it. */
    public fun toText(): String = "$id@$raftAddress"

    /** For logs and messages: `id at host:port`. */
    override fun toString(): String = "$id at $raftAddress"

    public companion object {
        /** Parses `id@host:port`. */
        public fun parse(text: String): Member {
            val separator = text.indexOf('@')
            require(separator > 0) { "expected id@host:port, was '$text'" }
            return Member(
                MemberId.parse(text.substring(0, separator)),
                HostPort.parse(text.substring(separator + 1)),
            )
        }
    }
}

/**
 * A database replicated by its own consensus group. [id] identifies the group and is identical on
 * every member. [name] is for humans and logs.
 */
public data class DatabaseRef(val name: String, val id: UUID) {
    override fun toString(): String = "$name($id)"
}
