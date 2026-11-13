package dev.dodeca.cluster

import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class MembershipFileTest {
    @TempDir lateinit var directory: Path

    private val file
        get() = MembershipFile(directory.resolve("raft").resolve("membership"))

    private fun member(host: String) = Member(MemberId(UUID.randomUUID()), HostPort(host, 7700))

    @Test
    fun `a member without a record has none`() {
        assertThat(file.read()).isNull()
    }

    @Test
    fun `the record survives being read again, and is replaced`() {
        val formation = listOf(member("core1"), member("::1"))
        file.write(MembershipRecord(MemberState.MEMBER, formation))

        assertThat(file.read()).isEqualTo(MembershipRecord(MemberState.MEMBER, formation))

        file.write(MembershipRecord(MemberState.REMOVED, formation))

        assertThat(file.read()).isEqualTo(MembershipRecord(MemberState.REMOVED, formation))
        assertThat(file.read()!!.formation[1].raftAddress).isEqualTo(HostPort("::1", 7700))
        assertThat(Files.list(directory.resolve("raft")).use { it.toList() }).hasSize(1)
    }

    @Test
    fun `the record keeps the cluster's format level, which older records lack`() {
        file.write(MembershipRecord(MemberState.MEMBER, formatLevel = 2))

        assertThat(file.read()!!.formatLevel).isEqualTo(2)

        Files.writeString(
            directory.resolve("raft").resolve("membership"),
            "version=1\nstate=MEMBER\nformation=\n",
        )

        assertThat(file.read()!!.formatLevel).isEqualTo(FormatLevels.INITIAL)
    }

    @Test
    fun `the record keeps the stores still to detach, which older records lack`() {
        file.write(MembershipRecord(MemberState.MEMBER, storesToDetach = setOf("system", "a.b")))

        assertThat(file.read()!!.storesToDetach).containsExactlyInAnyOrder("system", "a.b")

        Files.writeString(
            directory.resolve("raft").resolve("membership"),
            "version=1\nstate=MEMBER\nformation=\n",
        )

        assertThat(file.read()!!.storesToDetach).isEmpty()
    }

    @Test
    fun `the record keeps the member it belongs to, which older records lack`() {
        val owner = MemberId(UUID.randomUUID())
        file.write(MembershipRecord(MemberState.MEMBER, memberId = owner))

        assertThat(file.read()!!.memberId).isEqualTo(owner)

        Files.writeString(
            directory.resolve("raft").resolve("membership"),
            "version=1\nstate=MEMBER\nformation=\n",
        )

        assertThat(file.read()!!.memberId).isNull()
    }

    @Test
    fun `the record keeps the group of system once re-formed, which older records lack`() {
        val group = UUID.randomUUID()
        file.write(MembershipRecord(MemberState.MEMBER, systemGroup = group))

        assertThat(file.read()!!.systemGroup).isEqualTo(group)

        file.write(MembershipRecord(MemberState.MEMBER))

        assertThat(file.read()!!.systemGroup).isNull()
        assertThat(Files.readString(directory.resolve("raft").resolve("membership")))
            .doesNotContain("systemGroup")
    }

    @Test
    fun `a damaged record is reported`() {
        Files.createDirectories(directory.resolve("raft"))
        Files.writeString(
            directory.resolve("raft").resolve("membership"),
            "version=1\nstate=ASLEEP\n",
        )

        assertThatThrownBy { file.read() }.isInstanceOf(IOException::class.java)
    }
}
