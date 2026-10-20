package org.dozendb.cluster

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
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
    fun `a damaged record is reported`() {
        Files.createDirectories(directory.resolve("raft"))
        Files.writeString(
            directory.resolve("raft").resolve("membership"),
            "version=1\nstate=ASLEEP\n",
        )

        assertThatThrownBy { file.read() }.isInstanceOf(IOException::class.java)
    }
}
