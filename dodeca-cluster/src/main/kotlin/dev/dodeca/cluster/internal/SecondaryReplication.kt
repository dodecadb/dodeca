package dev.dodeca.cluster.internal

/**
 * Pauses and resumes the pulls of the secondaries on this member, for
 * `dbms.cluster.secondaryReplicationDisable`.
 */
class SecondaryReplication internal constructor(private val toggle: (String, Boolean) -> String) {
    /**
     * Pauses the secondary of the database [databaseName] here if [pause], else resumes it; returns
     * its state, `paused` or `running`.
     *
     * @throws IllegalArgumentException if the database does not run here as a secondary.
     */
    fun toggle(databaseName: String, pause: Boolean): String = toggle.invoke(databaseName, pause)
}
