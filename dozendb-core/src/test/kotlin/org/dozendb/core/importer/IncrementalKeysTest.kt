package org.dozendb.core.importer

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.core.importer.IncrementalImporter.Companion.key
import org.dozendb.core.importer.IncrementalImporter.Companion.keyType
import org.junit.jupiter.api.Test
import org.neo4j.batchimport.api.input.IdType
import org.neo4j.dbms.database.DbmsRuntimeVersion
import org.neo4j.kernel.KernelVersion
import org.neo4j.values.storable.Values

/** How an incremental import compares node keys, and which DBMS runtime opens a store. */
class IncrementalKeysTest {
    @Test
    fun `text keys are the text values of the store`() {
        assertThat(key(Values.stringValue("a"), KeyType.TEXT)).isEqualTo("a")
        assertThat(key(Values.longValue(5), KeyType.TEXT)).isNull()
    }

    @Test
    fun `integer keys are the whole numbers of the store, whatever their type`() {
        assertThat(key(Values.byteValue(5), KeyType.INTEGER)).isEqualTo(5L)
        assertThat(key(Values.intValue(5), KeyType.INTEGER)).isEqualTo(5L)
        assertThat(key(Values.longValue(Long.MIN_VALUE), KeyType.INTEGER)).isEqualTo(Long.MIN_VALUE)
        assertThat(key(Values.doubleValue(2.0), KeyType.INTEGER)).isEqualTo(2L)
        assertThat(key(Values.doubleValue(2.5), KeyType.INTEGER)).isNull()
        assertThat(key(Values.doubleValue(Double.NaN), KeyType.INTEGER)).isNull()
        assertThat(key(Values.doubleValue(Double.POSITIVE_INFINITY), KeyType.INTEGER)).isNull()
        assertThat(key(Values.stringValue("5"), KeyType.INTEGER)).isNull()
    }

    @Test
    fun `an ID space's own ID type decides how its keys compare`() {
        assertThat(keyType(null, null, IdType.STRING)).isEqualTo(KeyType.TEXT)
        assertThat(keyType(null, null, IdType.INTEGER)).isEqualTo(KeyType.INTEGER)
        assertThat(keyType("Movie", "int", IdType.STRING)).isEqualTo(KeyType.INTEGER)
        assertThat(keyType("Movie", "string", IdType.STRING)).isEqualTo(KeyType.TEXT)
        assertThatThrownBy { keyType("Movie", "date", IdType.STRING) }
            .hasMessage(
                "ID space 'Movie' has IDs of type date, which incremental import cannot match " +
                    "with existing nodes: use string, long, int, short or byte."
            )
        assertThatThrownBy { keyType("Movie", "string", IdType.INTEGER) }
            .hasMessage("ID space 'Movie' has text IDs, which --id-type=integer does not take.")
    }

    @Test
    fun `a store opens in the latest DBMS runtime of its kernel version`() {
        assertThat(ScratchDbms.runtimeVersion(KernelVersion.V5_25))
            .isEqualTo(DbmsRuntimeVersion.V5_25)
        // Two runtime versions ran kernel version 4.2.
        assertThat(ScratchDbms.runtimeVersion(KernelVersion.V4_2))
            .isEqualTo(DbmsRuntimeVersion.V4_3)
    }
}
