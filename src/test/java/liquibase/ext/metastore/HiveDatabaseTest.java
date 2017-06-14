package liquibase.ext.metastore;

import liquibase.database.ObjectQuotingStrategy;
import liquibase.ext.metastore.hive.database.HiveDatabase;
import liquibase.structure.core.Table;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HiveDatabaseTest {

    @Test
    public void testGetDefaultDriver() {
        HiveDatabase database = new HiveDatabase();
        assertEquals("com.cloudera.hive.jdbc41.HS2Driver", database.getDefaultDriver("jdbc:hive2://localhost:21050/test"));
        assertNull(database.getDefaultDriver("jdbc:oracle://localhost;databaseName=liquibase"));
    }

    @Test
    public void testEscapeObjectName() {
        HiveDatabase databaseWithDefaultQuoting = new HiveDatabase();
        databaseWithDefaultQuoting.setObjectQuotingStrategy(ObjectQuotingStrategy.LEGACY);
        assertEquals("Test", databaseWithDefaultQuoting.escapeObjectName("Test", Table.class));

        HiveDatabase databaseWithAllQuoting = new HiveDatabase();
        databaseWithAllQuoting.setObjectQuotingStrategy(ObjectQuotingStrategy.QUOTE_ALL_OBJECTS);
        assertEquals("`Test`", databaseWithAllQuoting.escapeObjectName("Test", Table.class));

        HiveDatabase databaseWithReservedWordsQuoting = new HiveDatabase();
        databaseWithReservedWordsQuoting.setReservedWords();
        databaseWithReservedWordsQuoting.setObjectQuotingStrategy(ObjectQuotingStrategy.QUOTE_ONLY_RESERVED_WORDS);
        assertEquals("`timestamp`", databaseWithReservedWordsQuoting.escapeObjectName("timestamp", Table.class));
        assertEquals("Test", databaseWithReservedWordsQuoting.escapeObjectName("Test", Table.class));
    }

    @Test
    public void testGetCurrentDateTimeFunction() {
        assertEquals("CURRENT_TIMESTAMP()", new HiveDatabase().getCurrentDateTimeFunction());
    }
}
