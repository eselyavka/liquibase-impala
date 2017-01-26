package liquibase.ext.metastore;

import liquibase.database.Database;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.ext.metastore.impala.database.ImpalaDatabase;
import liquibase.structure.core.Table;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ImpalaDatabaseTest {

    @Test
    public void testGetDefaultDriver() {
        Database database = new ImpalaDatabase();
        assertEquals("com.cloudera.impala.jdbc41.Driver", database.getDefaultDriver("jdbc:impala://localhost:21050/test"));
        assertNull(database.getDefaultDriver("jdbc:oracle://localhost;databaseName=liquibase"));
    }

    @Test
    public void testEscapeObjectName() {
        ImpalaDatabase databaseWithDefaultQuoting = new ImpalaDatabase();
        databaseWithDefaultQuoting.setObjectQuotingStrategy(ObjectQuotingStrategy.LEGACY);
        assertEquals("Test", databaseWithDefaultQuoting.escapeObjectName("Test", Table.class));

        ImpalaDatabase databaseWithAllQuoting = new ImpalaDatabase();
        databaseWithAllQuoting.setObjectQuotingStrategy(ObjectQuotingStrategy.QUOTE_ALL_OBJECTS);
        assertEquals("`Test`", databaseWithAllQuoting.escapeObjectName("Test", Table.class));

        ImpalaDatabase databaseWithReservedWordsQuoting = new ImpalaDatabase();
        databaseWithReservedWordsQuoting.setReservedWords();
        databaseWithReservedWordsQuoting.setObjectQuotingStrategy(ObjectQuotingStrategy.QUOTE_ONLY_RESERVED_WORDS);
        assertEquals("`timestamp`", databaseWithReservedWordsQuoting.escapeObjectName("timestamp", Table.class));
        assertEquals("Test", databaseWithReservedWordsQuoting.escapeObjectName("Test", Table.class));
    }

    @Test
    public void testGetCurrentDateTimeFunction() {
        assertEquals("NOW()", new ImpalaDatabase().getCurrentDateTimeFunction());
    }
}
