package liquibase.ext.metastore.database;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.logging.LogService;
import liquibase.logging.Logger;

import java.sql.*;

public class HiveMetastoreDatabase extends AbstractJdbcDatabase {

    private final String databaseProductName;
    private static final Logger LOG = LogService.getLog(HiveMetastoreDatabase.class);
    private final String prefix;
    private final String databaseDriver;

    public HiveMetastoreDatabase(String databaseProductName, String prefix, String driver) {
        this.databaseProductName = databaseProductName;
        this.prefix = prefix;
        this.databaseDriver = driver;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection databaseConnection) throws DatabaseException {
        return databaseProductName.equalsIgnoreCase(databaseConnection.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith(prefix)) {
            return databaseDriver;
        }
        return null;
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return databaseProductName;
    }

    @Override
    public Integer getDefaultPort() {
        return 0;
    }

    @Override
    public String getShortName() {
        return databaseProductName.toLowerCase();
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean requiresPassword() {
        return false;
    }

    @Override
    public boolean requiresUsername() {
        return true;
    }

    @Override
    public boolean isAutoCommit() throws DatabaseException {
        return true;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public boolean supportsSequences() {
        return false;
    }

    @Override
    public boolean supportsSchemas() {
        return true;
    }

    @Override
    public boolean supportsAutoIncrement() {
        return false;
    }

    @Override
    public boolean supportsRestrictForeignKeys() {
        return false;
    }

    @Override
    public boolean supportsDropTableCascadeConstraints() {
        return false;
    }

    @Override
    public boolean supportsDDLInTransaction() {
        return false;
    }

    @Override
    public boolean supportsPrimaryKeyNames() {
        return false;
    }

    public void setReservedWords() {
    }

    @Override
    public void setConnection(DatabaseConnection conn) {
        setReservedWords();
        super.setConnection(conn);
    }

    @Override
    public void setAutoCommit(boolean b) throws DatabaseException {
    }

    public Statement getStatement() throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {
        String url = super.getConnection().getURL();
        Driver driver = (Driver) Class.forName(getDefaultDriver(url)).newInstance();
        Connection con = driver.connect(url, System.getProperties());
        return con.createStatement();
    }

    protected String getSchemaDatabaseSpecific(String query) {
        Statement statement = null;
        try {
            statement = getStatement();
            ResultSet resultSet = statement.executeQuery(query);
            resultSet.next();
            String schema = resultSet.getString(1);
            LOG.info("Schema name is '" + schema + "'");
            return schema;
        } catch (Exception e) {
            LOG.info("Can't get default schema:", e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.warning("Can't close cursor", e);
                }
            }
        }
        return null;
    }
}
