package liquibase.ext.metastore.impala.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.metastore.impala.database.ImpalaDatabase;
import liquibase.ext.metastore.impala.statement.TruncateTableStatement;
import liquibase.ext.metastore.utils.DateTimeUtils;
import liquibase.ext.metastore.utils.CustomSqlGenerator;
import liquibase.ext.metastore.utils.UserSessionSettings;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.LockDatabaseChangeLogGenerator;
import liquibase.statement.core.InsertStatement;
import liquibase.statement.core.LockDatabaseChangeLogStatement;

public class ImpalaLockDatabaseChangeLogGenerator extends LockDatabaseChangeLogGenerator {

    @Override
    public boolean supports(LockDatabaseChangeLogStatement statement, Database database) {
        return database instanceof ImpalaDatabase && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(LockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String catalogName = database.getLiquibaseCatalogName();
        String schemaName = database.getDefaultSchemaName();
        String tableName = database.getDatabaseChangeLogLockTableName();
        InsertStatement insertStatement = new InsertStatement(catalogName, schemaName, tableName)
                .addColumnValue("ID", 1)
                .addColumnValue("LOCKED", Boolean.TRUE)
                .addColumnValue("LOCKEDBY", hostname + " [" + hostaddress + "]")
                .addColumnValue("LOCKGRANTED", DateTimeUtils.getCurrentTS("yyyy-MM-dd HH:mm:ss"));

        return CustomSqlGenerator.generateSql(database,
                UserSessionSettings.syncDdlStart(),
                new TruncateTableStatement(catalogName, schemaName, tableName),
                insertStatement,
                UserSessionSettings.syncDdlStop());
    }
}
