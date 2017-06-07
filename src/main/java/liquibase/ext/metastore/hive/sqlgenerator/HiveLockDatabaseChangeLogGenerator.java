package liquibase.ext.metastore.hive.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.metastore.hive.database.HiveDatabase;
import liquibase.ext.metastore.hive.statement.HiveInsertStatement;
import liquibase.ext.metastore.statement.TruncateTableStatement;
import liquibase.ext.metastore.utils.CustomSqlGenerator;
import liquibase.ext.metastore.utils.DateTimeUtils;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.LockDatabaseChangeLogGenerator;
import liquibase.statement.core.LockDatabaseChangeLogStatement;

public class HiveLockDatabaseChangeLogGenerator extends LockDatabaseChangeLogGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(LockDatabaseChangeLogStatement statement, Database database) {
        return database instanceof HiveDatabase && super.supports(statement, database);
    }

    @Override
    public Sql[] generateSql(LockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String catalogName = database.getLiquibaseCatalogName();
        String schemaName = database.getDefaultSchemaName();
        String tableName = database.getDatabaseChangeLogLockTableName();
        HiveInsertStatement hiveInsertStatement = new HiveInsertStatement(catalogName, schemaName, tableName);
        hiveInsertStatement.addColumnValue(1);
        hiveInsertStatement.addColumnValue(Boolean.TRUE);
        hiveInsertStatement.addColumnValue(hostname + " (" + hostaddress + ")");
        hiveInsertStatement.addColumnValue(DateTimeUtils.getCurrentTS("yyyy-MM-dd HH:mm:ss"));

        return CustomSqlGenerator.generateSql(database,
                new TruncateTableStatement(catalogName, schemaName, tableName),
                hiveInsertStatement);
    }
}
