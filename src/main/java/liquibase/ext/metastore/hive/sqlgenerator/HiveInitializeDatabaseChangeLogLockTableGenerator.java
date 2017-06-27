package liquibase.ext.metastore.hive.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.metastore.hive.database.HiveDatabase;
import liquibase.ext.metastore.hive.statement.HiveInsertStatement;
import liquibase.ext.metastore.statement.TruncateTableStatement;
import liquibase.ext.metastore.utils.CustomSqlGenerator;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.InitializeDatabaseChangeLogLockTableGenerator;
import liquibase.statement.core.InitializeDatabaseChangeLogLockTableStatement;

public class HiveInitializeDatabaseChangeLogLockTableGenerator extends InitializeDatabaseChangeLogLockTableGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(InitializeDatabaseChangeLogLockTableStatement statement, Database database) {
        return database instanceof HiveDatabase && super.supports(statement, database);
    }

    @Override
    public Sql[] generateSql(InitializeDatabaseChangeLogLockTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String catalogName = database.getLiquibaseCatalogName();
        String schemaName = database.getDefaultSchemaName();
        String tableName = database.getDatabaseChangeLogLockTableName();
        HiveInsertStatement insertStatementHive = new HiveInsertStatement(catalogName, schemaName, tableName)
                .addColumnValue(1)
                .addColumnValue(Boolean.FALSE)
                .addColumnValue("NULL")
                .addColumnValue("NULL");

        return CustomSqlGenerator.generateSql(database,
                new TruncateTableStatement(catalogName, schemaName, tableName),
                insertStatementHive);
    }
}
