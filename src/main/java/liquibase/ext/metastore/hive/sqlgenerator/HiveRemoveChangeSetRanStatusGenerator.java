package liquibase.ext.metastore.hive.sqlgenerator;

import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.ext.metastore.hive.database.HiveDatabase;
import liquibase.ext.metastore.statement.CreateTableAsSelectStatement;
import liquibase.ext.metastore.utils.CustomSqlGenerator;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.RemoveChangeSetRanStatusGenerator;
import liquibase.statement.core.DropTableStatement;
import liquibase.statement.core.RemoveChangeSetRanStatusStatement;
import liquibase.statement.core.RenameTableStatement;
import liquibase.structure.core.Column;

import java.util.UUID;

public class HiveRemoveChangeSetRanStatusGenerator extends RemoveChangeSetRanStatusGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(RemoveChangeSetRanStatusStatement statement, Database database) {
        return database instanceof HiveDatabase && super.supports(statement, database);
    }

    @Override
    public Sql[] generateSql(RemoveChangeSetRanStatusStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ChangeSet changeSet = statement.getChangeSet();
        String tmpTable = UUID.randomUUID().toString().replaceAll("-", "");
        String catalogName = database.getLiquibaseCatalogName();
        String schemaName = database.getDefaultSchemaName();
        String tableName = database.getDatabaseChangeLogTableName();
        CreateTableAsSelectStatement createTableAsSelectStatement = new CreateTableAsSelectStatement(catalogName, schemaName, tableName, tmpTable)
                .addColumnNames("ID", "AUTHOR", "FILENAME", "DATEEXECUTED", "ORDEREXECUTED", "EXECTYPE", "MD5SUM", "DESCRIPTION", "COMMENTS", "TAG", "LIQUIBASE", "CONTEXTS", "LABELS", "DEPLOYMENT_ID")
                .setWhereCondition(database.escapeObjectName("ID", Column.class) + " != ? " +
                        "AND " + database.escapeObjectName("FILENAME", Column.class) + " != ?")
                .addWhereParameters(changeSet.getId(), changeSet.getFilePath());

        return CustomSqlGenerator.generateSql(database,
                createTableAsSelectStatement,
                new DropTableStatement(catalogName, schemaName, tableName, false),
                new RenameTableStatement(catalogName, schemaName, tmpTable, tableName));
    }
}
