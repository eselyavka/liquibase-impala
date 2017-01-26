package liquibase.ext.metastore.impala.sqlgenerator;

import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.ext.metastore.impala.database.ImpalaDatabase;
import liquibase.ext.metastore.impala.statement.CreateTableAsSelectStatement;
import liquibase.ext.metastore.utils.CustomSqlGenerator;
import liquibase.ext.metastore.utils.UserSessionSettings;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.RemoveChangeSetRanStatusGenerator;
import liquibase.statement.core.DropTableStatement;
import liquibase.statement.core.RemoveChangeSetRanStatusStatement;
import liquibase.statement.core.RenameTableStatement;
import liquibase.structure.core.Column;

import java.util.UUID;

public class ImpalaRemoveChangeSetRanStatusGenerator extends RemoveChangeSetRanStatusGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(RemoveChangeSetRanStatusStatement statement, Database database) {
        return database instanceof ImpalaDatabase && super.supports(statement, database);
    }

    @Override
    public Sql[] generateSql(RemoveChangeSetRanStatusStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ChangeSet changeSet = statement.getChangeSet();
        String tempTable = UUID.randomUUID().toString().replaceAll("-", "");
        String catalogName = database.getLiquibaseCatalogName();
        String schemaName = database.getDefaultSchemaName();
        String tableName = database.getDatabaseChangeLogTableName();
        CreateTableAsSelectStatement createTableAsSelectStatement = new CreateTableAsSelectStatement(catalogName, schemaName, tableName, tempTable)
                .addColumnNames("ID", "AUTHOR", "FILENAME", "DATEEXECUTED", "ORDEREXECUTED", "EXECTYPE", "MD5SUM", "DESCRIPTION", "COMMENTS", "TAG", "LIQUIBASE", "CONTEXTS", "LABELS", "DEPLOYMENT_ID")
                .setWhereCondition(" NOT (" + database.escapeObjectName("ID", Column.class) + " = ? " +
                        "AND " + database.escapeObjectName("FILENAME", Column.class) + " = ?)")
                .addWhereParameters(changeSet.getId(), changeSet.getFilePath());

        return CustomSqlGenerator.generateSql(database,
                UserSessionSettings.syncDdlStart(),
                createTableAsSelectStatement,
                new DropTableStatement(catalogName, schemaName, tableName, false),
                new RenameTableStatement(catalogName, schemaName, tempTable, tableName),
                UserSessionSettings.syncDdlStop());
    }

}
