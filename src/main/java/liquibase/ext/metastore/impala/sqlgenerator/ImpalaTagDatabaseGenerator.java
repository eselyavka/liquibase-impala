package liquibase.ext.metastore.impala.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.impala.database.ImpalaDatabase;
import liquibase.ext.metastore.impala.statement.CreateTableAsSelectStatement;
import liquibase.ext.metastore.impala.statement.InsertAsSelectStatement;
import liquibase.ext.metastore.utils.CustomSqlGenerator;
import liquibase.ext.metastore.utils.UserSessionSettings;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.core.DropTableStatement;
import liquibase.statement.core.RenameTableStatement;
import liquibase.statement.core.TagDatabaseStatement;
import liquibase.structure.core.Column;

import java.util.UUID;

public class ImpalaTagDatabaseGenerator extends AbstractSqlGenerator<TagDatabaseStatement> {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(TagDatabaseStatement statement, Database database) {
        return database instanceof ImpalaDatabase && super.supports(statement, database);
    }

    @Override
    public ValidationErrors validate(TagDatabaseStatement tagDatabaseStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tag", tagDatabaseStatement.getTag());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(TagDatabaseStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String catalogName = database.getLiquibaseCatalogName();
        String schemaName = database.getDefaultSchemaName();
        String tableName = database.getDatabaseChangeLogTableName();
        String tableNameEscaped = database.escapeTableName(catalogName, schemaName, tableName);
        String dateColumnNameEscaped = database.escapeObjectName("DATEEXECUTED", Column.class);
        String tagColumnNameEscaped = database.escapeObjectName("TAG", Column.class);
        String tempTable = UUID.randomUUID().toString().replaceAll("-", "");
        CreateTableAsSelectStatement createTableAsSelectStatement = new CreateTableAsSelectStatement(catalogName, schemaName, tableName, tempTable)
                .addColumnNames("ID", "AUTHOR", "FILENAME", "DATEEXECUTED", "ORDEREXECUTED", "EXECTYPE", "MD5SUM", "DESCRIPTION", "COMMENTS", "TAG", "LIQUIBASE", "CONTEXTS", "LABELS", "DEPLOYMENT_ID")
                .setWhereCondition(dateColumnNameEscaped + " != (SELECT MAX(" + dateColumnNameEscaped + ") " +
                        "FROM " + tableNameEscaped + ")");
        InsertAsSelectStatement insertAsSelectStatement = new InsertAsSelectStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName(), tempTable)
                .addColumnNames("ID", "AUTHOR", "FILENAME", "DATEEXECUTED", "ORDEREXECUTED", "EXECTYPE", "MD5SUM", "DESCRIPTION", "COMMENTS", "'" + statement.getTag() + "'", "LIQUIBASE", "CONTEXTS", "LABELS", "DEPLOYMENT_ID")
                .setWhereCondition(dateColumnNameEscaped + " = (SELECT MAX(" + dateColumnNameEscaped + ") FROM " + tableNameEscaped + ") AND ("
                        + tagColumnNameEscaped + " IS NULL OR " + tagColumnNameEscaped + " != ?)")
                .addWhereParameters(statement.getTag());

        return CustomSqlGenerator.generateSql(database,
                UserSessionSettings.syncDdlStart(),
                createTableAsSelectStatement,
                insertAsSelectStatement,
                new DropTableStatement(catalogName, schemaName, tableName, false),
                new RenameTableStatement(catalogName, schemaName, tempTable, tableName),
                UserSessionSettings.syncDdlStop());
    }
}
