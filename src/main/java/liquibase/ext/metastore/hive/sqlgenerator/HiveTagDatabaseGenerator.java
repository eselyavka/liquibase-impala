package liquibase.ext.metastore.hive.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.hive.database.HiveDatabase;
import liquibase.ext.metastore.statement.CreateTableAsSelectStatement;
import liquibase.ext.metastore.statement.InsertAsSelectStatement;
import liquibase.ext.metastore.utils.CustomSqlGenerator;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.core.DropTableStatement;
import liquibase.statement.core.RenameTableStatement;
import liquibase.statement.core.TagDatabaseStatement;
import liquibase.structure.core.Column;

import java.util.UUID;

public class HiveTagDatabaseGenerator extends AbstractSqlGenerator<TagDatabaseStatement> {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(TagDatabaseStatement statement, Database database) {
        return database instanceof HiveDatabase && super.supports(statement, database);
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
                .setWhereCondition(tableNameEscaped + "." + dateColumnNameEscaped + " NOT IN (SELECT MAX(" + tableNameEscaped + "." + dateColumnNameEscaped + ") " +
                        "FROM " + tableNameEscaped + ")");
        InsertAsSelectStatement insertAsSelectStatement = new InsertAsSelectStatement(catalogName, schemaName, tableName, tempTable)
                .addColumnNames("ID", "AUTHOR", "FILENAME", "DATEEXECUTED", "ORDEREXECUTED", "EXECTYPE", "MD5SUM", "DESCRIPTION", "COMMENTS", "'" + statement.getTag() + "'", "LIQUIBASE", "CONTEXTS", "LABELS", "DEPLOYMENT_ID")
                .setWhereCondition(tableNameEscaped + "." + dateColumnNameEscaped + " IN (SELECT MAX(" + tableNameEscaped + "." + dateColumnNameEscaped + ") FROM " + tableNameEscaped + ") AND ("
                        + tableNameEscaped + "." + tagColumnNameEscaped + " IS NULL OR " + tableNameEscaped + "." + tagColumnNameEscaped + " != ?)").addWhereParameters(statement.getTag());

        return CustomSqlGenerator.generateSql(database,
                createTableAsSelectStatement,
                insertAsSelectStatement,
                new DropTableStatement(catalogName, schemaName, tableName, false),
                new RenameTableStatement(catalogName, schemaName, tempTable, tableName));
    }
}
