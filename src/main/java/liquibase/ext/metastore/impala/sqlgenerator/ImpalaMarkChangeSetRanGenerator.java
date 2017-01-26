package liquibase.ext.metastore.impala.sqlgenerator;

import liquibase.change.Change;
import liquibase.change.core.TagDatabaseChange;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.impala.database.ImpalaDatabase;
import liquibase.ext.metastore.impala.statement.CreateTableAsSelectStatement;
import liquibase.ext.metastore.utils.UserSessionSettings;
import liquibase.sql.Sql;
import liquibase.ext.metastore.utils.CustomSqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.DropTableStatement;
import liquibase.statement.core.InsertStatement;
import liquibase.statement.core.MarkChangeSetRanStatement;
import liquibase.statement.core.RenameTableStatement;
import liquibase.structure.core.Column;
import liquibase.util.LiquibaseUtil;
import liquibase.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ImpalaMarkChangeSetRanGenerator extends AbstractSqlGenerator<MarkChangeSetRanStatement> {

    @Override
    public boolean supports(MarkChangeSetRanStatement statement, Database database) {
        return database instanceof ImpalaDatabase && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(MarkChangeSetRanStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors errors = new ValidationErrors();
        errors.checkRequiredField("changeSet", statement.getChangeSet());
        return errors;
    }

    @Override
    public Sql[] generateSql(MarkChangeSetRanStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String dateValue = database.getCurrentDateTimeFunction();

        ChangeSet changeSet = statement.getChangeSet();

        List<SqlStatement> statements = new ArrayList<SqlStatement>();

        try {
            if (statement.getExecType().equals(ChangeSet.ExecType.FAILED) || statement.getExecType().equals(ChangeSet.ExecType.SKIPPED)) {
                return new Sql[0];
            }

            String tag = null;
            for (Change change : changeSet.getChanges()) {
                if (change instanceof TagDatabaseChange) {
                    TagDatabaseChange tagChange = (TagDatabaseChange) change;
                    tag = tagChange.getTag();
                }
            }

            String catalogName = database.getLiquibaseCatalogName();
            String schemaName = database.getDefaultSchemaName();
            String tableName = database.getDatabaseChangeLogTableName();
            String tempTable = UUID.randomUUID().toString().replaceAll("-", "");

            statements.add(UserSessionSettings.syncDdlStart());
            if (statement.getExecType().ranBefore) {
                CreateTableAsSelectStatement createTableAsSelectStatement = new CreateTableAsSelectStatement(catalogName, schemaName, tableName, tempTable)
                        .addColumnNames("ID", "AUTHOR", "FILENAME", "DATEEXECUTED", "ORDEREXECUTED", "EXECTYPE", "MD5SUM", "DESCRIPTION", "COMMENTS", "TAG", "LIQUIBASE", "CONTEXTS", "LABELS", "DEPLOYMENT_ID")
                        .setWhereCondition(" NOT (" + database.escapeObjectName("ID", Column.class) + " = ? " +
                                "AND " + database.escapeObjectName("AUTHOR", Column.class) + " = ? " +
                                "AND " + database.escapeObjectName("FILENAME", Column.class) + " = ?)")
                        .addWhereParameters(changeSet.getId(), changeSet.getAuthor(), changeSet.getFilePath());

                statements.add(createTableAsSelectStatement);
                InsertStatement insertStatement = new InsertStatement(catalogName, schemaName, tempTable)
                        .addColumnValue("ID", changeSet.getId())
                        .addColumnValue("AUTHOR", changeSet.getAuthor())
                        .addColumnValue("FILENAME", changeSet.getFilePath())
                        .addColumnValue("DATEEXECUTED", new DatabaseFunction(dateValue))
                        .addColumnValue("ORDEREXECUTED", ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database).getNextSequenceValue())
                        .addColumnValue("MD5SUM", changeSet.generateCheckSum().toString())
                        .addColumnValue("DESCRIPTION", limitSize(changeSet.getDescription()))
                        .addColumnValue("COMMENTS", limitSize(StringUtils.trimToEmpty(changeSet.getComments())))
                        .addColumnValue("EXECTYPE", statement.getExecType().value)
                        .addColumnValue("CONTEXTS", changeSet.getContexts() == null || changeSet.getContexts().isEmpty() ? null : changeSet.getContexts().toString())
                        .addColumnValue("LABELS", changeSet.getLabels() == null || changeSet.getLabels().isEmpty() ? null : changeSet.getLabels().toString())
                        .addColumnValue("LIQUIBASE", LiquibaseUtil.getBuildVersion())
                        .addColumnValue("DEPLOYMENT_ID", ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database).getDeploymentId());


                if (tag != null) {
                    insertStatement.addColumnValue("TAG", tag);
                }
                statements.add(insertStatement);
                statements.add(new DropTableStatement(catalogName, schemaName, tableName, false));
                statements.add(new RenameTableStatement(catalogName, schemaName, tempTable, tableName));
            } else {
                InsertStatement insertStatement = new InsertStatement(catalogName, schemaName, tableName)
                        .addColumnValue("ID", changeSet.getId())
                        .addColumnValue("AUTHOR", changeSet.getAuthor())
                        .addColumnValue("FILENAME", changeSet.getFilePath())
                        .addColumnValue("DATEEXECUTED", new DatabaseFunction(dateValue))
                        .addColumnValue("ORDEREXECUTED", ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database).getNextSequenceValue())
                        .addColumnValue("MD5SUM", changeSet.generateCheckSum().toString())
                        .addColumnValue("DESCRIPTION", limitSize(changeSet.getDescription()))
                        .addColumnValue("COMMENTS", limitSize(StringUtils.trimToEmpty(changeSet.getComments())))
                        .addColumnValue("EXECTYPE", statement.getExecType().value)
                        .addColumnValue("CONTEXTS", changeSet.getContexts() == null || changeSet.getContexts().isEmpty() ? null : changeSet.getContexts().toString())
                        .addColumnValue("LABELS", changeSet.getLabels() == null || changeSet.getLabels().isEmpty() ? null : changeSet.getLabels().toString())
                        .addColumnValue("LIQUIBASE", LiquibaseUtil.getBuildVersion())
                        .addColumnValue("DEPLOYMENT_ID", ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database).getDeploymentId());

                if (tag != null) {
                    insertStatement.addColumnValue("TAG", tag);
                }
                statements.add(insertStatement);
            }
            statements.add(UserSessionSettings.syncDdlStop());
        } catch (LiquibaseException e) {
            throw new UnexpectedLiquibaseException(e);
        }

        return CustomSqlGenerator.generateSql(database, statements);
    }

    private String limitSize(String string) {
        int maxLength = 250;
        if (string.length() > maxLength) {
            return string.substring(0, maxLength - 3) + "...";
        }
        return string;
    }
}
