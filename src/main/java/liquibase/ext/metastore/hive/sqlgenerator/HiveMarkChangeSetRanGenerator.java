package liquibase.ext.metastore.hive.sqlgenerator;

import liquibase.change.Change;
import liquibase.change.core.TagDatabaseChange;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.hive.database.HiveDatabase;
import liquibase.ext.metastore.hive.statement.HiveInsertStatement;
import liquibase.ext.metastore.utils.DateTimeUtils;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.MarkChangeSetRanStatement;
import liquibase.util.LiquibaseUtil;
import liquibase.util.StringUtils;

public class HiveMarkChangeSetRanGenerator extends AbstractSqlGenerator<MarkChangeSetRanStatement> {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(MarkChangeSetRanStatement statement, Database database) {
        return database instanceof HiveDatabase && super.supports(statement, database);
    }

    @Override
    public ValidationErrors validate(MarkChangeSetRanStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("changeSet", statement.getChangeSet());

        return validationErrors;
    }

    @Override
    public Sql[] generateSql(MarkChangeSetRanStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ChangeSet changeSet = statement.getChangeSet();

        SqlStatement runStatement;
        try {
            if (statement.getExecType().equals(ChangeSet.ExecType.FAILED) || statement.getExecType().equals(ChangeSet.ExecType.SKIPPED)) {
                return new Sql[0]; //don't mark
            }

            String tag = null;
            for (Change change : changeSet.getChanges()) {
                if (change instanceof TagDatabaseChange) {
                    TagDatabaseChange tagChange = (TagDatabaseChange) change;
                    tag = tagChange.getTag();
                }
            }

            runStatement = new HiveInsertStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName())
                    .addColumnValue(changeSet.getId())
                    .addColumnValue(changeSet.getAuthor())
                    .addColumnValue(changeSet.getFilePath())
                    .addColumnValue(DateTimeUtils.getCurrentTS("yyyy-MM-dd HH:mm:ss"))
                    .addColumnValue(ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database).getNextSequenceValue())
                    .addColumnValue(statement.getExecType().value)
                    .addColumnValue(changeSet.generateCheckSum().toString())
                    .addColumnValue(changeSet.getDescription())
                    .addColumnValue(StringUtils.trimToEmpty(changeSet.getComments()))
                    .addColumnValue(tag == null ? "NULL" : tag)
                    .addColumnValue(ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database).getDeploymentId())
                    .addColumnValue(changeSet.getContexts() == null || changeSet.getContexts().isEmpty() ? null : changeSet.getContexts().toString())
                    .addColumnValue(changeSet.getLabels() == null || changeSet.getLabels().isEmpty() ? null : changeSet.getLabels().toString())
                    .addColumnValue(LiquibaseUtil.getBuildVersion());
        } catch (LiquibaseException e) {
            throw new UnexpectedLiquibaseException(e);
        }

        return SqlGeneratorFactory.getInstance().generateSql(runStatement, database);
    }
}
