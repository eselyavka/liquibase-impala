package liquibase.ext.metastore.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;
import liquibase.ext.metastore.statement.TruncateTableStatement;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;

@SuppressWarnings("unused")
public class TruncateGenerator extends AbstractSqlGenerator<TruncateTableStatement> {
    @Override
    public boolean supports(TruncateTableStatement statement, Database database) {
        return database instanceof HiveMetastoreDatabase && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(TruncateTableStatement truncateStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        final ValidationErrors errors = new ValidationErrors();
        errors.checkRequiredField("tableName", truncateStatement.getTableName());
        return errors;
    }

    @Override
    public Sql[] generateSql(TruncateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        final String sql = "TRUNCATE TABLE " + database.escapeTableName(statement.getCatalogName(),
                statement.getSchemaName(), statement.getTableName());
        return new Sql[]{new UnparsedSql(sql, fetchAffectedTable(statement))};
    }

    private Relation fetchAffectedTable(TruncateTableStatement statement) {
        return new Table().setName(statement.getTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }
}
