package liquibase.ext.metastore.impala.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.impala.database.ImpalaDatabase;
import liquibase.ext.metastore.impala.statement.TruncateTableStatement;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;

public class ImpalaTruncateGenerator extends AbstractSqlGenerator<TruncateTableStatement> {

    @Override
    public boolean supports(TruncateTableStatement statement, Database database) {
        return database instanceof ImpalaDatabase && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(TruncateTableStatement truncateStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors errors = new ValidationErrors();
        errors.checkRequiredField("tableName", truncateStatement.getTableName());
        return errors;
    }

    private Relation fetchAffectedTable(TruncateTableStatement statement) {
        return new Table().setName(statement.getTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }

    @Override
    public Sql[] generateSql(TruncateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String sql = "TRUNCATE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName());
        return new Sql[]{new UnparsedSql(sql, fetchAffectedTable(statement))};
    }
}
