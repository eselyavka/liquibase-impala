package liquibase.ext.metastore.impala.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.impala.database.ImpalaDatabase;
import liquibase.ext.metastore.impala.statement.RefreshTableStatement;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;

public class ImpalaRefreshTableGenerator extends AbstractSqlGenerator<RefreshTableStatement> {

    @Override
    public boolean supports(RefreshTableStatement statement, Database database) {
        return database instanceof ImpalaDatabase && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(RefreshTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors errors = new ValidationErrors();
        errors.checkRequiredField("tableName", statement.getTableName());
        return errors;
    }

    @Override
    public Sql[] generateSql(RefreshTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String sql = "REFRESH " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName());
        return new Sql[]{new UnparsedSql(sql, fetchAffectedTable(statement))};
    }

    private Relation fetchAffectedTable(RefreshTableStatement statement) {
        return new Table().setName(statement.getTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }
}
