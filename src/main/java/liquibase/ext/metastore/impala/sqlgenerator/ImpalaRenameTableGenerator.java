package liquibase.ext.metastore.impala.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.core.RenameTableStatement;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;

public class ImpalaRenameTableGenerator extends AbstractSqlGenerator<RenameTableStatement> {

    @Override
    public boolean supports(RenameTableStatement statement, Database database) {
        return database instanceof HiveMetastoreDatabase && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(RenameTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors errors = new ValidationErrors();
        errors.checkRequiredField("newTableName", statement.getNewTableName());
        errors.checkRequiredField("oldTableName", statement.getOldTableName());
        return errors;
    }

    @Override
    public Sql[] generateSql(RenameTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String sql = "ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(),
                statement.getOldTableName()) + " RENAME TO " + database.escapeObjectName(statement.getNewTableName(), Table.class);
        return new Sql[]{
                new UnparsedSql(sql,
                        fetchAffectedOldTable(statement),
                        fetchAffectedNewTable(statement)
                )
        };
    }

    private Relation fetchAffectedNewTable(RenameTableStatement statement) {
        return new Table().setName(statement.getNewTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }

    private Relation fetchAffectedOldTable(RenameTableStatement statement) {
        return new Table().setName(statement.getOldTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }
}
