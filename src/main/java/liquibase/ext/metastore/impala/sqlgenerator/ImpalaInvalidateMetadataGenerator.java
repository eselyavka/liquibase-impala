package liquibase.ext.metastore.impala.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.impala.database.ImpalaDatabase;
import liquibase.ext.metastore.impala.statement.InvalidateMetadataStatement;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;

public class ImpalaInvalidateMetadataGenerator extends AbstractSqlGenerator<InvalidateMetadataStatement> {

    @Override
    public boolean supports(InvalidateMetadataStatement statement, Database database) {
        return database instanceof ImpalaDatabase && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(InvalidateMetadataStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors errors = new ValidationErrors();
        errors.checkRequiredField("tableName", statement.getTableName());
        return errors;
    }

    @Override
    public Sql[] generateSql(InvalidateMetadataStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String catalogName = statement.getCatalogName();
        String schemaName = statement.getSchemaName();
        String tableName = statement.getTableName();
        String sql = "INVALIDATE METADATA " + database.escapeTableName(catalogName, schemaName, tableName);
        return new Sql[]{new UnparsedSql(sql, fetchAffectedTable(statement))};
    }

    private Relation fetchAffectedTable(InvalidateMetadataStatement statement) {
        String catalogName = statement.getCatalogName();
        String schemaName = statement.getSchemaName();
        String tableName = statement.getTableName();
        return new Table().setName(tableName).setSchema(catalogName, schemaName);
    }
}
