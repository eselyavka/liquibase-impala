package liquibase.ext.metastore.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;
import liquibase.ext.metastore.statement.InsertAsSelectStatement;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;

import static liquibase.util.SqlUtil.replacePredicatePlaceholders;

public class InsertAsSelectGenerator extends AbstractSqlGenerator<InsertAsSelectStatement> {

    @Override
    public boolean supports(InsertAsSelectStatement statement, Database database) {
        return database instanceof HiveMetastoreDatabase && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(InsertAsSelectStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors errors = new ValidationErrors();
        errors.checkRequiredField("tableName", statement.getTableName());
        errors.checkRequiredField("dstTableName", statement.getDestTableName());
        return errors;
    }

    @Override
    public Sql[] generateSql(InsertAsSelectStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String catalogName = statement.getCatalogName();
        String schemaName = statement.getSchemaName();
        String tableName = statement.getTableName();
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(database.escapeTableName(statement.getCatalogName(),
                statement.getSchemaName(), statement.getDestTableName())).append(" SELECT ");
        generateColumnNames(sql, statement, database);
        sql.append(" FROM ").append(database.escapeTableName(catalogName, schemaName, tableName));
        if (statement.getWhereCondition() != null) {
            sql.append(" WHERE ").append(replacePredicatePlaceholders(database, statement.getWhereCondition(), statement.getWhereColumnNames(), statement.getWhereParameters()));
        }
        return new Sql[]{new UnparsedSql(sql.toString(), fetchAffectedTable(statement))};
    }

    private void generateColumnNames(StringBuilder sql, InsertAsSelectStatement statement, Database database) {
        String catalogName = statement.getCatalogName();
        String schemaName = statement.getSchemaName();
        String tableName = statement.getTableName();
        for (String column : statement.getColumnNames()) {
            if (column.startsWith("'") && column.endsWith("'")) {
                sql.append(column).append(", ");
            } else {
                sql.append(database.escapeColumnName(catalogName, schemaName, tableName, column)).append(", ");
            }
        }
        sql.deleteCharAt(sql.lastIndexOf(" "));
        int lastComma = sql.lastIndexOf(",");
        if (lastComma >= 0) {
            sql.deleteCharAt(lastComma);
        }
    }

    private Relation fetchAffectedTable(InsertAsSelectStatement statement) {
        return new Table().setName(statement.getDestTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }
}
