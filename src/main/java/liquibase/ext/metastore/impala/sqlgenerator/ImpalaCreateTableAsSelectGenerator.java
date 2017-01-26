package liquibase.ext.metastore.impala.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;
import liquibase.ext.metastore.impala.statement.CreateTableAsSelectStatement;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;

import static liquibase.util.SqlUtil.replacePredicatePlaceholders;

public class ImpalaCreateTableAsSelectGenerator extends AbstractSqlGenerator<CreateTableAsSelectStatement> {

    @Override
    public boolean supports(CreateTableAsSelectStatement statement, Database database) {
        return database instanceof HiveMetastoreDatabase && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(CreateTableAsSelectStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors errors = new ValidationErrors();
        errors.checkRequiredField("tableName", statement.getTableName());
        return errors;
    }

    @Override
    public Sql[] generateSql(CreateTableAsSelectStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder sql = new StringBuilder("CREATE TABLE ").append(database.escapeTableName(statement.getCatalogName(),
                statement.getSchemaName(), statement.getDestTableName())).append(" AS SELECT ");
        generateColumnNames(sql, statement, database);
        sql.append(" FROM ").append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()));
        if (statement.getWhereCondition() != null) {
            sql.append(" WHERE ").append(replacePredicatePlaceholders(database, statement.getWhereCondition(), statement.getWhereColumnNames(), statement.getWhereParameters()));
        }
        return new Sql[]{new UnparsedSql(sql.toString(), fetchAffectedTable(statement))};
    }

    private void generateColumnNames(StringBuilder sql, CreateTableAsSelectStatement statement, Database database) {
        for (String column : statement.getColumnNames()) {
            sql.append(database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), column)).append(", ");
        }
        sql.deleteCharAt(sql.lastIndexOf(" "));
        int lastComma = sql.lastIndexOf(",");
        if (lastComma >= 0) {
            sql.deleteCharAt(lastComma);
        }
    }

    private Relation fetchAffectedTable(CreateTableAsSelectStatement statement) {
        return new Table().setName(statement.getDestTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }
}
