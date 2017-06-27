package liquibase.ext.metastore.hive.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.hive.database.HiveDatabase;
import liquibase.ext.metastore.hive.statement.HiveInsertStatement;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;

import java.util.Date;

public class HiveInsertGenerator extends AbstractSqlGenerator<HiveInsertStatement> {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(HiveInsertStatement statement, Database database) {
        return database instanceof HiveDatabase && super.supports(statement, database);
    }

    @Override
    public ValidationErrors validate(HiveInsertStatement insertStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", insertStatement.getTableName());
        validationErrors.checkRequiredField("columns", insertStatement.getColumnValues());

        return validationErrors;
    }

    @Override
    public Sql[] generateSql(HiveInsertStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        StringBuilder sql = new StringBuilder();

        generateHeader(sql, statement, database);
        generateValues(sql, statement, database);

        return new Sql[]{new UnparsedSql(sql.toString(), getAffectedTable(statement))};
    }

    private void generateHeader(StringBuilder sql, HiveInsertStatement statement, Database database) {
        sql.append("INSERT INTO ")
                .append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()))
                .append(" VALUES ");
    }

    private void generateValues(StringBuilder sql, HiveInsertStatement statement, Database database) {
        sql.append("(");

        for (Object newValue : statement.getColumnValues()) {
            if (newValue == null || newValue.toString().equalsIgnoreCase("NULL")) {
                sql.append("NULL");
            } else if (newValue instanceof String && !looksLikeFunctionCall(((String) newValue), database)) {
                sql.append(DataTypeFactory.getInstance().fromObject(newValue, database).objectToSql(newValue, database));
            } else if (newValue instanceof Date) {
                sql.append(database.getDateLiteral(((Date) newValue)));
            } else if (newValue instanceof Boolean) {
                if (((Boolean) newValue)) {
                    sql.append(DataTypeFactory.getInstance().getTrueBooleanValue(database));
                } else {
                    sql.append(DataTypeFactory.getInstance().getFalseBooleanValue(database));
                }
            } else if (newValue instanceof DatabaseFunction) {
                sql.append(database.generateDatabaseFunctionValue((DatabaseFunction) newValue));
            } else {
                sql.append(newValue);
            }
            sql.append(", ");
        }

        sql.deleteCharAt(sql.lastIndexOf(" "));
        int lastComma = sql.lastIndexOf(",");
        if (lastComma >= 0) {
            sql.deleteCharAt(lastComma);
        }

        sql.append(")");
    }

    private Relation getAffectedTable(HiveInsertStatement statement) {
        return new Table().setName(statement.getTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName());
    }
}
