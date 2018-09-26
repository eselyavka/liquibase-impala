package liquibase.ext.metastore.hive.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.hive.database.HiveDatabase;
import liquibase.logging.LogService;
import liquibase.logging.Logger;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.core.DropColumnStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.Table;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveDropColumnGenerator extends AbstractSqlGenerator<DropColumnStatement> {
    private static final Logger LOG = LogService.getLog(HiveDropColumnGenerator.class);

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(DropColumnStatement dropColumnStatement, Database database) {
        return database instanceof HiveDatabase && super.supports(dropColumnStatement, database);
    }

    @Override
    public ValidationErrors validate(DropColumnStatement dropColumnStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        if (dropColumnStatement.isMultiple()) {
            ValidationErrors validationErrors = new ValidationErrors();
            DropColumnStatement firstColumn = dropColumnStatement.getColumns().get(0);

            for (DropColumnStatement drop : dropColumnStatement.getColumns()) {
                validationErrors.addAll(validateSingleColumn(drop));
                if (drop.getTableName() != null && !drop.getTableName().equals(firstColumn.getTableName())) {
                    validationErrors.addError("All columns must be targeted at the same table");
                }
                if (drop.isMultiple()) {
                    validationErrors.addError("Nested multiple drop column statements are not supported");
                }
            }
            return validationErrors;
        } else {
            return validateSingleColumn(dropColumnStatement);
        }
    }

    private ValidationErrors validateSingleColumn(DropColumnStatement dropColumnStatement) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", dropColumnStatement.getTableName());
        validationErrors.checkRequiredField("columnName", dropColumnStatement.getColumnName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DropColumnStatement dropColumnStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        Map<String, String> columnsPreserved = null;
        columnsPreserved = columnsMap((HiveDatabase) database, dropColumnStatement);
        return generateMultipleColumnSql(dropColumnStatement, database, columnsPreserved);
    }

    private Map<String, String> columnsMap(HiveDatabase database, DropColumnStatement dropColumnStatement) {
        String query = "DESCRIBE " + database.escapeObjectName(dropColumnStatement.getTableName(), Table.class);
        Map mapOfColNameDataTypes = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            mapOfColNameDataTypes = new HashMap<String, String>();
            statement = database.getStatement();
            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                String colName = resultSet.getString("col_name");
                String dataType = resultSet.getString("data_type");
                mapOfColNameDataTypes.put(colName.toUpperCase(), dataType);
            }
        } catch (Exception e) {
            LOG.warning("can't perform query", e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    LOG.warning("Can't close cursor");
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOG.warning("Can't close cursor");
                }
            }
        }
        return mapOfColNameDataTypes;
    }

    private Sql[] generateMultipleColumnSql(DropColumnStatement dropColumnStatement, Database database, Map<String, String> columnsPreserved) {
        if (columnsPreserved == null) {
            throw new UnexpectedLiquibaseException("no columns to preserve");
        }
        List<Sql> result = new ArrayList<Sql>();
        Map<String, String> columnsPreservedCopy = new HashMap<String, String>(columnsPreserved);
        String alterTable;
        List<DropColumnStatement> columns = null;

        if (dropColumnStatement.isMultiple()) {
            columns = dropColumnStatement.getColumns();
            for (DropColumnStatement statement : columns) {
                columnsPreservedCopy.remove(statement.getColumnName());
            }
            alterTable = "ALTER TABLE " + database.escapeTableName(columns.get(0).getCatalogName(), columns.get(0).getSchemaName(), columns.get(0).getTableName()) + " REPLACE COLUMNS (";
        } else {
            columnsPreservedCopy.remove(dropColumnStatement.getColumnName());
            alterTable = "ALTER TABLE " + database.escapeTableName(dropColumnStatement.getCatalogName(), dropColumnStatement.getSchemaName(), dropColumnStatement.getTableName()) + " REPLACE COLUMNS (";
        }

        int i = 0;
        for (String columnName : columnsPreservedCopy.keySet()) {
            alterTable += database.escapeObjectName(columnName, Column.class) + " " + columnsPreservedCopy.get(columnName);
            if (i < columnsPreservedCopy.size() - 1) {
                alterTable += ",";
            } else {
                alterTable += ")";
            }
            i++;
        }

        if (dropColumnStatement.isMultiple()) {
            result.add(new UnparsedSql(alterTable, getAffectedColumns(columns)));
        } else {
            result.add(new UnparsedSql(alterTable, getAffectedColumn(dropColumnStatement)));
        }
        return result.toArray(new Sql[result.size()]);
    }

    private Column[] getAffectedColumns(List<DropColumnStatement> columns) {
        List<Column> affected = new ArrayList<Column>();
        for (DropColumnStatement column : columns) {
            affected.add(getAffectedColumn(column));
        }
        return affected.toArray(new Column[affected.size()]);
    }

    private Column getAffectedColumn(DropColumnStatement statement) {
        return new Column().setName(statement.getColumnName()).setRelation(new Table().setName(statement.getTableName()).setSchema(statement.getCatalogName(), statement.getSchemaName()));
    }
}
