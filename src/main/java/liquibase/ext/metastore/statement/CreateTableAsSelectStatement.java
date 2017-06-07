package liquibase.ext.metastore.statement;

import liquibase.statement.AbstractSqlStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CreateTableAsSelectStatement extends AbstractSqlStatement {
    private String catalogName;
    private String schemaName;
    private String tableName;
    private String destTableName;
    private String whereCondition;
    private List<String> whereColumnNames = new ArrayList<String>();
    private List<Object> whereParameters = new ArrayList<Object>();
    private List<String> columnNames = new ArrayList<String>();

    public CreateTableAsSelectStatement(String catalogName, String schemaName, String tableName, String destTableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.destTableName = destTableName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDestTableName() {
        return destTableName;
    }

    public String getWhereCondition() {
        return whereCondition;
    }

    public List<Object> getWhereParameters() {
        return whereParameters;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getWhereColumnNames() {
        return whereColumnNames;
    }

    public CreateTableAsSelectStatement setWhereCondition(String whereCondition) {
        this.whereCondition = whereCondition;
        return this;
    }

    public CreateTableAsSelectStatement addWhereParameters(Object... value) {
        this.whereParameters.addAll(Arrays.asList(value));
        return this;
    }

    public CreateTableAsSelectStatement addWhereColumnNames(String value) {
        this.whereColumnNames.add(value);
        return this;
    }

    public CreateTableAsSelectStatement addColumnNames(String value) {
        this.columnNames.add(value);
        return this;
    }

    public CreateTableAsSelectStatement addColumnNames(String... value) {
        this.columnNames.addAll(Arrays.asList(value));
        return this;
    }
}
