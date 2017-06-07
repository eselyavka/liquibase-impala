package liquibase.ext.metastore.hive.statement;

import liquibase.statement.AbstractSqlStatement;

import java.util.ArrayList;
import java.util.List;

public class HiveInsertStatement extends AbstractSqlStatement {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private List<Object> columnValues = new ArrayList<Object>();

    public HiveInsertStatement(String catalogName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
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

    public List<Object> getColumnValues() {
        return columnValues;
    }

    public HiveInsertStatement addColumnValue(Object newValue) {
        columnValues.add(newValue);
        return this;
    }
}
