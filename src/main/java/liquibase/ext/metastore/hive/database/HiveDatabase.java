package liquibase.ext.metastore.hive.database;

import liquibase.ext.metastore.database.HiveMetastoreDatabase;

import java.util.Arrays;

public class HiveDatabase extends HiveMetastoreDatabase {

    public HiveDatabase() {
        super("Apache Hive", "jdbc:hive2", "com.cloudera.hive.jdbc41.HS2Driver");
        quotingStartCharacter = "`";
        quotingEndCharacter = "`";
    }

    @Override
    public Integer getDefaultPort() {
        return 10000;
    }

    @Override
    public void setReservedWords() {
        addReservedWords(Arrays.asList("ALL", "ALTER", "AND", "ARRAY", "AS", "AUTHORIZATION", "BETWEEN",
                "BIGINT", "BINARY", "BOOLEAN", "BOTH", "BY", "CASE", "CAST", "CHAR",
                "COLUMN", "CONF", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE",
                "CURRENT_TIMESTAMP", "CURSOR", "DATABASE", "DATE", "DECIMAL", "DELETE",
                "DESCRIBE", "DISTINCT", "DOUBLE", "DROP", "ELSE", "END", "EXCHANGE", "EXISTS",
                "EXTENDED", "EXTERNAL", "FALSE", "FETCH", "FLOAT", "FOLLOWING", "FOR", "FROM",
                "FULL", "FUNCTION", "GRANT", "GROUP", "GROUPING", "HAVING", "IF", "IMPORT", "IN",
                "INNER", "INSERT", "INT", "INTERSECT", "INTERVAL", "INTO", "IS", "JOIN", "LATERAL",
                "LEFT", "LESS", "LIKE", "LOCAL", "MACRO", "MAP", "MORE", "NONE", "NOT", "NULL", "OF",
                "ON", "OR", "ORDER", "OUT", "OUTER", "OVER", "PARTIALSCAN", "PARTITION", "PERCENT",
                "PRECEDING", "PRESERVE", "PROCEDURE", "RANGE", "READS", "REDUCE", "REVOKE", "RIGHT",
                "ROLLUP", "ROW", "ROWS", "SELECT", "SET", "SMALLINT", "TABLE", "TABLESAMPLE", "THEN",
                "TIMESTAMP", "TO", "TRANSFORM", "TRIGGER", "TRUE", "TRUNCATE", "UNBOUNDED", "UNION",
                "UNIQUEJOIN", "UPDATE", "USER", "USING", "UTC_TMESTAMP", "VALUES", "VARCHAR", "WHEN",
                "WHERE", "WINDOW", "WITH", "COMMIT", "ONLY", "REGEXP", "RLIKE", "ROLLBACK", "START",
                "CACHE", "CONSTRAINT", "FOREIGN", "PRIMARY", "REFERENCES", "DAYOFWEEK", "EXTRACT",
                "FLOOR", "INTEGER", "PRECISION", "VIEWS"));
    }

    @Override
    public String getCurrentDateTimeFunction() {
        return "CURRENT_TIMESTAMP()";
    }

    @Override
    protected String getConnectionSchemaName() {
        String tokens[] = super.getConnection().getURL().split("\\/");
        String dbName = tokens[tokens.length - 1].split(";")[0];
        String schema = getSchemaDatabaseSpecific("SHOW SCHEMAS LIKE '" + dbName + "'");
        return schema == null ? "default" : schema;
    }
}
