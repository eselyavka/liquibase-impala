package liquibase.ext.metastore.impala.database;

import liquibase.ext.metastore.database.HiveMetastoreDatabase;

import java.util.Arrays;

public class ImpalaDatabase extends HiveMetastoreDatabase {

    public ImpalaDatabase() {
        super("Impala", "jdbc:impala", "com.cloudera.impala.jdbc41.Driver");
        quotingStartCharacter = "`";
        quotingEndCharacter = "`";
    }

    @Override
    public Integer getDefaultPort() {
        return 21050;
    }

    public void setReservedWords() {
        addReservedWords(Arrays.asList("API_VERSION", "AS", "ASC", "AVRO", "BETWEEN", "BIGINT", "BINARY",
                "BOOLEAN", "BY", "CACHED", "CASE", "CAST", "CHANGE", "CHAR", "CLASS",
                "CLOSE_FN", "COLUMN", "COLUMNS", "COMMENT", "COMPUTE", "CREATE", "CROSS",
                "CURRENT", "DATA", "DATABASE", "DATABASES", "DATE", "DATETIME", "DECIMAL",
                "DELIMITED", "DESC", "DESCRIBE", "DISTINCT", "DIV", "DOUBLE", "DROP",
                "ELSE", "END", "ESCAPED", "EXISTS", "EXPLAIN", "EXTERNAL", "FALSE",
                "FIELDS", "FILEFORMAT", "FINALIZE_FN", "FIRST", "FLOAT", "FOLLOWING",
                "FOR", "FORMAT", "FORMATTED", "FROM", "FULL", "FUNCTION", "FUNCTIONS",
                "GRANT", "GROUP", "HAVING", "IF", "IN", "INCREMENTAL", "INIT_FN", "INNER",
                "INPATH", "INSERT", "INT", "INTEGER", "INTERMEDIATE", "INTERVAL", "INTO",
                "INVALIDATE", "IS", "JOIN", "LAST", "LEFT", "LIKE", "LIMIT", "LINES",
                "LOAD", "LOCATION", "MERGE_FN", "METADATA", "NOT", "NULL", "NULLS",
                "OFFSET", "ON", "OR", "ORDER", "OUTER", "OVER", "OVERWRITE", "PARQUET",
                "PARQUETFILE", "PARTITION", "PARTITIONED", "PARTITIONS", "PRECEDING",
                "PREPARE_FN", "PRODUCED", "RANGE", "RCFILE", "REAL", "REFRESH", "REGEXP",
                "RENAME", "REPLACE", "RETURNS", "REVOKE", "RIGHT", "RLIKE", "ROLE", "ROLES",
                "ROW", "ROWS", "SCHEMA", "SCHEMAS", "SELECT", "SEMI", "SEQUENCEFILE",
                "SERDEPROPERTIES", "SERIALIZE_FN", "SET", "SHOW", "SMALLINT", "STATS",
                "STORED", "STRAIGHT_JOIN", "STRING", "SYMBOL", "TABLE", "TABLES",
                "TBLPROPERTIES", "TERMINATED", "TEXTFILE", "THEN", "TIMESTAMP", "TINYINT",
                "TO", "TRUE", "UNBOUNDED", "UNCACHED", "UNION", "UPDATE_FN", "USE",
                "USING", "VALUES", "VARCHAR", "VIEW", "WHEN", "WHERE", "WITH",
                "ADD", "AGGREGATE", "ALL", "ALTER", "ANALYTIC", "AND", "ANTI"));
    }

    @Override
    public String getCurrentDateTimeFunction() {
        return "NOW()";
    }

    @Override
    protected String getConnectionSchemaName() {
        String tokens[] = super.getConnection().getURL().split("\\/");
        String dbName = tokens[tokens.length - 1].split(";")[0];
        String schema = getSchemaDatabaseSpecific("SHOW DATABASES '" + dbName + "'");
        return schema == null ? "default" : schema;
    }
}

