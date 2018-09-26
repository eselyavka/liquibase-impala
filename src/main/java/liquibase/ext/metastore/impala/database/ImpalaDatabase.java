package liquibase.ext.metastore.impala.database;

import liquibase.configuration.LiquibaseConfiguration;
import liquibase.exception.LiquibaseException;
import liquibase.ext.metastore.configuration.HiveMetastoreConfiguration;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;
import liquibase.ext.metastore.utils.UserSessionSettings;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.statement.SqlStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ImpalaDatabase extends HiveMetastoreDatabase {
    private static Boolean syncDdl = LiquibaseConfiguration.getInstance().getConfiguration(HiveMetastoreConfiguration.class).getSyncDDL();

    public ImpalaDatabase() {
        super("Impala", "jdbc:impala", "com.cloudera.impala.jdbc41.Driver");
    }

    @Override
    protected String getQuotingStartCharacter() {
        return "`";
    }

    @Override
    protected String getQuotingEndCharacter() {
        return "`";
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

    @Override
    public void execute(SqlStatement[] statements, List<SqlVisitor> sqlVisitors) throws LiquibaseException {
        if (syncDdl) {
            List<SqlStatement> sqlStatementList = new ArrayList<SqlStatement>();
            sqlStatementList.add(UserSessionSettings.syncDdlStart());
            sqlStatementList.addAll(Arrays.asList(statements));
            sqlStatementList.add(UserSessionSettings.syncDdlStop());
            SqlStatement[] wrappedStatements = new SqlStatement[sqlStatementList.size()];
            sqlStatementList.toArray(wrappedStatements);
            super.execute(wrappedStatements, sqlVisitors);
        } else {
            super.execute(statements, sqlVisitors);
        }
    }
}

