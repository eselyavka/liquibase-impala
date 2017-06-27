package liquibase.ext.metastore.hive.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.metastore.hive.database.HiveDatabase;
import liquibase.ext.metastore.sqlgenerator.TruncateGenerator;
import liquibase.ext.metastore.statement.TruncateTableStatement;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;

public class HiveTruncateGenerator extends TruncateGenerator {

    @Override
    public boolean supports(TruncateTableStatement statement, Database database) {
        return database instanceof HiveDatabase && super.supports(statement, database);
    }

    @Override
    public Sql[] generateSql(TruncateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String sql = "TRUNCATE TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName());
        return new Sql[]{new UnparsedSql(sql, fetchAffectedTable(statement))};
    }
}

