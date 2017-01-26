package liquibase.ext.metastore.utils;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.SqlStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CustomSqlGenerator {

    public static Sql[] generateSql(Database database, List<SqlStatement> statements) {
        List<Sql> sqls = new ArrayList<Sql>();
        SqlGeneratorFactory generatorFactory = SqlGeneratorFactory.getInstance();
        for (SqlStatement statement : statements) {
            sqls.addAll(Arrays.asList(generatorFactory.generateSql(statement, database)));
        }
        return sqls.toArray(new Sql[sqls.size()]);
    }

    public static Sql[] generateSql(Database database, SqlStatement... statements) {
        List<Sql> sqls = new ArrayList<Sql>();
        SqlGeneratorFactory generatorFactory = SqlGeneratorFactory.getInstance();
        for (SqlStatement statement : statements) {
            sqls.addAll(Arrays.asList(generatorFactory.generateSql(statement, database)));
        }
        return sqls.toArray(new Sql[sqls.size()]);
    }
}
