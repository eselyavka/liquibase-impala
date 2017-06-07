package liquibase.ext.metastore.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;
import liquibase.ext.metastore.statement.SetStatement;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.structure.core.Schema;

public class SetGenerator extends AbstractSqlGenerator<SetStatement> {

    @Override
    public boolean supports(SetStatement statement, Database database) {
        return database instanceof HiveMetastoreDatabase && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public ValidationErrors validate(SetStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors errors = new ValidationErrors();
        errors.checkRequiredField("queryOption", statement.getQueryOption());
        errors.checkRequiredField("optionValue", statement.getOptionValue());
        return errors;
    }

    @Override
    public Sql[] generateSql(SetStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String sql = "SET " + statement.getQueryOption() + "=" + statement.getOptionValue();
        return new Sql[]{new UnparsedSql(sql, new Schema().getName())};
    }
}
