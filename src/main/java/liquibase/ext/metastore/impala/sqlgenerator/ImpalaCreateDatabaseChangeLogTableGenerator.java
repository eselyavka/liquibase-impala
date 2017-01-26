package liquibase.ext.metastore.impala.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;
import liquibase.ext.metastore.utils.CustomSqlGenerator;
import liquibase.ext.metastore.utils.UserSessionSettings;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;
import liquibase.statement.core.CreateTableStatement;

public class ImpalaCreateDatabaseChangeLogTableGenerator extends CreateDatabaseChangeLogTableGenerator {

    @Override
    public boolean supports(CreateDatabaseChangeLogTableStatement statement, Database database) {
        return database instanceof HiveMetastoreDatabase && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(CreateDatabaseChangeLogTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String TypeNameChar = "STRING";

        CreateTableStatement createTableStatement = new CreateTableStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName())
                .setTablespace(database.getLiquibaseTablespaceName())
                .addColumn("ID", DataTypeFactory.getInstance().fromDescription(TypeNameChar, database))
                .addColumn("AUTHOR", DataTypeFactory.getInstance().fromDescription(TypeNameChar, database))
                .addColumn("FILENAME", DataTypeFactory.getInstance().fromDescription(TypeNameChar, database))
                .addColumn("DATEEXECUTED", DataTypeFactory.getInstance().fromDescription("TIMESTAMP", database))
                .addColumn("ORDEREXECUTED", DataTypeFactory.getInstance().fromDescription("INT", database))
                .addColumn("EXECTYPE", DataTypeFactory.getInstance().fromDescription(TypeNameChar, database))
                .addColumn("MD5SUM", DataTypeFactory.getInstance().fromDescription(TypeNameChar, database))
                .addColumn("DESCRIPTION", DataTypeFactory.getInstance().fromDescription(TypeNameChar, database))
                .addColumn("COMMENTS", DataTypeFactory.getInstance().fromDescription(TypeNameChar, database))
                .addColumn("TAG", DataTypeFactory.getInstance().fromDescription(TypeNameChar, database))
                .addColumn("LIQUIBASE", DataTypeFactory.getInstance().fromDescription(TypeNameChar, database))
                .addColumn("CONTEXTS", DataTypeFactory.getInstance().fromDescription(TypeNameChar, database))
                .addColumn("LABELS", DataTypeFactory.getInstance().fromDescription(TypeNameChar, database))
                .addColumn("DEPLOYMENT_ID", DataTypeFactory.getInstance().fromDescription(TypeNameChar, database));

        return CustomSqlGenerator.generateSql(database, UserSessionSettings.syncDdlStart(), createTableStatement, UserSessionSettings.syncDdlStop());
    }
}
