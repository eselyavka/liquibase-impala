package liquibase.ext.metastore.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.DatabaseDataType;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;
import liquibase.sqlgenerator.core.AddColumnGenerator;
import liquibase.statement.core.AddColumnStatement;

public class MetastoreAddColumnGenerator extends AddColumnGenerator {

    @Override
    public boolean supports(AddColumnStatement statement, Database database) {
        return database instanceof HiveMetastoreDatabase && super.supports(statement, database);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    protected String generateSingleColumnSQL(AddColumnStatement statement, Database database) {
        DatabaseDataType databaseColumnType = DataTypeFactory.getInstance().fromDescription(statement.getColumnType(), database).toDatabaseDataType(database);
        return " ADD COLUMNS (" + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName()) + " " + databaseColumnType + ")";
    }
}
