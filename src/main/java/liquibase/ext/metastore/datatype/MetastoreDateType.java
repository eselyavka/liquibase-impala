package liquibase.ext.metastore.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.DateType;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;

@DataTypeInfo(name = "date", aliases = {"java.sql.Types.DATE",
        "java.sql.Date"}, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class MetastoreDateType extends DateType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof HiveMetastoreDatabase) {
            return new DatabaseDataType("TIMESTAMP", getParameters());
        }

        return super.toDatabaseDataType(database);
    }
}
