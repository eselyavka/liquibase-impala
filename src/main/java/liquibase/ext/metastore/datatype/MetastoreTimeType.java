package liquibase.ext.metastore.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.TimeType;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;

@DataTypeInfo(name = "time", aliases = {"java.sql.Types.TIME",
        "java.util.Date",
        "timetz"}, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class MetastoreTimeType extends TimeType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof HiveMetastoreDatabase) {
            return new DatabaseDataType("TIMESTAMP", getParameters());
        }

        return super.toDatabaseDataType(database);
    }
}
