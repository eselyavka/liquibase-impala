package liquibase.ext.metastore.impala.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.DateType;
import liquibase.ext.metastore.impala.database.ImpalaDatabase;

@DataTypeInfo(name = "datetime", aliases = {"java.sql.Types.DATETIME",
        "java.util.Date"}, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class ImpalaDateTimeType extends DateType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof ImpalaDatabase) {
            return new DatabaseDataType("TIMESTAMP", getParameters());
        }

        return super.toDatabaseDataType(database);
    }
}
