package liquibase.ext.metastore.impala.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.ClobType;
import liquibase.ext.metastore.impala.database.ImpalaDatabase;

@DataTypeInfo(name = "text", aliases = {"java.sql.Types.CLOB",
        "java.sql.Types.NCLOB",
        "java.sql.Types.BLOB",
        "java.lang.String",
        "string",
        "clob",
        "blob"}, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class ImpalaTextType extends ClobType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof ImpalaDatabase) {
            return new DatabaseDataType("STRING", getParameters());
        }

        return super.toDatabaseDataType(database);
    }
}
