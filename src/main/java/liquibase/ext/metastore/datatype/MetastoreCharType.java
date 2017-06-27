package liquibase.ext.metastore.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.CharType;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;

@DataTypeInfo(name = "char", aliases = {"java.sql.Types.CHAR",
        "java.sql.Types.NCHAR",
        "java.lang.Character",
        "char",
        "nchar"}, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class MetastoreCharType extends CharType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof HiveMetastoreDatabase) {
            return new DatabaseDataType("STRING", getParameters());
        }

        return super.toDatabaseDataType(database);
    }
}
