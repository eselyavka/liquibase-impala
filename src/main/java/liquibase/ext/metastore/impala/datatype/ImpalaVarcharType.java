package liquibase.ext.metastore.impala.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.VarcharType;
import liquibase.ext.metastore.impala.database.ImpalaDatabase;

@DataTypeInfo(name = "varchar", aliases = {"java.sql.Types.VARCHAR",
        "java.sql.Types.LONGVARCHAR",
        "java.sql.Types.NVARCHAR",
        "java.sql.Types.LONGNVARCHAR",
        "java.lang.String",
        "varchar",
        "nvarchar",
        "varchar2",
        "nvarchar2"}, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class ImpalaVarcharType extends VarcharType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof ImpalaDatabase) {
            return new DatabaseDataType("STRING", getParameters());
        }

        return super.toDatabaseDataType(database);
    }
}
