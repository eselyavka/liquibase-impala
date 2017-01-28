package liquibase.ext.metastore.impala.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.BigIntType;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;

@DataTypeInfo(name = "bigint", aliases = {"java.sql.Types.BIGINT",
        "java.math.BigInteger",
        "java.lang.Long",
        "integer8",
        "bigserial",
        "serial8",
        "int8"}, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class ImpalaBigIntType extends BigIntType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof HiveMetastoreDatabase) {
            return new DatabaseDataType("BIGINT");
        }

        return super.toDatabaseDataType(database);
    }
}
