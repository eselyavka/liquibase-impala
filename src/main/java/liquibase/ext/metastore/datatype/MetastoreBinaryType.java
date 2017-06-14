package liquibase.ext.metastore.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.BigIntType;
import liquibase.ext.metastore.database.HiveMetastoreDatabase;

@DataTypeInfo(name = "binary", aliases = {"java.sql.BINARY"}, minParameters = 0, maxParameters = 0, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class MetastoreBinaryType extends BigIntType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof HiveMetastoreDatabase) {
            return new DatabaseDataType("BINARY");
        }

        return super.toDatabaseDataType(database);
    }
}
