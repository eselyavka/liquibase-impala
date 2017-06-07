package liquibase.ext.metastore.utils;

import liquibase.ext.metastore.statement.SetStatement;

public class UserSessionSettings {

    public static SetStatement syncDdlStart() {
        return new SetStatement("SYNC_DDL", 1);
    }

    public static SetStatement syncDdlStop() {
        return new SetStatement("SYNC_DDL", 0);
    }
}
