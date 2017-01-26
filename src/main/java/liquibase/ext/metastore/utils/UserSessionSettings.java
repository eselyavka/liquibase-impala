package liquibase.ext.metastore.utils;

import liquibase.ext.metastore.impala.statement.SessionSetStatement;

public class UserSessionSettings {

    public static SessionSetStatement syncDdlStart() {
        return new SessionSetStatement("SYNC_DDL", 1);
    }

    public static SessionSetStatement syncDdlStop() {
        return new SessionSetStatement("SYNC_DDL", 0);
    }
}
