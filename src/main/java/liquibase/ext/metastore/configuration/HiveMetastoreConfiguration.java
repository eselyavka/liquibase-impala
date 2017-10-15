package liquibase.ext.metastore.configuration;

import liquibase.configuration.AbstractConfigurationContainer;

public class HiveMetastoreConfiguration extends AbstractConfigurationContainer {
    private static final String LOCK = "lock";
    private static final String SYNC_DDL = "syncDDL";
    private static final String CUSTOM_HIVE_JDBC_DRIVER = "customHiveJDBCDriver";
    private static final String CUSTOM_IMPALA_JDBC_DRIVER = "customImpalaJDBCDriver";

    public HiveMetastoreConfiguration() {
        super("liquibase");
        getContainer().addProperty(LOCK, Boolean.class)
                .setDescription("Should Liquibase lock database while executing")
                .setDefaultValue(true)
                .addAlias("lock");
        getContainer().addProperty(SYNC_DDL, Boolean.class)
                .setDescription("Wrap every statement with SYNC_DDL")
                .setDefaultValue(true)
                .addAlias("syncDDL");
        getContainer().addProperty(CUSTOM_HIVE_JDBC_DRIVER, String.class)
                .setDescription("Set custom JDBC driver for Hive")
                .setDefaultValue("com.cloudera.hive.jdbc41.HS2Driver")
                .addAlias("customHiveJDBCDriver");
        getContainer().addProperty(CUSTOM_IMPALA_JDBC_DRIVER, String.class)
                .setDescription("Set custom JDBC driver for Impala")
                .setDefaultValue("com.cloudera.impala.jdbc41.Driver")
                .addAlias("customImpalaJDBCDriver");
    }

    public boolean getLock() {
        return getContainer().getValue(LOCK, Boolean.class);
    }

    public HiveMetastoreConfiguration setLock(boolean noLock) {
        getContainer().setValue(LOCK, noLock);
        return this;
    }

    public boolean getSyncDDL() {
        return getContainer().getValue(SYNC_DDL, Boolean.class);
    }

    public HiveMetastoreConfiguration setSyncDDL(boolean syncDdl) {
        getContainer().setValue(SYNC_DDL, Boolean.class);
        return this;
    }

    public String getHiveDriver() {
        return getContainer().getValue(CUSTOM_HIVE_JDBC_DRIVER, String.class);
    }

    public HiveMetastoreConfiguration setHiveDriver(String driverName) {
        getContainer().setValue(CUSTOM_HIVE_JDBC_DRIVER, String.class);
        return this;
    }

    public String getImpalaDriver() {
        return getContainer().getValue(CUSTOM_IMPALA_JDBC_DRIVER, String.class);
    }

    public HiveMetastoreConfiguration setImpalaDriver(String driverName) {
        getContainer().setValue(CUSTOM_IMPALA_JDBC_DRIVER, String.class);
        return this;
    }
}
