package liquibase.ext.metastore.configuration;

import liquibase.configuration.AbstractConfigurationContainer;

public class HiveMetastoreConfiguration extends AbstractConfigurationContainer {
    private static final String LOCK = "lock";
    private static final String SYNC_DDL = "syncDDL";

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
}
