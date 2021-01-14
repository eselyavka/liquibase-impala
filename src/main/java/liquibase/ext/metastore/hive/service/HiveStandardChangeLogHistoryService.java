package liquibase.ext.metastore.hive.service;

import liquibase.ContextExpression;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Labels;
import liquibase.change.CheckSum;
import liquibase.change.ColumnConfig;
import liquibase.changelog.AbstractChangeLogHistoryService;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.RanChangeSet;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.DatabaseHistoryException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.metastore.hive.database.HiveDatabase;
import liquibase.logging.Logger;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotControl;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.AddColumnStatement;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;
import liquibase.statement.core.DropTableStatement;
import liquibase.statement.core.GetNextChangeSetSequenceValueStatement;
import liquibase.statement.core.MarkChangeSetRanStatement;
import liquibase.statement.core.RemoveChangeSetRanStatusStatement;
import liquibase.statement.core.SelectFromDatabaseChangeLogStatement;
import liquibase.statement.core.TagDatabaseStatement;
import liquibase.statement.core.UpdateChangeSetChecksumStatement;
import liquibase.statement.core.UpdateStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.Table;
import liquibase.Scope;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class HiveStandardChangeLogHistoryService extends AbstractChangeLogHistoryService {

    private final Logger LOG = Scope.getCurrentScope().getLog(getClass());
    private List<RanChangeSet> ranChangeSetList;
    private boolean serviceInitialized = false;
    private Boolean hasDatabaseChangeLogTable = null;
    private boolean databaseChecksumsCompatible = true;
    private Integer lastChangeSetSequenceValue;

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof HiveDatabase;
    }

    private String getDatabaseChangeLogTableName() {
        return getDatabase().getDatabaseChangeLogTableName();
    }

    private String getLiquibaseSchemaName() {
        return getDatabase().getLiquibaseSchemaName();
    }

    public String getLiquibaseCatalogName() {
        return getDatabase().getLiquibaseCatalogName();
    }

    private boolean canCreateChangeLogTable() throws DatabaseException {
        return true;
    }

    @Override
    public void reset() {
        this.ranChangeSetList = null;
        this.serviceInitialized = false;
        this.hasDatabaseChangeLogTable = null;
    }

    private boolean hasDatabaseChangeLogTable() throws DatabaseException {
        if (hasDatabaseChangeLogTable == null) {
            try {
                hasDatabaseChangeLogTable = SnapshotGeneratorFactory.getInstance().hasDatabaseChangeLogTable(getDatabase());
            } catch (LiquibaseException e) {
                throw new UnexpectedLiquibaseException(e);
            }
        }
        return hasDatabaseChangeLogTable;
    }

    private String getCharTypeName() {
        return "string";
    }

    @Override
    public void init() throws DatabaseException {
        if (serviceInitialized) {
            return;
        }
        Database database = getDatabase();
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", getDatabase());

        Table changeLogTable = null;
        try {
            changeLogTable = SnapshotGeneratorFactory.getInstance().getDatabaseChangeLogTable(new SnapshotControl(database, false, Table.class, Column.class), database);
        } catch (LiquibaseException e) {
            throw new UnexpectedLiquibaseException(e);
        }

        List<SqlStatement> statementsToExecute = new ArrayList<SqlStatement>();

        if (changeLogTable != null) {
            boolean hasDescription = changeLogTable.getColumn("DESCRIPTION") != null;
            boolean hasComments = changeLogTable.getColumn("COMMENTS") != null;
            boolean hasTag = changeLogTable.getColumn("TAG") != null;
            boolean hasLiquibase = changeLogTable.getColumn("LIQUIBASE") != null;
            boolean hasContexts = changeLogTable.getColumn("CONTEXTS") != null;
            boolean hasLabels = changeLogTable.getColumn("LABELS") != null;
            boolean hasOrderExecuted = changeLogTable.getColumn("ORDEREXECUTED") != null;
            boolean hasExecTypeColumn = changeLogTable.getColumn("EXECTYPE") != null;
            String charTypeName = getCharTypeName();
            boolean hasDeploymentIdColumn = changeLogTable.getColumn("DEPLOYMENT_ID") != null;

            if (!hasDescription) {
                executor.comment("Adding missing databasechangelog.description column");
                statementsToExecute.add(new AddColumnStatement(getLiquibaseCatalogName(), getLiquibaseSchemaName(), getDatabaseChangeLogTableName(), "DESCRIPTION", charTypeName, null));
            }
            if (!hasTag) {
                executor.comment("Adding missing databasechangelog.tag column");
                statementsToExecute.add(new AddColumnStatement(getLiquibaseCatalogName(), getLiquibaseSchemaName(), getDatabaseChangeLogTableName(), "TAG", charTypeName, null));
            }
            if (!hasComments) {
                executor.comment("Adding missing databasechangelog.comments column");
                statementsToExecute.add(new AddColumnStatement(getLiquibaseCatalogName(), getLiquibaseSchemaName(), getDatabaseChangeLogTableName(), "COMMENTS", charTypeName, null));
            }
            if (!hasLiquibase) {
                executor.comment("Adding missing databasechangelog.liquibase column");
                statementsToExecute.add(new AddColumnStatement(getLiquibaseCatalogName(), getLiquibaseSchemaName(), getDatabaseChangeLogTableName(), "LIQUIBASE", charTypeName, null));
            }
            if (!hasOrderExecuted) {
                executor.comment("Adding missing databasechangelog.orderexecuted column");
                statementsToExecute.add(new AddColumnStatement(getLiquibaseCatalogName(), getLiquibaseSchemaName(), getDatabaseChangeLogTableName(), "ORDEREXECUTED", "int", null));
            }
            if (!hasExecTypeColumn) {
                executor.comment("Adding missing databasechangelog.exectype column");
                statementsToExecute.add(new AddColumnStatement(getLiquibaseCatalogName(), getLiquibaseSchemaName(), getDatabaseChangeLogTableName(), "EXECTYPE", charTypeName, null));
            }

            if (!hasContexts) {
                executor.comment("Adding missing databasechangelog.contexts column");
                statementsToExecute.add(new AddColumnStatement(getLiquibaseCatalogName(), getLiquibaseSchemaName(), getDatabaseChangeLogTableName(), "CONTEXTS", charTypeName, null));
            }

            if (!hasLabels) {
                executor.comment("Adding missing databasechangelog.labels column");
                statementsToExecute.add(new AddColumnStatement(getLiquibaseCatalogName(), getLiquibaseSchemaName(), getDatabaseChangeLogTableName(), "LABELS", charTypeName, null));
            }

            if (!hasDeploymentIdColumn) {
                executor.comment("Adding missing databasechangelog.deployment_id column");
                statementsToExecute.add(new AddColumnStatement(getLiquibaseCatalogName(), getLiquibaseSchemaName(), getDatabaseChangeLogTableName(), "DEPLOYMENT_ID", charTypeName, null));
            }
        } else {
            executor.comment("Create Database Change Log Table");
            SqlStatement createTableStatement = new CreateDatabaseChangeLogTableStatement();
            if (!canCreateChangeLogTable()) {
                throw new DatabaseException("Cannot create " + getDatabase().escapeTableName(getLiquibaseCatalogName(), getLiquibaseSchemaName(), getDatabaseChangeLogTableName()) + " table for your getDatabase().\n\n" +
                        "Please construct it manually using the following SQL as a base and re-run Liquibase:\n\n" +
                        createTableStatement);
            }
            // If there is no table in the database for recording change history create one.
            statementsToExecute.add(createTableStatement);
            LOG.info("Creating database history table with name: " + getDatabase().escapeTableName(getLiquibaseCatalogName(), getLiquibaseSchemaName(), getDatabaseChangeLogTableName()));
        }

        for (SqlStatement sql : statementsToExecute) {
            if (SqlGeneratorFactory.getInstance().supports(sql, database)) {
                executor.execute(sql);
                getDatabase().commit();
            } else {
                LOG.info("Cannot run " + sql.getClass().getSimpleName() + " on " + getDatabase().getShortName() + " when checking databasechangelog table");
            }
        }
        serviceInitialized = true;
    }

    @Override
    public void upgradeChecksums(final DatabaseChangeLog databaseChangeLog, final Contexts contexts, LabelExpression labels) throws DatabaseException {
        super.upgradeChecksums(databaseChangeLog, contexts, labels);
        getDatabase().commit();
    }

    public String concat(String field) {
        String databaseChangeLogTableName = getDatabase().escapeObjectName(getDatabaseChangeLogTableName(), Table.class);
        return databaseChangeLogTableName + "." + field;
    }

    @Override
    public List<RanChangeSet> getRanChangeSets() throws DatabaseException {
        if (this.ranChangeSetList == null) {
            Database database = getDatabase();
            String databaseChangeLogTableName = getDatabase().escapeObjectName(getDatabaseChangeLogTableName(), Table.class);
            List<RanChangeSet> ranChangeSetList = new ArrayList<RanChangeSet>();
            if (hasDatabaseChangeLogTable()) {
                LOG.info("Reading from " + databaseChangeLogTableName);
                List<Map<String, ?>> results = queryDatabaseChangeLogTable(database);
                for (Map rs : results) {
                    String fileName = rs.get(concat("FILENAME")).toString();
                    String author = rs.get(concat("AUTHOR")).toString();
                    String id = rs.get(concat("ID")).toString();
                    String md5sum = rs.get(concat("MD5SUM")) == null || !databaseChecksumsCompatible ? null : rs.get(concat("MD5SUM")).toString();
                    String description = rs.get(concat("DESCRIPTION")) == null ? null : rs.get(concat("DESCRIPTION")).toString();
                    String comments = rs.get(concat("COMMENTS")) == null ? null : rs.get(concat("COMMENTS")).toString();
                    Object tmpDateExecuted = rs.get(concat("DATEEXECUTED"));
                    Date dateExecuted = null;
                    if (tmpDateExecuted instanceof Date) {
                        dateExecuted = (Date) tmpDateExecuted;
                    } else {
                        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            dateExecuted = df.parse((String) tmpDateExecuted);
                        } catch (ParseException e) {
                        }
                    }
                    String tmpOrderExecuted = rs.get(concat("ORDEREXECUTED")).toString();
                    Integer orderExecuted = (tmpOrderExecuted == null ? null : Integer.valueOf(tmpOrderExecuted));
                    String tag = rs.get(concat("TAG")) == null ? null : rs.get(concat("TAG")).toString();
                    String execType = rs.get(concat("EXECTYPE")) == null ? null : rs.get(concat("EXECTYPE")).toString();
                    ContextExpression contexts = new ContextExpression((String) rs.get(concat("CONTEXTS")));
                    Labels labels = new Labels((String) rs.get(concat("LABELS")));
                    String deploymentId = (String) rs.get(concat("DEPLOYMENT_ID"));

                    try {
                        RanChangeSet ranChangeSet = new RanChangeSet(fileName, id, author, CheckSum.parse(md5sum), dateExecuted, tag, ChangeSet.ExecType.valueOf(execType), description, comments, contexts, labels, deploymentId);
                        ranChangeSet.setOrderExecuted(orderExecuted);
                        ranChangeSetList.add(ranChangeSet);
                    } catch (IllegalArgumentException e) {
                        LOG.severe("Unknown EXECTYPE from database: " + execType);
                        throw e;
                    }
                }
            }

            this.ranChangeSetList = ranChangeSetList;
        }
        return Collections.unmodifiableList(ranChangeSetList);
    }

    private List<Map<String, ?>> queryDatabaseChangeLogTable(Database database) throws DatabaseException {
        SelectFromDatabaseChangeLogStatement select = new SelectFromDatabaseChangeLogStatement(new ColumnConfig().setName("*").setComputed(true)).setOrderBy("DATEEXECUTED ASC", "ORDEREXECUTED ASC");
        return Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", getDatabase()).queryForList(select);
    }

    @Override
    protected void replaceChecksum(ChangeSet changeSet) throws DatabaseException {
        Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", getDatabase()).execute(new UpdateChangeSetChecksumStatement(changeSet));

        getDatabase().commit();
        reset();
    }

    @Override
    public RanChangeSet getRanChangeSet(final ChangeSet changeSet) throws DatabaseException, DatabaseHistoryException {
        if (!hasDatabaseChangeLogTable()) {
            return null;
        }

        return super.getRanChangeSet(changeSet);
    }

    @Override
    public void setExecType(ChangeSet changeSet, ChangeSet.ExecType execType) throws DatabaseException {
        Database database = getDatabase();

        Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", getDatabase()).execute(new MarkChangeSetRanStatement(changeSet, execType));
        getDatabase().commit();
        if (this.ranChangeSetList != null) {
            this.ranChangeSetList.add(new RanChangeSet(changeSet, execType, null, null));
        }

    }

    @Override
    public void removeFromHistory(final ChangeSet changeSet) throws DatabaseException {
        Database database = getDatabase();
        Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", getDatabase()).execute(new RemoveChangeSetRanStatusStatement(changeSet));
        getDatabase().commit();

        if (this.ranChangeSetList != null) {
            this.ranChangeSetList.remove(new RanChangeSet(changeSet));
        }
    }

    @Override
    public int getNextSequenceValue() throws LiquibaseException {
        if (lastChangeSetSequenceValue == null) {
            if (getDatabase().getConnection() == null) {
                lastChangeSetSequenceValue = 0;
            } else {
                lastChangeSetSequenceValue = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", getDatabase()).queryForInt(new GetNextChangeSetSequenceValueStatement());
            }
        }

        return ++lastChangeSetSequenceValue;
    }

    @Override
    public void tag(final String tagString) throws DatabaseException {
        Database database = getDatabase();
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", getDatabase());
        try {
            int totalRows = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", getDatabase()).queryForInt(new SelectFromDatabaseChangeLogStatement(new ColumnConfig().setName("COUNT(*)", true)));
            if (totalRows == 0) {
                ChangeSet emptyChangeSet = new ChangeSet(String.valueOf(new Date().getTime()), "liquibase", false, false, "liquibase-internal", null, null, getDatabase().getObjectQuotingStrategy(), null);
                this.setExecType(emptyChangeSet, ChangeSet.ExecType.EXECUTED);
            }

//            Timestamp lastExecutedDate = (Timestamp) this.getExecutor().queryForObject(createChangeToTagSQL(), Timestamp.class);
            executor.execute(new TagDatabaseStatement(tagString));
            getDatabase().commit();

            if (this.ranChangeSetList != null) {
                ranChangeSetList.get(ranChangeSetList.size() - 1).setTag(tagString);
            }
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean tagExists(final String tag) throws DatabaseException {
        int count = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", getDatabase()).queryForInt(new SelectFromDatabaseChangeLogStatement(new SelectFromDatabaseChangeLogStatement.ByTag(tag), new ColumnConfig().setName("COUNT(*)", true)));
        return count > 0;
    }

    @Override
    public void clearAllCheckSums() throws LiquibaseException {
        Database database = getDatabase();
        UpdateStatement updateStatement = new UpdateStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName());
        updateStatement.addNewColumnValue("MD5SUM", null);
        Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", getDatabase()).execute(updateStatement);
        database.commit();
    }

    @Override
    public void destroy() throws DatabaseException {
        Database database = getDatabase();
        try {
            if (SnapshotGeneratorFactory.getInstance().has(new Table().setName(database.getDatabaseChangeLogTableName()).setSchema(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName()), database)) {
                Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", getDatabase()).execute(new DropTableStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName(), false));
            }
            reset();
        } catch (InvalidExampleException e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }
}

