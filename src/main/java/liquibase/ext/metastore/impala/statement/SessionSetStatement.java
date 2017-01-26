package liquibase.ext.metastore.impala.statement;

import liquibase.statement.AbstractSqlStatement;

public class SessionSetStatement extends AbstractSqlStatement {
    private String queryOption;
    private Object optionValue;

    public SessionSetStatement(String queryOption, Object optionValue) {
        this.queryOption = queryOption;
        this.optionValue = optionValue;
    }

    public String getQueryOption() {
        return queryOption;
    }

    public Object getOptionValue() {
        return optionValue;
    }
}
