package liquibase.ext.metastore.statement;

import liquibase.statement.AbstractSqlStatement;

public class SetStatement extends AbstractSqlStatement {
    private String queryOption;
    private Object optionValue;

    public SetStatement(String queryOption, Object optionValue) {
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
