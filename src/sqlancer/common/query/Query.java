package sqlancer.common.query;

import sqlancer.GlobalState;
import sqlancer.SQLancerDBConnection;
import sqlancer.common.log.Loggable;

public abstract class Query<C extends SQLancerDBConnection> implements Loggable {

    /**
     * Gets the query string, which is guaranteed to be terminated with a semicolon.
     * 获取查询字符串，保证以分号终止。
     *
     * @return the query string.
     */
    public abstract String getQueryString();

    /**
     * Gets the query string without trailing semicolons.
     * 获取不带尾随分号的查询字符串。
     * 
     * @return the query string that does not end with a ";".
     */
    public abstract String getUnterminatedQueryString();

    /**
     * Whether the query could affect the schema (i.e., by add/deleting columns or
     * tables).
     * 查询是否会影响模式（即通过添加/删除列或表）。
     *
     * @return true if the query can affect the database's schema, false otherwise
     */
    public abstract boolean couldAffectSchema();

    // 执行 SQL 查询，并返回查询是否成功
    public abstract <G extends GlobalState<?, ?, C>> boolean execute(G globalState, String... fills) throws Exception;

    public abstract ExpectedErrors getExpectedErrors();

    @Override
    public String toString() {
        return getQueryString();
    }

    // 执行查询，并返回结果集
    public <G extends GlobalState<?, ?, C>> SQLancerResultSet executeAndGet(G globalState, String... fills)
            throws Exception {
        throw new AssertionError();
    }

    public <G extends GlobalState<?, ?, C>> boolean executeLogged(G globalState) throws Exception {
        logQueryString(globalState);
        return execute(globalState);
    }

    public <G extends GlobalState<?, ?, C>> SQLancerResultSet executeAndGetLogged(G globalState) throws Exception {
        logQueryString(globalState);
        return executeAndGet(globalState);
    }

    private <G extends GlobalState<?, ?, C>> void logQueryString(G globalState) {
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(getQueryString());
        }
    }

}
