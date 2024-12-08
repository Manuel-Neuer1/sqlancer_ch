package sqlancer.common.query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import sqlancer.GlobalState;
import sqlancer.SQLConnection;

public class SQLQueryResultCheckAdapter extends SQLQueryAdapter {

    private final Consumer<ResultSet> rsChecker;

    public SQLQueryResultCheckAdapter(String query, Consumer<ResultSet> rsChecker) {
        super(query);
        this.rsChecker = rsChecker;
    }

    @Override
    public <G extends GlobalState<?, ?, SQLConnection>> boolean execute(G globalState, String... fills)
            throws SQLException {
        /*
         * Statement 是 Java JDBC API 中的一个接口，用于执行 SQL 查询和更新操作。它代表了一种用于发送静态 SQL 语句到数据库的方式。
         * Statement 对象通过 JDBC 连接 (Connection) 的方法创建：
         * Statement stmt = connection.createStatement();
         * connection 是通过 JDBC 驱动获取的数据库连接对象（java.sql.Connection）。
         * 创建的 Statement 用于执行不带参数的简单 SQL 语句。
         * ResultSet rs = stmt.executeQuery(String sql);
         * 用于执行 SELECT 查询。
         * 返回 ResultSet 对象，表示查询结果。
         */
        try (Statement s = globalState.getConnection().createStatement()) {
            // getQueryString 方法返回需要执行的 SQL 查询字符串
            // executeQuery()用于执行查询语句，返回一个 ResultSet 对象，包含查询结果
            ResultSet rs = s.executeQuery(getQueryString());
            rsChecker.accept(rs);
            return true;
        } catch (Exception e) {
            checkException(e);
            return false;
        }
    }

}
