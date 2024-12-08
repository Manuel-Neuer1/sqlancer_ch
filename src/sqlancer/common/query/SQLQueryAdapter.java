package sqlancer.common.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import sqlancer.GlobalState;
import sqlancer.Main;
import sqlancer.SQLConnection;

public class SQLQueryAdapter extends Query<SQLConnection> {
    // SQLQueryAdapter 是一个继承自 Query<SQLConnection> 的类
    /*
     * 管理 SQL 查询的执行：
     * 提供对查询字符串的封装，并通过 JDBC 接口实际执行查询。
     * 
     * 处理查询结果：
     * 使用 SQLancerResultSet 封装查询结果集。
     * 
     * 捕获和检查错误：
     * 在执行过程中捕获异常，并验证是否属于预期错误。
     * 
     * 查询元信息：
     * 提供查询是否会影响数据库模式（schema）等信息。
     */

    private final String query; // SQL 查询的字符串表示形式
    private final ExpectedErrors expectedErrors;
    private final boolean couldAffectSchema; // 指示该查询是否可能影响数据库模式（如 CREATE TABLE 操作）

    // 仅指定查询字符串
    public SQLQueryAdapter(String query) {
        this(query, new ExpectedErrors());
    }

    // 指定查询字符串和 schema 影响
    public SQLQueryAdapter(String query, boolean couldAffectSchema) {
        this(query, new ExpectedErrors(), couldAffectSchema);
    }

    // 指定查询字符串和预期错误
    public SQLQueryAdapter(String query, ExpectedErrors expectedErrors) {
        this(query, expectedErrors, guessAffectSchemaFromQuery(query));
    }

    private static boolean guessAffectSchemaFromQuery(String query) {
        // 使用String类型的contains方法
        // boolean String.contains(str) 返回子字符串str是否在原字符串中存在
        // startWith(str) 方法也是，判断字符串开头是不是以str子字符串
        return query.contains("CREATE TABLE") && !query.startsWith("EXPLAIN");
    }

    public SQLQueryAdapter(String query, ExpectedErrors expectedErrors, boolean couldAffectSchema) {
        this(query, expectedErrors, couldAffectSchema, true);
    }

    // 全参构造函数
    public SQLQueryAdapter(String query, ExpectedErrors expectedErrors, boolean couldAffectSchema,
            boolean canonicalizeString) {
        if (canonicalizeString) {
            this.query = canonicalizeString(query);
        } else {
            this.query = query;
        }
        this.expectedErrors = expectedErrors;
        this.couldAffectSchema = couldAffectSchema;
        checkQueryString();
    }

    private String canonicalizeString(String s) {
        if (s.endsWith(";")) {
            return s;
        } else if (!s.contains("--")) {
            return s + ";";
        } else {
            // query contains a comment
            return s;
        }
    }

    private void checkQueryString() {
        // 如果是Create Table 语句并且没有修改couldAffectSchema为true
        if (!couldAffectSchema && guessAffectSchemaFromQuery(query)) {
            throw new AssertionError("CREATE TABLE statements should set couldAffectSchema to true");
        }
    }

    // 重写Query类中的方法，返回完整的 SQL 查询字符串
    @Override
    public String getQueryString() {
        return query;
    }

    // 如果查询字符串以分号结尾，则移除分号并返回
    @Override
    public String getUnterminatedQueryString() {
        String result;
        if (query.endsWith(";")) {
            result = query.substring(0, query.length() - 1);
        } else {
            result = query;
        }
        assert !result.endsWith(";");
        return result;
    }

    @Override
    public <G extends GlobalState<?, ?, SQLConnection>> boolean execute(G globalState, String... fills)
            throws SQLException {
        Statement s;
        if (fills.length > 0) {
            // 如果 fills 参数非空，说明我们有占位符，需要使用 PreparedStatement
            s = globalState.getConnection().prepareStatement(fills[0]);
            for (int i = 1; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            // 如果 fills 参数为空，使用 Statement 来执行 SQL 查询
            s = globalState.getConnection().createStatement();
        }
        try {
            if (fills.length > 0) {
                // 如果 fills 非空，使用 PreparedStatement 执行
                ((PreparedStatement) s).execute();
            } else {
                // 否则使用 Statement 执行普通的查询
                s.execute(query);
            }
            Main.nrSuccessfulActions.addAndGet(1); // 成功执行时增加成功计数
            return true;
        } catch (Exception e) {
            Main.nrUnsuccessfulActions.addAndGet(1); // 出错时增加失败计数
            checkException(e);
            return false;
        } finally {
            s.close(); // 无论成功与否，最后都关闭 Statement
        }
    }

    public void checkException(Exception e) throws AssertionError {
        Throwable ex = e;

        while (ex != null) {
            if (expectedErrors.errorIsExpected(ex.getMessage())) {
                return;
            } else {
                ex = ex.getCause();
            }
        }

        throw new AssertionError(query, e);
    }

    @Override
    public <G extends GlobalState<?, ?, SQLConnection>> SQLancerResultSet executeAndGet(G globalState, String... fills)
            throws SQLException {
        Statement s;
        if (fills.length > 0) {
            s = globalState.getConnection().prepareStatement(fills[0]);
            for (int i = 1; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            s = globalState.getConnection().createStatement();
        }
        ResultSet result;
        try {
            if (fills.length > 0) {
                result = ((PreparedStatement) s).executeQuery();
            } else {
                result = s.executeQuery(query);
            }
            Main.nrSuccessfulActions.addAndGet(1);
            if (result == null) {
                return null;
            }
            return new SQLancerResultSet(result);
        } catch (Exception e) {
            s.close();
            Main.nrUnsuccessfulActions.addAndGet(1);
            checkException(e);
        }
        return null;
    }

    @Override
    public boolean couldAffectSchema() {
        // 返回查询是否可能影响数据库模式
        // 由构造函数中的 couldAffectSchema 字段决定
        return couldAffectSchema;
    }

    @Override
    public ExpectedErrors getExpectedErrors() {
        return expectedErrors;
    }

    @Override
    public String getLogString() {
        return getQueryString();
    }
}
