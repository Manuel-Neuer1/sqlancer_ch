package sqlancer.common.query;

import java.io.Closeable;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLancerResultSet implements Closeable {
    // SQLancerResultSet 是一个包装类，用于封装 JDBC 的 ResultSet 对象
    ResultSet rs;
    // 持有一个 JDBC 的 ResultSet 对象，代表 SQL 查询的结果集,用于通过 SQL 查询获取的记录
    // ResultSet 是通过执行 SQL 查询语句（如 SELECT）生成的结果集
    // 数据以“表”的形式表示，每一行代表一条记录，每一列代表字段
    // ResultSet 以光标的形式工作，初始时光标位于第一行之前。
    // 通过调用 next() 方法，可以将光标移动到下一行并读取该行数据。

    private Runnable runnableEpilogue;

    public SQLancerResultSet(ResultSet rs) {
        this.rs = rs;
    }

    @Override
    public void close() {
        try {
            if (runnableEpilogue != null) {
                runnableEpilogue.run();
            }
            rs.getStatement().close();
            rs.close();
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    public boolean next() throws SQLException {
        return rs.next();
    }

    public int getInt(int i) throws SQLException {
        return rs.getInt(i);
    }

    public String getString(int i) throws SQLException {
        try {
            return rs.getString(i);
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    public boolean isClosed() throws SQLException {
        return rs.isClosed();
    }

    public long getLong(int i) throws SQLException {
        return rs.getLong(i);
    }

    // 获取当前行第 i 列的 SQL 数据类型名称
    public String getType(int i) throws SQLException {
        return rs.getMetaData().getColumnTypeName(i);
    }

    public void registerEpilogue(Runnable runnableEpilogue) {
        this.runnableEpilogue = runnableEpilogue;
    }

}
