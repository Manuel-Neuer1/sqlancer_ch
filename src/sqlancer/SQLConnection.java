package sqlancer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLConnection implements SQLancerDBConnection {
    // SQLConnection 实现了 SQLancerDBConnection 接口
    // SQLConnection 必须实现 SQLancerDBConnection 中声明的所有方法

    // 持有一个 Connection 对象，该对象代表与数据库的连接
    private final Connection connection;

    // 构造函数
    public SQLConnection(Connection connection) {
        this.connection = connection;
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
        // DatabaseMetaData 是一个接口，提供了许多方法来获取关于数据库的各种信息，比如数据库的版本、支持的 SQL 特性、是否支持事务等
        DatabaseMetaData meta = connection.getMetaData();
        return meta.getDatabaseProductVersion();// 获取数据库版本
    }

    @Override
    public void close() throws SQLException {
        connection.close(); // 关闭数据库连接
    }

    // 接下来提供了两个方法；
    // 分别用于准备一个 SQL 语句（prepareStatement）
    // 创建一个简单的 SQL 语句对象（createStatement）。
    public Statement prepareStatement(String arg) throws SQLException {
        // String arg，这通常是一个 SQL 查询语句
        return connection.prepareStatement(arg);
    }

    public Statement createStatement() throws SQLException {
        return connection.createStatement();
    }
}
