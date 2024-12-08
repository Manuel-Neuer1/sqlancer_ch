package sqlancer.common.query;

@FunctionalInterface
public interface SQLQueryProvider<S> {
    // SQLQueryAdapter 是一个封装了 SQL 查询的类
    // SQL 查询字符串
    SQLQueryAdapter getQuery(S globalState) throws Exception;
}
