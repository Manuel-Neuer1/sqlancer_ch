package sqlancer.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBToStringVisitor;

public class DuckDBQueryPartitioningDistinctTester extends DuckDBQueryPartitioningBase {

    public DuckDBQueryPartitioningDistinctTester(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        // 设置查询为DISTINCT
        select.setDistinct(true);
        // 清除WHERE子句
        select.setWhereClause(null);
        // 将SELECT语句转换为字符串
        String originalQueryString = DuckDBToStringVisitor.asString(select);
        // 执行原始查询并获取结果集（仅第一列）
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        // 随机决定是否保留DISTINCT
        if (Randomly.getBoolean()) {
            select.setDistinct(false);
        }
        // 设置WHERE子句为预定义的谓词
        select.setWhereClause(predicate);
        // 生成第一个变体查询字符串
        String firstQueryString = DuckDBToStringVisitor.asString(select);
        // 设置WHERE子句为否定谓词
        select.setWhereClause(negatedPredicate);
        // 生成第二个变体查询字符串
        String secondQueryString = DuckDBToStringVisitor.asString(select);
        // 设置WHERE子句为IS NULL谓词
        select.setWhereClause(isNullPredicate);
        // 生成第三个变体查询字符串
        String thirdQueryString = DuckDBToStringVisitor.asString(select);
        // 创建一个列表用于存储合并后的查询字符串
        List<String> combinedString = new ArrayList<>();
        // 执行三个变体查询并合并结果（去重）
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        // 比较原始查询结果和合并后的变体查询结果
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state, ComparatorHelper::canonicalizeResultValue);
    }

}
