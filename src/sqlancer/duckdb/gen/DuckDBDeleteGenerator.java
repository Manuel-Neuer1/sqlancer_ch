package sqlancer.duckdb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;

public final class DuckDBDeleteGenerator {

    private DuckDBDeleteGenerator() {
    }

    // 这个方法负责生成一个随机的 DELETE 语句
    public static SQLQueryAdapter generate(DuckDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        ExpectedErrors errors = new ExpectedErrors();
        // 从数据库模式中随机选择一个表。
        // t -> !t.isView() 表示排除了视图（isView() 返回 true 表示是视图，排除视图只选择普通的表）
        DuckDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        // 生成一个随机的布尔值。如果是 true，则添加 WHERE 子句，表示在删除时需要添加条件
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(DuckDBToStringVisitor.asString(
                    new DuckDBExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
        DuckDBErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
