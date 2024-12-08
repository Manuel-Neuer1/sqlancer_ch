package sqlancer.duckdb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBSchema.DuckDBTables;
import sqlancer.duckdb.ast.DuckDBConstant;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.ast.DuckDBTableReference;

public final class DuckDBRandomQuerySynthesizer {
    // 工具类（Utility Class）：DuckDB 随机查询合成
    // 方法 generateSelect 是静态方法，意味着可以直接通过类名调用，而不需要实例化对象
    private DuckDBRandomQuerySynthesizer() {
    }

    // 生成一个随机的 DuckDB SELECT 查询
    public static DuckDBSelect generateSelect(DuckDBGlobalState globalState, int nrColumns) {
        // 1 取表
        DuckDBTables targetTables = globalState.getSchema().getRandomTableNonEmptyTables();
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState)
                .setColumns(targetTables.getColumns());
        DuckDBSelect select = new DuckDBSelect();
        // TODO: distinct
        // select.setDistinct(Randomly.getBoolean());
        // boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
        List<DuckDBExpression> columns = new ArrayList<>();
        for (int i = 0; i < nrColumns; i++) {
            // if (allowAggregates && Randomly.getBoolean()) {
            DuckDBExpression expression = gen.generateExpression();
            columns.add(expression);
            // } else {
            // columns.add(gen());
            // }
        }
        select.setFetchColumns(columns);
        List<DuckDBTable> tables = targetTables.getTables();
        List<DuckDBTableReference> tableList = tables.stream().map(t -> new DuckDBTableReference(t))
                .collect(Collectors.toList());
        List<DuckDBJoin> joins = DuckDBJoin.getJoins(tableList, globalState);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }

        if (Randomly.getBoolean()) {
            select.setLimitClause(DuckDBConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(
                    DuckDBConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setHavingClause(gen.generateHavingClause());
        }
        return select;
    }

}
