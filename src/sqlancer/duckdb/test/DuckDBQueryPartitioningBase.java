package sqlancer.duckdb.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBSchema.DuckDBTables;
import sqlancer.duckdb.ast.DuckDBColumnReference;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.ast.DuckDBTableReference;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;

public class DuckDBQueryPartitioningBase extends TernaryLogicPartitioningOracleBase<DuckDBExpression, DuckDBGlobalState>
        implements TestOracle<DuckDBGlobalState> {

    DuckDBSchema s; // s : DuckDB 的 Sehema
    DuckDBTables targetTables; // 目标表s（List）
    DuckDBExpressionGenerator gen; // DuckDB 的表达式生成器
    DuckDBSelect select; // DuckDBSelect 类旨在构建并操作 SELECT 查询语句

    public DuckDBQueryPartitioningBase(DuckDBGlobalState state) {
        super(state);
        DuckDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema(); // getSchema() : DuckDBSchema getSchema()
        // 随机选择非空表作为测试目标表s
        targetTables = s.getRandomTableNonEmptyTables(); // DuckDBTable DuckDBSchema.getRandomTableNonEmptyTables()
        gen = new DuckDBExpressionGenerator(state).setColumns(targetTables.getColumns()); // 创建一个新的表达式生成器，并设置它使用目标表的列
        initializeTernaryPredicateVariants(); // 初始化用于测试的三元谓词变体
        /* 创建 Select 语句 */
        select = new DuckDBSelect(); // select 是 DuckDBSelect 类型的对象，创建一个新的SELECT语句对象
        select.setFetchColumns(generateFetchColumns()); // 调用select的set方法,generateFetchColumns方法获得所有表的列
        List<DuckDBTable> tables = targetTables.getTables(); // 得到所有表-> tables
        List<DuckDBTableReference> tableList = tables.stream().map(t -> new DuckDBTableReference(t))
                .collect(Collectors.toList()); // 将表转换为表引用对象的列表
        /* 设置 Join 子句 */
        List<DuckDBJoin> joins = DuckDBJoin.getJoins(tableList, state); // 一个 DuckDBJoin 的列表，每个元素表示一个表与另一个表的 Join 操作
        select.setJoinList(joins.stream().collect(Collectors.toList())); // 设置 Select 的 Join 子句
        /* 设置 From 子句 */
        select.setFromList(tableList.stream().collect(Collectors.toList())); // 设置 Select 的 From 子句
        /* 设置 Where 子句为 null */
        select.setWhereClause(null); // 设置 Select 的Where 子句为 NULL
    }

    // 返回当前所有表的所有列
    List<DuckDBExpression> generateFetchColumns() {
        List<DuckDBExpression> columns = new ArrayList<>(); // 初始化一个空列表 columns，用来存储生成的列表达式。
        if (Randomly.getBoolean()) { // Randomly.getBoolean() 是一个随机布尔值生成器，用于随机决定列的选择方式
            // true
            // 创建一个表示 "所有列" 的列引用 DuckDBColumn("*", ...)
            // DuckDBColumn(String name, DuckDBCompositeDataType columnType, boolean
            // isPrimaryKey, boolean isNullable)
            columns.add(new DuckDBColumnReference(new DuckDBColumn("*", null, false, false)));
        } else {
            // false
            // Randomly.nonEmptySubset(...) 从所有列中随机选择一个非空子集
            // targetTables.getColumns() 返回当前目标表的所有列。
            // 使用 stream().map(...) 将每个列转换为 DuckDBColumnReference 对象
            // collect(Collectors.toList()) 将结果收集为一个列表并赋值给 columns
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream().map(c -> new DuckDBColumnReference(c))
                    .collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<DuckDBExpression> getGen() {
        return gen;
    }

}
