package sqlancer.duckdb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBDataType;
import sqlancer.duckdb.DuckDBToStringVisitor;
import sqlancer.duckdb.ast.DuckDBExpression;

public class DuckDBTableGenerator {

    public SQLQueryAdapter getQuery(DuckDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        // String是不可变类型，如果每次堆String类型修改，实际上是在创建新的String类型
        // StringBuilder 是可变字符串类型
        StringBuilder sb = new StringBuilder();
        String tableName = globalState.getSchema().getFreeTableName();
        sb.append("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(");
        // getNewColumns()方法是返回一个新生成的列集合
        List<DuckDBColumn> columns = getNewColumns();
        // 我猜是将这个列集合更新这个表得到globalState相关信息.............................
        UntypedExpressionGenerator<DuckDBExpression, DuckDBColumn> gen = new DuckDBExpressionGenerator(globalState)
                .setColumns(columns);
        // 有多少个列就循环多少次，每个列的信息可能不一样
        // 使用 StringBuilder 对 sb 进行构造，生成类似以下形式的 SQL 列定义：
        // col_name1 col_type [constraints], col_name2 col_type [constraints], ...
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            // 获取列的名称 (columns.get(i).getName()) 和类型 (columns.get(i).getType()) 并拼接。
            sb.append(columns.get(i).getName());
            sb.append(" ");
            sb.append(columns.get(i).getType());

            // 条件性添加 COLLATE 子句(如果当前列的类型是 VARCHAR，并且满足随机条件)
            /*
             * 这些关键字是与字符串比较相关的 排序规则
             * "NOCASE" 忽略字符串的大小写
             * "NOACCENT" 忽略字符串中的重音符号
             * "NOACCENT.NOCASE" 同时忽略大小写和重音符号
             * "C" 使用标准的 C 语言字符比较规则
             * "POSIX" 使用 POSIX 匹配规则（与 Unix 标准兼容）
             */
            if (globalState.getDbmsSpecificOptions().testCollate && Randomly.getBooleanWithRatherLowProbability()
                    && columns.get(i).getType().getPrimitiveDataType() == DuckDBDataType.VARCHAR) {
                sb.append(" COLLATE ");
                sb.append(getRandomCollate());
            }
            // 条件性添加 UNIQUE 约束
            // 随机为某些列添加 UNIQUE 约束，保证该列的值必须唯一
            if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" UNIQUE");
            }
            // 条件性添加 NOT NULL 约束
            // 随机为某些列添加 NOT NULL 约束，要求该列的值不能为 NULL
            if (globalState.getDbmsSpecificOptions().testNotNullConstraints
                    && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" NOT NULL");
            }
            // 条件性添加 CHECK 约束
            // 随机为某些列添加 CHECK 约束，确保列的值满足特定条件：CHECK (<generated_expression>)
            if (globalState.getDbmsSpecificOptions().testCheckConstraints
                    && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" CHECK(");
                sb.append(DuckDBToStringVisitor.asString(gen.generateExpression()));
                DuckDBErrors.addExpressionErrors(errors);
                sb.append(")");
            }
            // 条件性添加 DEFAULT 子句
            // 随机为某些列添加默认值 DEFAULT (<generated_constant>) 和这个列的数据类型有关
            if (Randomly.getBoolean() && globalState.getDbmsSpecificOptions().testDefaultValues) {
                sb.append(" DEFAULT(");
                sb.append(DuckDBToStringVisitor.asString(gen.generateConstant()));
                sb.append(")");
            }
        }
        // 只有在用户选择测试索引，并且随机条件满足时，才会执行主键生成的逻辑
        if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBoolean()) {
            // 将可能出现的错误添加到 errors 列表中
            /*
             * 该错误可能发生在以下情况下：
             * 某些列的数据类型不支持用作主键（如大对象类型）。
             * 主键的组合不合法。
             */
            errors.add("Invalid type for index");
            // 随机从 columns（当前表的所有列）中选择非空子集，用作主键列。
            List<DuckDBColumn> primaryKeyColumns = Randomly.nonEmptySubset(columns);
            sb.append(", PRIMARY KEY(");
            sb.append(primaryKeyColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
        }
        sb.append(")");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    // 从 {"NOCASE", "NOACCENT", "NOACCENT.NOCASE", "C", "POSIX"} 这些选项中随机返回一个
    public static String getRandomCollate() {
        return Randomly.fromOptions("NOCASE", "NOACCENT", "NOACCENT.NOCASE", "C", "POSIX");
    }

    // 返回一个集合，这个集合里有一组列 DuckDBColumn(columnName, columnType, false, false)
    private static List<DuckDBColumn> getNewColumns() {
        List<DuckDBColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            DuckDBCompositeDataType columnType = DuckDBCompositeDataType.getRandomWithoutNull();
            columns.add(new DuckDBColumn(columnName, columnType, false, false));
        }
        return columns;
    }

}
