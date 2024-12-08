package sqlancer.duckdb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.duckdb.DuckDBErrors;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;

//final 类，意味着它不能被继承
public final class DuckDBAlterTableGenerator {
    private DuckDBAlterTableGenerator() {
    }

    // 一个枚举类型 Action，表示三种不同的 ALTER TABLE 操作：添加列、修改列数据类型、删除列
    enum Action {
        ADD_COLUMN, ALTER_COLUMN, DROP_COLUMN
    }

    // 接受 DuckDBGlobalState 类型的参数 globalState
    // globalState 是整个数据库状态的代表，包含了当前数据库的 schema 等信息。
    public static SQLQueryAdapter getQuery(DuckDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors(); // 用来保存执行 SQL 查询时可能出现的错误信息
        errors.add(" does not have a column with name \"rowid\"");
        errors.add("Table does not contain column rowid referenced in alter statement");
        StringBuilder sb = new StringBuilder("ALTER TABLE "); // sb 用于构建最终的 SQL 查询字符串
        DuckDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());// 从数据库模式中随机选择一个表，排除视图
        // gen 是用于生成随机 SQL 表达式的类，这里它使用当前表的列信息来生成表达式
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(table.getName());// 这里获取了当前表名，然后添加到 sb 这个String类型的字符串
        sb.append(" ");// 在添加一个空格
        Action action = Randomly.fromOptions(Action.values()); //
        switch (action) {
            case ADD_COLUMN:
                sb.append("ADD COLUMN "); // sb 字符串添加 "ADD COLUMN "
                // getFreeColumnName() 方法在AbstTable.java 文件中，获取一个没用使用的列名，如c3
                String columnName = table.getFreeColumnName();
                sb.append(columnName); // 把列名添加到sb中
                sb.append(" ");// 在添加一个空格
                // DuckDBCompositeDataType：这是一个类在 DuckDBSchema.java 文件中
                // getRandomWithoutNull()：这是 DuckDBCompositeDataType 类中的一个方法。
                // 这个方法的作用是生成一个随机的数据类型，但不包含 NULL 数据类型。
                // 调用 toString() 会将 getRandomWithoutNull() 返回的类型转换为字符串
                sb.append(DuckDBCompositeDataType.getRandomWithoutNull().toString());
                break;
            case ALTER_COLUMN:
                sb.append("ALTER COLUMN ");
                sb.append(table.getRandomColumn().getName());
                sb.append(" SET DATA TYPE ");
                sb.append(DuckDBCompositeDataType.getRandomWithoutNull().toString());
                if (Randomly.getBoolean()) {
                    sb.append(" USING ");
                    DuckDBErrors.addExpressionErrors(errors);
                    sb.append(DuckDBToStringVisitor.asString(gen.generateExpression()));
                }
                errors.add("Cannot change the type of this column: an index depends on it!");
                errors.add("Cannot change the type of a column that has a UNIQUE or PRIMARY KEY constraint specified");
                errors.add("Unimplemented type for cast");
                errors.add("Conversion:");
                errors.add("Cannot change the type of a column that has a CHECK constraint specified");
                break;
            case DROP_COLUMN:
                sb.append("DROP COLUMN ");
                sb.append(table.getRandomColumn().getName());
                errors.add("named in key does not exist"); // TODO
                errors.add("Cannot drop this column:");
                errors.add("Cannot drop column: table only has one column remaining!");
                errors.add("because there is a CHECK constraint that depends on it");
                errors.add("because there is a UNIQUE constraint that depends on it");
                break;
            default:
                throw new AssertionError(action);
        }
        // 构造好的 SQL 查询通过 SQLQueryAdapter 返回，并传递可能出现的错误信息
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
