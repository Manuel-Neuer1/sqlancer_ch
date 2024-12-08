package sqlancer.duckdb.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Join;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;

public class DuckDBJoin implements DuckDBExpression, Join<DuckDBExpression, DuckDBTable, DuckDBColumn> {

    private final DuckDBTableReference leftTable;
    private final DuckDBTableReference rightTable;
    private final JoinType joinType;
    private DuckDBExpression onCondition;
    private OuterType outerType;

    public enum JoinType {
        INNER, NATURAL, LEFT, RIGHT;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum OuterType {
        FULL, LEFT, RIGHT;

        public static OuterType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    // DuckDB 的构造函数
    public DuckDBJoin(DuckDBTableReference leftTable, DuckDBTableReference rightTable, JoinType joinType,
            DuckDBExpression whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public DuckDBTableReference getLeftTable() {
        return leftTable;
    }

    public DuckDBTableReference getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public DuckDBExpression getOnCondition() {
        return onCondition;
    }

    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public OuterType getOuterType() {
        return outerType;
    }

    /*
     * tableList:
     * 一个 DuckDBTableReference 的列表，每个元素代表一个表。
     * 表示可以参与 Join 操作的表集合。
     * 
     * globalState:
     * 表示全局状态，包含随机数生成器和其他辅助信息，用于生成表达式时的上下文。
     * 
     * 返回值：
     * 一个 DuckDBJoin 的列表，每个元素表示一个表与另一个表的 Join 操作
     */
    public static List<DuckDBJoin> getJoins(List<DuckDBTableReference> tableList, DuckDBGlobalState globalState) {
        List<DuckDBJoin> joinExpressions = new ArrayList<>();// 创建一个空的 DuckDBJoin 列表，用于存储生成的 Join 表达式
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            // 至少需要两个表才能进行 Join，在概率满足的情况下才会执行 Join 操作
            // 从 tableList 中取出前两个表
            // leftTable: 第一个表
            // rightTable: 第二个表
            // remove(0) 会移除列表中的第一个元素，确保这些表不会被重复使用
            DuckDBTableReference leftTable = tableList.remove(0);
            DuckDBTableReference rightTable = tableList.remove(0);
            // 将 leftTable 和 rightTable 中的所有列组合成一个新列表 columns，为 Join 条件生成器提供可用的列集合
            List<DuckDBColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            // 创建一个 DuckDBExpressionGenerator 对象，用于生成 Join 的条件表达式
            DuckDBExpressionGenerator joinGen = new DuckDBExpressionGenerator(globalState).setColumns(columns);
            // 随机选择 Join 类型并创建 Join
            // 这里我想将只生成leftjoin
            // 这里我想判断DuckDBJoin.joinType是否是Left
            // joinExpressions.add(DuckDBJoin.createLeftOuterJoin(leftTable, rightTable,
            // joinGen.generateExpression()));
            switch (DuckDBJoin.JoinType.getRandom()) {
                case INNER: // createInnerJoin: 生成 INNER JOIN，需要一个条件表达式
                    joinExpressions
                            .add(DuckDBJoin.createInnerJoin(leftTable, rightTable,
                                    joinGen.generateExpression()));
                    break;
                case NATURAL: // createNaturalJoin: 生成 NATURAL JOIN，随机决定是否为 FULL、LEFT 或 RIGHT
                    joinExpressions.add(DuckDBJoin.createNaturalJoin(leftTable, rightTable,
                            OuterType.getRandom()));
                    break;
                case LEFT: //
                    joinExpressions
                            .add(DuckDBJoin.createLeftOuterJoin(leftTable, rightTable,
                                    joinGen.generateExpression()));
                    break;
                case RIGHT: //
                    joinExpressions
                            .add(DuckDBJoin.createRightOuterJoin(leftTable, rightTable,
                                    joinGen.generateExpression()));
                    break;
                default:
                    throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    public static DuckDBJoin createRightOuterJoin(DuckDBTableReference left, DuckDBTableReference right,
            DuckDBExpression predicate) {
        return new DuckDBJoin(left, right, JoinType.RIGHT, predicate);
    }

    public static DuckDBJoin createLeftOuterJoin(DuckDBTableReference left, DuckDBTableReference right,
            DuckDBExpression predicate) {
        return new DuckDBJoin(left, right, JoinType.LEFT, predicate);
    }

    public static DuckDBJoin createInnerJoin(DuckDBTableReference left, DuckDBTableReference right,
            DuckDBExpression predicate) {
        return new DuckDBJoin(left, right, JoinType.INNER, predicate);
    }

    public static DuckDBJoin createNaturalJoin(DuckDBTableReference left, DuckDBTableReference right,
            OuterType naturalJoinType) {
        DuckDBJoin join = new DuckDBJoin(left, right, JoinType.NATURAL, null);
        join.setOuterType(naturalJoinType);
        return join;
    }

    @Override
    public void setOnClause(DuckDBExpression onClause) {
        this.onCondition = onClause;
    }
}
