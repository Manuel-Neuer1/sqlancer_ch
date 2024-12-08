package sqlancer.duckdb;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.duckdb.ast.DuckDBConstant;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBJoin;
import sqlancer.duckdb.ast.DuckDBSelect;

public class DuckDBToStringVisitor extends NewToStringVisitor<DuckDBExpression> {

    @Override
    public void visitSpecific(DuckDBExpression expr) {
        // check expr 是哪个类的实例
        if (expr instanceof DuckDBConstant) {
            visit((DuckDBConstant) expr);
        } else if (expr instanceof DuckDBSelect) {
            visit((DuckDBSelect) expr);
        } else if (expr instanceof DuckDBJoin) {
            visit((DuckDBJoin) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(DuckDBJoin join) {
        // 将一个 DuckDBJoin 对象转换为字符串表示，以生成 SQL 查询中的 JOIN 子句
        // 递归地访问 DuckDBJoin 的各个部分（左表、右表、JOIN 类型和条件），并构造对应的 SQL 语句部分
        visit((TableReferenceNode<DuckDBExpression, DuckDBSchema.DuckDBTable>) join.getLeftTable()); // 这一步是 get 左表名
        sb.append(" ");
        sb.append(join.getJoinType());// INNER, NATURAL, LEFT, RIGHT;
        sb.append(" ");
        if (join.getOuterType() != null) { // FULL, LEFT, RIGHT;
            sb.append(join.getOuterType());
        }
        sb.append(" JOIN ");
        visit((TableReferenceNode<DuckDBExpression, DuckDBSchema.DuckDBTable>) join.getRightTable()); //// 这一步是 get 右表名
        if (join.getOnCondition() != null) {// e.g. t1 LEFT JOIN t2 ON t1.id = t2.id
            sb.append(" ON ");
            visit(join.getOnCondition());// 把 condition加入sb中
        }
    }

    private void visit(DuckDBConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(DuckDBSelect select) {
        sb.append("SELECT ");
        if (select.isDistinct()) {
            sb.append("DISTINCT ");
        }
        visit(select.getFetchColumns());// 要project的列字段 SELECT （DISTINCT） t1.c1
        sb.append(" FROM ");
        visit(select.getFromList()); // ... FROM t1, t2, t3, ...
        if (!select.getFromList().isEmpty() && !select.getJoinList().isEmpty()) {
            sb.append(", ");
        }
        if (!select.getJoinList().isEmpty()) {
            visit(select.getJoinList());
        }
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
        if (!select.getGroupByExpressions().isEmpty()) {
            sb.append(" GROUP BY ");
            visit(select.getGroupByExpressions());
        }
        if (select.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(select.getHavingClause());
        }
        if (!select.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByClauses());
        }
        if (select.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(select.getLimitClause());
        }
        if (select.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(select.getOffsetClause());
        }
    }

    public static String asString(DuckDBExpression expr) {
        DuckDBToStringVisitor visitor = new DuckDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
