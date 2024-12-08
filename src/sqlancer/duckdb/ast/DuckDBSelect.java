package sqlancer.duckdb.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Select;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.DuckDBToStringVisitor;

public class DuckDBSelect extends SelectBase<DuckDBExpression>
        implements Select<DuckDBJoin, DuckDBExpression, DuckDBTable, DuckDBColumn>, DuckDBExpression {

    // 这个类继承了 SelectBase，实现了 Select 和 DuckDBExpression 接口，旨在构建并操作 SELECT 查询语句
    private boolean isDistinct; // select是否为去重查询

    public void setDistinct(boolean isDistinct) {
        // 设置是否为去重查询
        this.isDistinct = isDistinct;
    }

    public boolean isDistinct() {
        // 返回是否为去重查询
        return isDistinct;
    }

    @Override
    public void setJoinClauses(List<DuckDBJoin> joinStatements) {
        // 将 joinStatements 转换为 DuckDBExpression 类型的列表，并将其赋值给 joinList
        List<DuckDBExpression> expressions = joinStatements.stream().map(e -> (DuckDBExpression) e)
                .collect(Collectors.toList());
        setJoinList(expressions);
    }

    @Override
    public List<DuckDBJoin> getJoinClauses() {
        return getJoinList().stream().map(e -> (DuckDBJoin) e).collect(Collectors.toList());
    }

    @Override
    public String asString() {
        return DuckDBToStringVisitor.asString(this);
    }
}
