package sqlancer.duckdb.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.duckdb.DuckDBSchema;

public class DuckDBTableReference extends TableReferenceNode<DuckDBExpression, DuckDBSchema.DuckDBTable>
        implements DuckDBExpression {
    public DuckDBTableReference(DuckDBSchema.DuckDBTable table) {
        super(table);
    }
    // 在 SQL 查询中，每个表都会在 AST 中表示为一个节点
    // SELECT * FROM my_table;
    // 在这里，my_table 就是一个表引用，可以由 DuckDBTableReference 来表示
}
