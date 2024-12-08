package sqlancer.common.schema;

import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.SQLGlobalState;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;

public class AbstractRelationalTable<C extends AbstractTableColumn<?, ?>, I extends TableIndex, G extends SQLGlobalState<?, ?>>
        extends AbstractTable<C, I, G> {

    public AbstractRelationalTable(String name, List<C> columns, List<I> indexes, boolean isView) {
        super(name, columns, indexes, isView);
    }

    @Override
    public long getNrRows(G globalState) {
        if (rowCount == NO_ROW_COUNT_AVAILABLE) {
            // name 是继承 AbstractTable 的
            SQLQueryAdapter q = new SQLQueryAdapter("SELECT COUNT(*) FROM " + name);
            // executeAndGet(G globalState, String... fills)
            // 方法：执行查询，并返回结果集；fills为空，用Statement
            try (SQLancerResultSet query = q.executeAndGet(globalState)) {
                if (query == null) {
                    throw new IgnoreMeException();
                }
                // query对象有个ResultSet的成员变量，调用ResultSet的next方法
                query.next();
                rowCount = query.getLong(1);
                return rowCount;
            } catch (Throwable t) {
                // an exception might be expected, for example, when invalid view is created
                throw new IgnoreMeException();
            }
        } else {
            return rowCount;
        }
    }

}
