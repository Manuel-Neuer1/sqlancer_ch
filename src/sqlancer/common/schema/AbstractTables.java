package sqlancer.common.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AbstractTables<T extends AbstractTable<C, ?, ?>, C extends AbstractTableColumn<?, ?>> {

    private final List<T> tables;
    private final List<C> columns; // 这个cols是一个list，保存所有的表的所有列

    public AbstractTables(List<T> tables) {
        this.tables = tables;
        columns = new ArrayList<>();
        for (T t : tables) {
            // 这里调用的是AbstractTable类的getColumns方法
            // 返回当前表t的所有列
            columns.addAll(t.getColumns());
        }
    }

    public String tableNamesAsString() {// t1.name, t2.name, t3.name
        return tables.stream().map(t -> t.getName()).collect(Collectors.joining(", "));
    }

    public List<T> getTables() {
        return tables;
    }

    public List<C> getColumns() {
        return columns;
    }

    public String columnNamesAsString(Function<C, String> function) { // t1.c0.name, t1.c1.name, t2.c0.name, t3.c0.name
        return getColumns().stream().map(function).collect(Collectors.joining(", "));
    }

}
