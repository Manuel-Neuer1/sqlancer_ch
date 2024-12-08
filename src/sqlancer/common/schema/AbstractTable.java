package sqlancer.common.schema;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;

public abstract class AbstractTable<C extends AbstractTableColumn<?, ?>, I extends TableIndex, G extends GlobalState<?, ?, ?>>
        implements Comparable<AbstractTable<?, ?, ?>> {

    protected static final int NO_ROW_COUNT_AVAILABLE = -1;
    protected final String name;
    private final List<C> columns;
    private final List<I> indexes;
    private final boolean isView;
    protected long rowCount = NO_ROW_COUNT_AVAILABLE;

    protected AbstractTable(String name, List<C> columns, List<I> indexes, boolean isView) {
        this.name = name;
        this.indexes = indexes;
        this.isView = isView;
        this.columns = Collections.unmodifiableList(columns);
    }

    public String getName() {
        return name;
    }

    @Override
    public int compareTo(AbstractTable<?, ?, ?> o) {
        return o.getName().compareTo(getName());
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getName());
        sb.append("\n");
        for (C c : columns) {
            sb.append("\t").append(c).append("\n");
        }
        return sb.toString();
    }

    public List<I> getIndexes() {
        return indexes;
    }

    public List<C> getColumns() {
        return columns;
    }

    public String getColumnsAsString() {
        return columns.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
    }

    public C getRandomColumn() {
        return Randomly.fromList(columns);
    }

    public C getRandomColumnOrBailout(Predicate<C> predicate) {
        List<C> relevantColumns = columns.stream().filter(predicate).collect(Collectors.toList());
        if (relevantColumns.isEmpty()) {
            throw new IgnoreMeException();
        }

        return Randomly.fromList(relevantColumns);
    }

    public boolean hasIndexes() {
        return !indexes.isEmpty();
    }

    public TableIndex getRandomIndex() {
        return Randomly.fromList(indexes);
    }

    public List<C> getRandomNonEmptyColumnSubset() {
        return Randomly.nonEmptySubset(getColumns());
    }

    public List<C> getRandomNonEmptyColumnSubsetFilter(Predicate<C> predicate) {
        return Randomly.nonEmptySubset(getColumns().stream().filter(predicate).collect(Collectors.toList()));
    }

    public List<C> getRandomNonEmptyColumnSubset(int size) {
        return Randomly.nonEmptySubset(getColumns(), size);
    }

    public boolean isView() {
        return isView;
    }

    // 生成一个不重复的列名
    public String getFreeColumnName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            // 如果返回 true，则会生成一个介于 0 到 100 之间的随机整数作为 i 的值
            i = (int) Randomly.getNotCachedInteger(0, 100);// 生成不在缓存中的整数的函数，范围从 0 到 100
        }
        // 该循环会不断生成列名并检查是否已经存在，直到生成一个未被使用的列名为止
        do {
            // 生成一个列名 c0, c1, c2 ...，i++ 表示每次生成一个新列名时，i 会自增
            String columnName = String.format("c%d", i++);
            // columns 是一个包含所有列的集合 private final List<C> columns;
            // noneMatch 方法会遍历这些列，检查是否有任何一列的名字与 columnName 匹配
            if (columns.stream().noneMatch(t -> t.getName().contentEquals(columnName))) {
                // 如果没有找到任何匹配的列名（即 noneMatch 返回 true）
                // 表示生成的列名是唯一的，可以返回该列名
                return columnName;
            }
        } while (true);

    }

    public void recomputeCount() {
        rowCount = NO_ROW_COUNT_AVAILABLE;
    }

    public abstract long getNrRows(G globalState);
}
