package sqlancer;

import sqlancer.common.query.Query;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;

public abstract class GlobalState<O extends DBMSSpecificOptions<?>, S extends AbstractSchema<?, ?>, C extends SQLancerDBConnection> {

    protected C databaseConnection;// 保存数据库连接实例
    private Randomly r;// 随机数生成器
    private MainOptions options; // 选项 --help 之类的
    private O dbmsSpecificOptions;// 数据库管理系统特定的选项。
    // 用途：存储与数据库特性相关的配置项，允许 SQLancer 针对不同的数据库管理系统定制行为
    private S schema;// 数据库的模式（schema），描述当前数据库的结构，即数据库中表、列等结构的描述。
    private Main.StateLogger logger;
    private StateToReproduce state;
    private Main.QueryManager<C> manager;
    private String databaseName; // 数据库的名称

    // 用于设置和获取数据库连接
    public void setConnection(C con) {
        this.databaseConnection = con;
    }

    public C getConnection() {
        return databaseConnection;
    }

    // 用于设置和获取数据库管理系统特定的选项（比如 MySQL、PostgreSQL 的特定配置）
    @SuppressWarnings("unchecked")
    public void setDbmsSpecificOptions(Object dbmsSpecificOptions) {
        this.dbmsSpecificOptions = (O) dbmsSpecificOptions;
    }

    public O getDbmsSpecificOptions() {
        return dbmsSpecificOptions;
    }

    // 用于设置和获取随机数生成器
    public void setRandomly(Randomly r) {
        this.r = r;
    }

    public Randomly getRandomly() {
        return r;
    }

    public MainOptions getOptions() {
        return options;
    }

    public void setMainOptions(MainOptions options) {
        this.options = options;
    }

    public void setStateLogger(Main.StateLogger logger) {
        this.logger = logger;
    }

    public Main.StateLogger getLogger() {
        return logger;
    }

    public void setState(StateToReproduce state) {
        this.state = state;
    }

    public StateToReproduce getState() {
        return state;
    }

    public Main.QueryManager<C> getManager() {
        return manager;
    }

    public void setManager(Main.QueryManager<C> manager) {
        this.manager = manager;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    // 用于执行查询的前置操作，例如记录查询执行时间、日志等
    private ExecutionTimer executePrologue(Query<?> q) throws Exception {
        boolean logExecutionTime = getOptions().logExecutionTime();// MainOptions类的logExecutionTime()方法返回true
        ExecutionTimer timer = null;
        if (logExecutionTime) {
            timer = new ExecutionTimer().start();
        }
        if (getOptions().printAllStatements()) {
            System.out.println(q.getLogString());//// q.getLogString() 方法的作用是返回当前 SQL 查询的字符串表示形式
        }
        if (getOptions().logEachSelect()) {
            if (logExecutionTime) {
                getLogger().writeCurrentNoLineBreak(q.getLogString());
            } else {
                getLogger().writeCurrent(q.getLogString());
            }
        }
        return timer;
    }

    protected abstract void executeEpilogue(Query<?> q, boolean success, ExecutionTimer timer) throws Exception;

    public boolean executeStatement(Query<C> q, String... fills) throws Exception {
        ExecutionTimer timer = executePrologue(q);
        boolean success = manager.execute(q, fills);
        executeEpilogue(q, success, timer);
        return success;
    }

    public SQLancerResultSet executeStatementAndGet(Query<C> q, String... fills) throws Exception {
        ExecutionTimer timer = executePrologue(q);
        SQLancerResultSet result = manager.executeAndGet(q, fills);
        boolean success = result != null;
        if (success) {
            result.registerEpilogue(() -> {
                try {
                    executeEpilogue(q, success, timer);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
        }
        return result;
    }

    public S getSchema() {
        if (schema == null) {
            try {
                updateSchema();
            } catch (Exception e) {
                throw new AssertionError(e.getMessage());
            }
        }
        return schema;
    }

    protected void setSchema(S schema) {
        this.schema = schema;
    }

    public void updateSchema() throws Exception {
        setSchema(readSchema());
        for (AbstractTable<?, ?, ?> table : schema.getDatabaseTables()) {
            table.recomputeCount();
        }
    }

    protected abstract S readSchema() throws Exception;

}
