package sqlancer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.JCommander.Builder;

import sqlancer.citus.CitusProvider;
import sqlancer.clickhouse.ClickHouseProvider;
import sqlancer.cnosdb.CnosDBProvider;
import sqlancer.cockroachdb.CockroachDBProvider;
import sqlancer.common.log.Loggable;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.databend.DatabendProvider;
import sqlancer.doris.DorisProvider;
import sqlancer.duckdb.DuckDBProvider;
import sqlancer.h2.H2Provider;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.mariadb.MariaDBProvider;
import sqlancer.materialize.MaterializeProvider;
import sqlancer.mysql.MySQLProvider;
import sqlancer.oceanbase.OceanBaseProvider;
import sqlancer.postgres.PostgresProvider;
import sqlancer.presto.PrestoProvider;
import sqlancer.questdb.QuestDBProvider;
import sqlancer.sqlite3.SQLite3Provider;
import sqlancer.tidb.TiDBProvider;
import sqlancer.yugabyte.ycql.YCQLProvider;
import sqlancer.yugabyte.ysql.YSQLProvider;

public final class Main {

    public static final File LOG_DIRECTORY = new File("logs");
    public static volatile AtomicLong nrQueries = new AtomicLong();
    public static volatile AtomicLong nrDatabases = new AtomicLong();
    public static volatile AtomicLong nrSuccessfulActions = new AtomicLong();
    public static volatile AtomicLong nrUnsuccessfulActions = new AtomicLong();
    public static volatile AtomicLong threadsShutdown = new AtomicLong();
    static boolean progressMonitorStarted;

    static {
        System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "ERROR");
        if (!LOG_DIRECTORY.exists()) {
            LOG_DIRECTORY.mkdir();
        }
        System.out.println("Current working directory: " + System.getProperty("user.dir"));
        // Current working directory: /home/lyuce/BugProject/sqlancer/target
    }

    private Main() {
    }

    public static final class StateLogger {

        private final File loggerFile;
        private File curFile;
        private File queryPlanFile;
        private File reduceFile;
        private FileWriter logFileWriter;
        public FileWriter currentFileWriter;
        private FileWriter queryPlanFileWriter;
        private FileWriter reduceFileWriter;

        private static final List<String> INITIALIZED_PROVIDER_NAMES = new ArrayList<>();
        private final boolean logEachSelect;
        private final boolean logQueryPlan;

        private final boolean useReducer;
        private final DatabaseProvider<?, ?, ?> databaseProvider;

        private static final class AlsoWriteToConsoleFileWriter extends FileWriter {

            AlsoWriteToConsoleFileWriter(File file) throws IOException {
                super(file);
            }

            @Override
            public Writer append(CharSequence arg0) throws IOException {
                System.err.println(arg0);
                return super.append(arg0);
            }

            @Override
            public void write(String str) throws IOException {
                System.err.println(str);
                super.write(str);
            }
        }

        public StateLogger(String databaseName, DatabaseProvider<?, ?, ?> provider, MainOptions options) {
            File dir = new File(LOG_DIRECTORY, provider.getDBMSName());
            if (dir.exists() && !dir.isDirectory()) {
                throw new AssertionError(dir);
            }
            ensureExistsAndIsEmpty(dir, provider);
            loggerFile = new File(dir, databaseName + ".log");
            System.out.println(loggerFile.getAbsolutePath());
            /*
             * /home/lyuce/BugProject/sqlancer/target/logs/sqlite3/database0.log
             * /home/lyuce/BugProject/sqlancer/target/logs/sqlite3/database1.log
             * /home/lyuce/BugProject/sqlancer/target/logs/sqlite3/database2.log
             * /home/lyuce/BugProject/sqlancer/target/logs/sqlite3/database3.log
             */
            if (!loggerFile.exists()) {
                System.out.println("No this file!!!");
            }
            logEachSelect = options.logEachSelect(); // 默认返回为true
            if (logEachSelect) {
                curFile = new File(dir, databaseName + "-cur.log");
            }
            logQueryPlan = options.logQueryPlan();
            if (logQueryPlan) {
                queryPlanFile = new File(dir, databaseName + "-plan.log");
            }
            this.useReducer = options.useReducer();
            if (useReducer) {
                File reduceFileDir = new File(dir, "reduce");
                if (!reduceFileDir.exists()) {
                    reduceFileDir.mkdir();
                }
                this.reduceFile = new File(reduceFileDir, databaseName + "-reduce.log");

            }
            this.databaseProvider = provider;
        }

        private void ensureExistsAndIsEmpty(File dir, DatabaseProvider<?, ?, ?> provider) {
            if (INITIALIZED_PROVIDER_NAMES.contains(provider.getDBMSName())) {
                return;
            }
            synchronized (INITIALIZED_PROVIDER_NAMES) {
                if (!dir.exists()) {
                    try {
                        Files.createDirectories(dir.toPath());
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
                File[] listFiles = dir.listFiles();
                assert listFiles != null : "directory was just created, so it should exist";
                for (File file : listFiles) {
                    if (!file.isDirectory()) {
                        file.delete();
                    }
                }
                INITIALIZED_PROVIDER_NAMES.add(provider.getDBMSName());
            }
        }

        private FileWriter getLogFileWriter() {
            if (logFileWriter == null) {
                try {
                    logFileWriter = new AlsoWriteToConsoleFileWriter(loggerFile);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            return logFileWriter;
        }

        public FileWriter getCurrentFileWriter() {
            if (!logEachSelect) {
                throw new UnsupportedOperationException();
            }
            if (currentFileWriter == null) {
                try {
                    currentFileWriter = new FileWriter(curFile, false);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            return currentFileWriter;
        }

        public FileWriter getQueryPlanFileWriter() {
            if (!logQueryPlan) {
                throw new UnsupportedOperationException();
            }
            if (queryPlanFileWriter == null) {
                try {
                    queryPlanFileWriter = new FileWriter(queryPlanFile, true);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            return queryPlanFileWriter;
        }

        public FileWriter getReduceFileWriter() {
            if (!useReducer) {
                throw new UnsupportedOperationException();
            }
            if (reduceFileWriter == null) {
                try {
                    reduceFileWriter = new FileWriter(reduceFile, false);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            return reduceFileWriter;
        }

        public void writeCurrent(StateToReproduce state) {
            if (!logEachSelect) {
                throw new UnsupportedOperationException();
            }
            printState(getCurrentFileWriter(), state);
            try {
                currentFileWriter.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void writeCurrent(String input) {
            write(databaseProvider.getLoggableFactory().createLoggable(input));
        }

        public void writeCurrentNoLineBreak(String input) {
            write(databaseProvider.getLoggableFactory().createLoggableWithNoLinebreak(input));
        }

        private void write(Loggable loggable) {
            if (!logEachSelect) {
                throw new UnsupportedOperationException();
            }
            try {
                getCurrentFileWriter().write(loggable.getLogString());

                currentFileWriter.flush();
            } catch (IOException e) {
                throw new AssertionError();
            }
        }

        public void writeQueryPlan(String queryPlan) {
            if (!logQueryPlan) {
                throw new UnsupportedOperationException();
            }
            try {
                getQueryPlanFileWriter().append(removeNamesFromQueryPlans(queryPlan));
                queryPlanFileWriter.flush();
            } catch (IOException e) {
                throw new AssertionError();
            }
        }

        public void logReducer(String reducerLog) {
            FileWriter reduceFileWriter = getReduceFileWriter();

            StringBuilder sb = new StringBuilder();
            sb.append("[reducer log] ");
            sb.append(reducerLog);
            try {
                reduceFileWriter.write(sb.toString());
            } catch (IOException e) {
                throw new AssertionError(e);
            } finally {
                try {
                    reduceFileWriter.flush();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        public void logReduced(StateToReproduce state) {
            FileWriter reduceFileWriter = getReduceFileWriter();

            StringBuilder sb = new StringBuilder();
            for (Query<?> s : state.getStatements()) {
                sb.append(databaseProvider.getLoggableFactory().createLoggable(s.getLogString()).getLogString());
            }
            try {
                reduceFileWriter.write(sb.toString());

            } catch (IOException e) {
                throw new AssertionError(e);
            } finally {
                try {
                    reduceFileWriter.flush();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }

        public void logException(Throwable reduce, StateToReproduce state) {
            Loggable stackTrace = getStackTrace(reduce);
            FileWriter logFileWriter2 = getLogFileWriter();
            try {
                logFileWriter2.write(stackTrace.getLogString());
                printState(logFileWriter2, state);
            } catch (IOException e) {
                throw new AssertionError(e);
            } finally {
                try {
                    logFileWriter2.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private Loggable getStackTrace(Throwable e1) {
            return databaseProvider.getLoggableFactory().convertStacktraceToLoggable(e1);
        }

        private void printState(FileWriter writer, StateToReproduce state) {
            StringBuilder sb = new StringBuilder();

            sb.append(databaseProvider.getLoggableFactory()
                    .getInfo(state.getDatabaseName(), state.getDatabaseVersion(), state.getSeedValue()).getLogString());

            for (Query<?> s : state.getStatements()) {
                sb.append(databaseProvider.getLoggableFactory().createLoggable(s.getLogString()).getLogString());
            }
            try {
                writer.write(sb.toString());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        private String removeNamesFromQueryPlans(String queryPlan) {
            String result = queryPlan;
            result = result.replaceAll("t[0-9]+", "t0"); // Avoid duplicate tables
            result = result.replaceAll("v[0-9]+", "v0"); // Avoid duplicate views
            result = result.replaceAll("i[0-9]+", "i0"); // Avoid duplicate indexes
            return result + "\n";
        }
    }

    public static class QueryManager<C extends SQLancerDBConnection> {

        private final GlobalState<?, ?, C> globalState;

        QueryManager(GlobalState<?, ?, C> globalState) {
            this.globalState = globalState;
        }

        public boolean execute(Query<C> q, String... fills) throws Exception {
            boolean success;
            success = q.execute(globalState, fills);
            Main.nrSuccessfulActions.addAndGet(1);
            if (globalState.getOptions().loggerPrintFailed() || success) {
                globalState.getState().logStatement(q);
            }
            return success;
        }

        public SQLancerResultSet executeAndGet(Query<C> q, String... fills) throws Exception {
            globalState.getState().logStatement(q);
            SQLancerResultSet result;
            result = q.executeAndGet(globalState, fills);
            Main.nrSuccessfulActions.addAndGet(1);
            return result;
        }

        public void incrementSelectQueryCount() {
            Main.nrQueries.addAndGet(1);
        }

        public Long getSelectQueryCount() {
            return Main.nrQueries.get();
        }

        public void incrementCreateDatabase() {
            Main.nrDatabases.addAndGet(1);
        }

    }

    /*
     * 
     * public static void main() 入口
     * 
     */
    public static void main(String[] args) {
        System.exit(executeMain(args));
    }

    public static class DBMSExecutor<G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> {

        private final DatabaseProvider<G, O, C> provider;
        private final MainOptions options;
        private final O command;
        private final String databaseName;
        private StateLogger logger;
        private StateToReproduce stateToRepro;
        private final Randomly r;

        public DBMSExecutor(DatabaseProvider<G, O, C> provider, MainOptions options, O dbmsSpecificOptions,
                String databaseName, Randomly r) {
            this.provider = provider;
            this.options = options;
            this.databaseName = databaseName;
            this.command = dbmsSpecificOptions;
            this.r = r;
        }

        private G createGlobalState() {
            try {
                return provider.getGlobalStateClass().getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        public O getCommand() {
            return command;
        }

        public void testConnection() throws Exception {
            G state = getInitializedGlobalState(options.getRandomSeed());
            try (SQLancerDBConnection con = provider.createDatabase(state)) {
                return;
            }
        }

        public void run() throws Exception {
            G state = createGlobalState();
            stateToRepro = provider.getStateToReproduce(databaseName);
            stateToRepro.seedValue = r.getSeed();
            state.setState(stateToRepro);
            logger = new StateLogger(databaseName, provider, options);
            state.setRandomly(r);
            state.setDatabaseName(databaseName);
            state.setMainOptions(options);
            state.setDbmsSpecificOptions(command);
            try (C con = provider.createDatabase(state)) {
                QueryManager<C> manager = new QueryManager<>(state);
                try {
                    stateToRepro.databaseVersion = con.getDatabaseVersion();
                } catch (Exception e) {
                    // ignore
                }
                state.setConnection(con);
                state.setStateLogger(logger);
                state.setManager(manager);
                if (options.logEachSelect()) {
                    logger.writeCurrent(state.getState());
                }
                Reproducer<G> reproducer = null;
                if (options.enableQPG()) {
                    provider.generateAndTestDatabaseWithQueryPlanGuidance(state);
                } else {
                    reproducer = provider.generateAndTestDatabase(state);
                }
                try {
                    logger.getCurrentFileWriter().close();
                    logger.currentFileWriter = null;
                } catch (IOException e) {
                    throw new AssertionError(e);
                }

                if (options.reduceAST() && !options.useReducer()) {
                    throw new AssertionError("To reduce AST, use-reducer option must be enabled first");
                }
                if (options.useReducer()) {
                    if (reproducer == null) {
                        logger.getReduceFileWriter().write("current oracle does not support experimental reducer.");
                        throw new IgnoreMeException();
                    }
                    G newGlobalState = createGlobalState();
                    newGlobalState.setState(stateToRepro);
                    newGlobalState.setRandomly(r);
                    newGlobalState.setDatabaseName(databaseName);
                    newGlobalState.setMainOptions(options);
                    newGlobalState.setDbmsSpecificOptions(command);
                    QueryManager<C> newManager = new QueryManager<>(newGlobalState);
                    newGlobalState.setStateLogger(new StateLogger(databaseName, provider, options));
                    newGlobalState.setManager(newManager);

                    Reducer<G> reducer = new StatementReducer<>(provider);
                    reducer.reduce(state, reproducer, newGlobalState);

                    if (options.reduceAST()) {
                        Reducer<G> astBasedReducer = new ASTBasedReducer<>(provider);
                        astBasedReducer.reduce(state, reproducer, newGlobalState);
                    }

                    try {
                        logger.getReduceFileWriter().close();
                        logger.reduceFileWriter = null;
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }

                    throw new AssertionError("Found a potential bug, please check reducer log for detail.");
                }
            }
        }

        private G getInitializedGlobalState(long seed) {
            G state = createGlobalState();
            stateToRepro = provider.getStateToReproduce(databaseName);
            stateToRepro.seedValue = seed;
            state.setState(stateToRepro);
            logger = new StateLogger(databaseName, provider, options);
            Randomly r = new Randomly(seed);
            state.setRandomly(r);
            state.setDatabaseName(databaseName);
            state.setMainOptions(options);
            state.setDbmsSpecificOptions(command);
            return state;
        }

        public StateLogger getLogger() {
            return logger;
        }

        public StateToReproduce getStateToReproduce() {
            return stateToRepro;
        }
    }

    public static class DBMSExecutorFactory<G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> {

        private final DatabaseProvider<G, O, C> provider;
        private final MainOptions options;
        private final O command;

        public DBMSExecutorFactory(DatabaseProvider<G, O, C> provider, MainOptions options) {
            this.provider = provider;
            this.options = options;
            this.command = createCommand();
        }

        private O createCommand() {
            try {
                return provider.getOptionClass().getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        public O getCommand() {
            return command;
        }

        @SuppressWarnings("unchecked")
        public DBMSExecutor<G, O, C> getDBMSExecutor(String databaseName, Randomly r) {
            try {
                return new DBMSExecutor<G, O, C>(provider.getClass().getDeclaredConstructor().newInstance(), options,
                        command, databaseName, r);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        public DatabaseProvider<G, O, C> getProvider() {
            return provider;
        }

    }

    /*
     * String... args 是 Java 中的可变参数
     * 它允许方法接受多个 String 参数，或者一个 String 数组
     * 在 executeMain 方法中，这些参数代表从命令行传入的参数
     * 例如输入： java -jar sqlancer-*.jar --num-threads 4 sqlite3 --oracle NoREC
     * args = {"--num-threads", "4", "sqlite3", "--oracle", "NoREC"};
     */
    public static int executeMain(String... args) throws AssertionError {
        List<DatabaseProvider<?, ?, ?>> providers = getDBMSProviders();
        Map<String, DBMSExecutorFactory<?, ?, ?>> nameToProvider = new HashMap<>();
        MainOptions options = new MainOptions();
        Builder commandBuilder = JCommander.newBuilder().addObject(options);
        for (DatabaseProvider<?, ?, ?> provider : providers) {
            String name = provider.getDBMSName();
            DBMSExecutorFactory<?, ?, ?> executorFactory = new DBMSExecutorFactory<>(provider, options);
            commandBuilder = commandBuilder.addCommand(name, executorFactory.getCommand());
            nameToProvider.put(name, executorFactory);
        }
        JCommander jc = commandBuilder.programName("SQLancer").build();
        jc.parse(args);

        if (jc.getParsedCommand() == null || options.isHelp()) {
            jc.usage();
            return options.getErrorExitCode();
        }

        Randomly.initialize(options);
        if (options.printProgressInformation()) {
            // true 打印查询进度信息
            startProgressMonitor();// 跟踪程序（设置定时任务，并且打印信息）
            if (options.printProgressSummary()) {
                // java -jar sqlancer-*.jar --num-threads 4 --print-progress-summary true
                // sqlite3 --oracle NoREC
                // 然后CTRL+C 杀掉进程后就可以看到下面的输出：
                /*
                 * 例如：
                 * Overall execution statistics
                 * ============================
                 * 199k queries
                 * 4 databases
                 * 409k successfully-executed statements
                 * 23k unsuccessfuly-executed statements
                 */
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

                    @Override
                    public void run() {
                        System.out.println("Overall execution statistics");
                        System.out.println("============================");
                        System.out.println(formatInteger(nrQueries.get()) + " queries");
                        System.out.println(formatInteger(nrDatabases.get()) + " databases");
                        System.out.println(
                                formatInteger(nrSuccessfulActions.get()) + " successfully-executed statements");
                        System.out.println(
                                formatInteger(nrUnsuccessfulActions.get()) + " unsuccessfuly-executed statements");
                    }

                    private String formatInteger(long intValue) {
                        if (intValue > 1000) {
                            return String.format("%,9dk", intValue / 1000);
                        } else {
                            return String.format("%,10d", intValue);
                        }
                    }
                }));
            }
        }
        // System.out.println("I am Here");
        // Executors 类中的一个静态方法，用于创建一个固定大小的线程池。线程池的大小由参数 nThreads 指定，线程池中的线程数固定，不会动态调整。
        // getNumberConcurrentThreads()： 默认是16，如果 --num-threads 4 这种制定就是4
        ExecutorService execService = Executors.newFixedThreadPool(options.getNumberConcurrentThreads());
        DBMSExecutorFactory<?, ?, ?> executorFactory = nameToProvider.get(jc.getParsedCommand());
        // 请求进行数据库连接测试
        if (options.performConnectionTest()) {
            try {
                executorFactory.getDBMSExecutor(options.getDatabasePrefix() + "connectiontest", new Randomly())
                        .testConnection();
            } catch (Exception e) {
                System.err.println(
                        "SQLancer failed creating a test database, indicating that SQLancer might have failed connecting to the DBMS. In order to change the username, password, host and port, you can use the --username, --password, --host and --port options.\n\n");
                e.printStackTrace();
                return options.getErrorExitCode();
            }
        }
        final AtomicBoolean someOneFails = new AtomicBoolean(false);
        // getTotalNumberTries()：返回一个值totalNumberTries（默认100），该值指定发现多少个错误后停止测试
        for (int i = 0; i < options.getTotalNumberTries(); i++) {
            final String databaseName = options.getDatabasePrefix() + i;// "databasei"
            final long seed;
            if (options.getRandomSeed() == -1) {
                // 如果没有显式指定随机种子（-1？）
                // 通过当前的时间戳（System.currentTimeMillis()）加上循环变量 i 来生成一个种子值
                seed = System.currentTimeMillis() + i;
            } else {
                // 如果指定了 options.getRandomSeed()，则使用指定的种子加上循环的索引 i 来生成种子值
                // 这样可以确保在指定种子的基础上，每次循环使用不同的种子。
                seed = options.getRandomSeed() + i;
            }
            // 通过 ExecutorService 创建并执行一个多线程任务
            execService.execute(new Runnable() {

                @Override
                public void run() {
                    // 设置当前线程的名称为 databaseName，通常用于在日志中区分不同线程的执行
                    Thread.currentThread().setName(databaseName);
                    // 参数databaseName传入的就是"databasei"
                    runThread(databaseName);
                }

                private void runThread(final String databaseName) {
                    Randomly r = new Randomly(seed);
                    try {
                        // 通过getMaxGeneratedDatabases()方法获得maxGeneratedDatabases，默认为-1
                        int maxNrDbs = options.getMaxGeneratedDatabases();
                        // run without a limit if maxNrDbs == -1 如果maxNrDbs是-1，表示没有限制，即会无限制地进行操作while(1)
                        // 如果有限制，则会根据 maxNrDbs 执行相应次数的数据库操作
                        for (int i = 0; i < maxNrDbs || maxNrDbs == -1; i++) {
                            Boolean continueRunning = run(options, execService, executorFactory, r, databaseName);
                            // 如果为false，则说明有错误
                            if (!continueRunning) {
                                someOneFails.set(true);
                                break;
                            }
                        }
                    } finally {
                        threadsShutdown.addAndGet(1);// 线程池中完成的任务数加一
                        if (threadsShutdown.get() == options.getTotalNumberTries()) {
                            execService.shutdown();
                        }
                    }
                }

                private boolean run(MainOptions options, ExecutorService execService,
                        DBMSExecutorFactory<?, ?, ?> executorFactory, Randomly r, final String databaseName) {
                    DBMSExecutor<?, ?, ?> executor = executorFactory.getDBMSExecutor(databaseName, r);
                    try {
                        executor.run();
                        return true;
                    } catch (IgnoreMeException e) {
                        return true;
                    } catch (Throwable reduce) {
                        reduce.printStackTrace();
                        executor.getStateToReproduce().exception = reduce.getMessage();
                        executor.getLogger().logFileWriter = null;
                        executor.getLogger().logException(reduce, executor.getStateToReproduce());
                        return false;
                    } finally {
                        try {
                            if (options.logEachSelect()) {
                                if (executor.getLogger().currentFileWriter != null) {
                                    executor.getLogger().currentFileWriter.close();
                                }
                                executor.getLogger().currentFileWriter = null;
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        try {
            if (options.getTimeoutSeconds() == -1) {
                execService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            } else {
                execService.awaitTermination(options.getTimeoutSeconds(), TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return someOneFails.get() ? options.getErrorExitCode() : 0;
    }

    /**
     * To register a new provider, it is necessary to implement the DatabaseProvider
     * interface and add an additional
     * configuration file, see
     * https://docs.oracle.com/javase/9/docs/api/java/util/ServiceLoader.html.
     * Currently, we use
     * an @AutoService annotation to create the configuration file automatically.
     * This allows SQLancer to pick up
     * providers in other JARs on the classpath.
     *
     * @return The list of service providers on the classpath
     */
    static List<DatabaseProvider<?, ?, ?>> getDBMSProviders() {
        List<DatabaseProvider<?, ?, ?>> providers = new ArrayList<>();
        @SuppressWarnings("rawtypes")
        ServiceLoader<DatabaseProvider> loader = ServiceLoader.load(DatabaseProvider.class);
        for (DatabaseProvider<?, ?, ?> provider : loader) {
            providers.add(provider);
        }
        checkForIssue799(providers);
        return providers;
    }

    // see https://github.com/sqlancer/sqlancer/issues/799
    private static void checkForIssue799(List<DatabaseProvider<?, ?, ?>> providers) {
        if (providers.isEmpty()) {
            System.err.println(
                    "No DBMS implementations (i.e., instantiations of the DatabaseProvider class) were found. You likely ran into an issue described in https://github.com/sqlancer/sqlancer/issues/799. As a workaround, I now statically load all supported providers as of June 7, 2023.");
            providers.add(new CitusProvider());
            providers.add(new ClickHouseProvider());
            providers.add(new CnosDBProvider());
            providers.add(new CockroachDBProvider());
            providers.add(new DatabendProvider());
            providers.add(new DorisProvider());
            providers.add(new DuckDBProvider());
            providers.add(new H2Provider());
            providers.add(new HSQLDBProvider());
            providers.add(new MariaDBProvider());
            providers.add(new MaterializeProvider());
            providers.add(new MySQLProvider());
            providers.add(new OceanBaseProvider());
            providers.add(new PrestoProvider());
            providers.add(new PostgresProvider());
            providers.add(new QuestDBProvider());
            providers.add(new SQLite3Provider());
            providers.add(new TiDBProvider());
            providers.add(new YCQLProvider());
            providers.add(new YSQLProvider());
        }
    }

    private static synchronized void startProgressMonitor() {
        /*
         * 打印信息如下：
         * [2024/11/26 16:10:11] Executed 65308 queries (13051 queries/s; 0.80/s dbs,
         * successful statements: 94%). Threads shut down: 0.
         * [2024/11/26 16:10:16] Executed 191730 queries (25309 queries/s; 0.20/s dbs,
         * successful statements: 94%). Threads shut down: 0.
         * [2024/11/26 16:10:21] Executed 288294 queries (19312 queries/s; 0.00/s dbs,
         * successful statements: 95%). Threads shut down: 0.
         */
        // 启动一个定时任务，用于定期输出程序的执行进度和性能数据
        if (progressMonitorStarted) { // 如果进度监控已经启动，则直接返回
            /*
             * it might be already started if, for example, the main method is called
             * multiple times in a test (see
             * https://github.com/sqlancer/sqlancer/issues/90).
             */
            return;
        } else {
            progressMonitorStarted = true; //
        }
        // 创建一个定时任务调度器，使用单线程池，保证定时任务按照顺序执行。
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        // 定时调度任务，每隔 5 秒钟执行一次。定时任务是一个匿名 Runnable 实现
        scheduler.scheduleAtFixedRate(new Runnable() {

            private long timeMillis = System.currentTimeMillis();
            private long lastNrQueries;
            private long lastNrDbs;

            {
                timeMillis = System.currentTimeMillis();
            }

            @Override
            public void run() {
                // 计算自上次任务执行以来的时间 (elapsedTimeMillis)
                long elapsedTimeMillis = System.currentTimeMillis() - timeMillis;
                long currentNrQueries = nrQueries.get();// 获取当前查询数
                // 计算这次周期内执行的查询数
                long nrCurrentQueries = currentNrQueries - lastNrQueries;
                // 计算查询吞吐量
                double throughput = nrCurrentQueries / (elapsedTimeMillis / 1000d);
                // 获取当前数据库数
                long currentNrDbs = nrDatabases.get();
                long nrCurrentDbs = currentNrDbs - lastNrDbs;
                // 计算数据库吞吐量
                double throughputDbs = nrCurrentDbs / (elapsedTimeMillis / 1000d);
                // 计算成功的语句比例
                long successfulStatementsRatio = (long) (100.0 * nrSuccessfulActions.get()
                        / (nrSuccessfulActions.get() + nrUnsuccessfulActions.get()));
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = new Date();
                System.out.println(String.format(
                        "[%s] Executed %d queries (%d queries/s; %.2f/s dbs, successful statements: %2d%%). Threads shut down: %d.",
                        dateFormat.format(date), currentNrQueries, (int) throughput, throughputDbs,
                        successfulStatementsRatio, threadsShutdown.get()));
                timeMillis = System.currentTimeMillis();
                lastNrQueries = currentNrQueries;
                lastNrDbs = currentNrDbs;
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

}
