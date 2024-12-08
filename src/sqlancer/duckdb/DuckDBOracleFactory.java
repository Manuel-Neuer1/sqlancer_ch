package sqlancer.duckdb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;
import sqlancer.duckdb.test.DuckDBQueryPartitioningAggregateTester;
import sqlancer.duckdb.test.DuckDBQueryPartitioningDistinctTester;
import sqlancer.duckdb.test.DuckDBQueryPartitioningGroupByTester;
import sqlancer.duckdb.test.DuckDBQueryPartitioningHavingTester;

public enum DuckDBOracleFactory implements OracleFactory<DuckDBProvider.DuckDBGlobalState> {
    /*
     * 每个枚举常量都代表了一个特定类型的 Oracle：
     * NOREC：没有记录（NoRECOracle）的 Oracle。
     * HAVING：针对 HAVING 子句的测试（DuckDBQueryPartitioningHavingTester）。
     * WHERE：针对 WHERE 子句的测试（TLPWhereOracle）。
     * GROUP_BY：针对 GROUP BY 子句的测试（DuckDBQueryPartitioningGroupByTester）。
     * AGGREGATE：针对聚合函数的测试（DuckDBQueryPartitioningAggregateTester）。
     * DISTINCT：针对 DISTINCT 关键字的测试（DuckDBQueryPartitioningDistinctTester）。
     * QUERY_PARTITIONING：一个综合的 Oracle，结合了多个子 Oracle。
     */
    NOREC {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws SQLException {
            // DuckDBExpressionGenerator：用于生成 DuckDB 表达式，这些表达式用于 SQL 查询中的条件和表达式
            DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState);
            // ExpectedErrors：用来指定在执行测试时期望出现的错误。这里设置了可能会出现的表达式错误和正则表达式匹配的错误类型。
            ExpectedErrors errors = ExpectedErrors.newErrors().with(DuckDBErrors.getExpressionErrors())
                    .withRegex(DuckDBErrors.getExpressionErrorsRegex())
                    .with("canceling statement due to statement timeout").build();
            return new NoRECOracle<>(globalState, gen, errors);
        }

    },
    HAVING {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws SQLException {
            return new DuckDBQueryPartitioningHavingTester(globalState);
        }
    },
    WHERE {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws SQLException {
            DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(DuckDBErrors.getExpressionErrors())
                    .with(DuckDBErrors.getGroupByErrors()).withRegex(DuckDBErrors.getExpressionErrorsRegex()).build();

            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    GROUP_BY {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws SQLException {
            return new DuckDBQueryPartitioningGroupByTester(globalState);
        }
    },
    AGGREGATE {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws SQLException {
            return new DuckDBQueryPartitioningAggregateTester(globalState);
        }

    },
    DISTINCT {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws SQLException {
            return new DuckDBQueryPartitioningDistinctTester(globalState);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<DuckDBProvider.DuckDBGlobalState> create(DuckDBProvider.DuckDBGlobalState globalState)
                throws Exception {
            List<TestOracle<DuckDBProvider.DuckDBGlobalState>> oracles = new ArrayList<>();
            oracles.add(WHERE.create(globalState));
            oracles.add(HAVING.create(globalState));
            oracles.add(AGGREGATE.create(globalState));
            oracles.add(DISTINCT.create(globalState));
            oracles.add(GROUP_BY.create(globalState));
            return new CompositeTestOracle<DuckDBProvider.DuckDBGlobalState>(oracles, globalState);
        }
    };

}
