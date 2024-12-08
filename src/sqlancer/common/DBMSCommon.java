package sqlancer.common;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DBMSCommon {
    // SQLANCER_INDEX_PATTERN 是一个静态的 Pattern 对象，定义了一个正则表达式，用于匹配特定的字符串模式。
    private static final Pattern SQLANCER_INDEX_PATTERN = Pattern.compile("^i\\d+");

    private DBMSCommon() {
    }

    // 根据传入的 int nr 创建表名，格式为 t{nr}，例如 t1、t2
    public static String createTableName(int nr) {
        return String.format("t%d", nr);
    }

    // 根据传入的 int nr 创建类名，格式为c{nr}，例如c1、c2
    public static String createColumnName(int nr) {
        return String.format("c%d", nr);
    }

    // 根据传入的 int nr 创建索引名，格式为i{nr}，例如i1、i2
    public static String createIndexName(int nr) {
        return String.format("i%d", nr);
    }

    // 检查给定的索引名是否符合预定义的格式（以 "i" 开头，后接数字）
    // SQLANCER_INDEX_PATTERN 是一个 Pattern 对象，它存储了一个正则表达式。
    public static boolean matchesIndexName(String indexName) {
        Matcher matcher = SQLANCER_INDEX_PATTERN.matcher(indexName);
        return matcher.matches();
    }

    // 在一个 double 类型的数组中找到最大值的索引
    // 对于输入数组 {1.5, 2.3, 0.8}，返回索引 1
    public static int getMaxIndexInDoubleArray(double... doubleArray) {
        int maxIndex = 0;
        double maxValue = 0.0;
        for (int j = 0; j < doubleArray.length; j++) {
            double curReward = doubleArray[j];
            if (curReward > maxValue) {
                maxIndex = j;
                maxValue = curReward;
            }
        }
        return maxIndex;
    }

    // 判断两个查询计划序列是否相似，使用编辑距离算法，若编辑距离小于等于 1，则认为序列相似
    public static boolean areQueryPlanSequencesSimilar(List<String> list1, List<String> list2) {
        return editDistance(list1, list2) <= 1;
    }

    // 计算两个字符串列表之间的编辑距离，即通过插入、删除或替换操作将一个列表转换成另一个列表所需的最小操作数
    public static int editDistance(List<String> list1, List<String> list2) {
        int[][] dp = new int[list1.size() + 1][list2.size() + 1];
        for (int i = 0; i <= list1.size(); i++) {
            for (int j = 0; j <= list2.size(); j++) {
                if (i == 0) {
                    dp[i][j] = j;
                } else if (j == 0) {
                    dp[i][j] = i;
                } else {
                    dp[i][j] = Math.min(dp[i - 1][j - 1] + costOfSubstitution(list1.get(i - 1), list2.get(j - 1)),
                            Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1));
                }
            }
        }
        return dp[list1.size()][list2.size()];
    }

    // 计算两个字符串的替换成本。如果两个字符串相同，则成本为 0；否则，成本为 1
    private static int costOfSubstitution(String string, String string2) {
        return string.equals(string2) ? 0 : 1;
    }

}
