package com.dataplatform.quality.util;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClickHouse 性能测试
 * 验证查询响应时间 < 1秒 的要求
 */
public class ClickHousePerformanceTest {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHousePerformanceTest.class);
    
    // ClickHouse 连接配置
    // 禁用 LZ4 压缩以避免协议兼容性问题
    private static final String JDBC_URL = "jdbc:clickhouse://192.168.128.141:8123/data_quality?compress=0";
    private static final String USERNAME = "default";
    private static final String PASSWORD = "";
    
    /**
     * 测试：基础查询性能
     */
    @Test
    public void testBasicQueryPerformance() {
        System.out.println("\n========== ClickHouse 基础查询性能测试 ==========\n");
        
        ClickHousePerformanceTester tester = new ClickHousePerformanceTester(JDBC_URL, USERNAME, PASSWORD);
        
        // 测试 1: 简单的聚合查询
        String query1 = "SELECT " +
            "rule_name, " +
            "COUNT(*) as count, " +
            "AVG(pass_rate) as avg_pass_rate, " +
            "MIN(pass_rate) as min_pass_rate, " +
            "MAX(pass_rate) as max_pass_rate " +
            "FROM data_quality_metrics " +
            "GROUP BY rule_name";
        
        LOG.info("【测试 1】简单聚合查询");
        ClickHousePerformanceTester.QueryPerformanceResult result1 = tester.testQueryPerformance(query1, 10);
        result1.printReport("简单聚合查询");
        
        // 测试 2: 时间范围查询
        String query2 = "SELECT " +
            "toDate(check_time) as date, " +
            "rule_name, " +
            "SUM(total_count) as total, " +
            "SUM(valid_count) as valid, " +
            "SUM(invalid_count) as invalid " +
            "FROM data_quality_metrics " +
            "WHERE check_time >= now() - INTERVAL 1 DAY " +
            "GROUP BY date, rule_name " +
            "ORDER BY date DESC";
        
        LOG.info("\n【测试 2】时间范围查询");
        ClickHousePerformanceTester.QueryPerformanceResult result2 = tester.testQueryPerformance(query2, 10);
        result2.printReport("时间范围查询");
        
        // 测试 3: 复杂的多维聚合
        String query3 = "SELECT " +
            "toDate(check_time) as date, " +
            "rule_name, " +
            "COUNT(*) as count, " +
            "AVG(pass_rate) as avg_pass_rate, " +
            "MIN(pass_rate) as min_pass_rate, " +
            "MAX(pass_rate) as max_pass_rate " +
            "FROM data_quality_metrics " +
            "WHERE check_time >= now() - INTERVAL 7 DAY " +
            "GROUP BY date, rule_name " +
            "ORDER BY date DESC, rule_name";
        
        LOG.info("\n【测试 3】复杂多维聚合");
        ClickHousePerformanceTester.QueryPerformanceResult result3 = tester.testQueryPerformance(query3, 10);
        result3.printReport("复杂多维聚合");
        
        // 总结
        LOG.info("\n========== 性能测试总结 ==========");
        int passCount = 0;
        if (result1.avgTime < 1000) passCount++;
        if (result2.avgTime < 1000) passCount++;
        if (result3.avgTime < 1000) passCount++;
        
        LOG.info("满足 < 1秒 要求的查询: {}/3", passCount);
        if (passCount == 3) {
            LOG.info("✅ 所有查询都满足性能要求");
        } else {
            LOG.warn("⚠️ 部分查询需要优化");
        }
    }
    
    /**
     * 测试：物化视图性能对比
     */
    @Test
    public void testMaterializedViewPerformance() {
        System.out.println("\n========== ClickHouse 物化视图性能对比 ==========\n");
        
        ClickHousePerformanceTester tester = new ClickHousePerformanceTester(JDBC_URL, USERNAME, PASSWORD);
        
        // 普通表查询（需要实时聚合）
        String normalTableQuery = "SELECT " +
            "toDate(check_time) as date, " +
            "rule_name, " +
            "SUM(total_count) as total, " +
            "SUM(valid_count) as valid, " +
            "AVG(pass_rate) as avg_pass_rate " +
            "FROM data_quality_metrics " +
            "WHERE check_time >= now() - INTERVAL 7 DAY " +
            "GROUP BY date, rule_name";
        
        // 物化视图查询（预聚合）
        // 注意：需要先在 ClickHouse 中创建物化视图
        String materializedViewQuery = "SELECT " +
            "date, " +
            "rule_name, " +
            "total, " +
            "valid, " +
            "avg_pass_rate " +
            "FROM data_quality_metrics_daily_mv " +
            "WHERE date >= today() - INTERVAL 7 DAY";
        
        try {
            LOG.info("对比普通表和物化视图的性能...");
            ClickHousePerformanceTester.ComparisonResult result = 
                tester.comparePerformance(normalTableQuery, materializedViewQuery, 10);
            result.printReport();
        } catch (Exception e) {
            LOG.warn("物化视图测试失败（可能物化视图还未创建）: {}", e.getMessage());
            LOG.info("请先在 ClickHouse 中创建物化视图：");
            LOG.info("CREATE MATERIALIZED VIEW data_quality_metrics_daily_mv AS " +
                "SELECT " +
                "toDate(check_time) as date, " +
                "rule_name, " +
                "SUM(total_count) as total, " +
                "SUM(valid_count) as valid, " +
                "AVG(pass_rate) as avg_pass_rate " +
                "FROM data_quality_metrics " +
                "GROUP BY date, rule_name");
        }
    }
    
    /**
     * 测试：大数据量查询性能
     */
    @Test
    public void testLargeDatasetPerformance() {
        System.out.println("\n========== 大数据量查询性能测试 ==========\n");
        
        ClickHousePerformanceTester tester = new ClickHousePerformanceTester(JDBC_URL, USERNAME, PASSWORD);
        
        // 查询最近 30 天的数据
        String largeDatasetQuery = "SELECT " +
            "toDate(check_time) as date, " +
            "rule_name, " +
            "COUNT(*) as count, " +
            "SUM(total_count) as total, " +
            "SUM(valid_count) as valid, " +
            "SUM(invalid_count) as invalid, " +
            "AVG(pass_rate) as avg_pass_rate, " +
            "MIN(pass_rate) as min_pass_rate, " +
            "MAX(pass_rate) as max_pass_rate " +
            "FROM data_quality_metrics " +
            "WHERE check_time >= now() - INTERVAL 30 DAY " +
            "GROUP BY date, rule_name " +
            "ORDER BY date DESC, rule_name";
        
        LOG.info("【测试】30 天数据聚合查询");
        ClickHousePerformanceTester.QueryPerformanceResult result = 
            tester.testQueryPerformance(largeDatasetQuery, 5);
        result.printReport("30 天数据聚合");
    }
}
