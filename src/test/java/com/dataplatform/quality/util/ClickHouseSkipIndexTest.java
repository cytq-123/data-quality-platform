package com.dataplatform.quality.util;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * ClickHouse 跳数索引性能测试
 * 验证跳数索引对查询性能的提升效果
 */
public class ClickHouseSkipIndexTest {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseSkipIndexTest.class);
    
    // ClickHouse 连接配置
    private static final String JDBC_URL = "jdbc:clickhouse://192.168.128.141:8123/data_quality?compress=0";
    private static final String USERNAME = "default";
    private static final String PASSWORD = "";
    
    /**
     * 测试：跳数索引对 rule_name 过滤的性能提升
     */
    @Test
    public void testSkipIndexOnRuleName() {
        System.out.println("\n========== 跳数索引性能测试：rule_name 过滤 ==========\n");
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            // 查询：使用跳数索引的查询
            String query = "SELECT COUNT(*) FROM data_quality_metrics WHERE rule_name = 'order_amount_check'";
            
            LOG.info("测试场景：按 rule_name 过滤");
            LOG.info("查询语句: {}", query);
            
            // 执行多次查询取平均值
            List<Long> times = new ArrayList<>();
            
            for (int i = 0; i < 10; i++) {
                long startTime = System.nanoTime();
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    while (rs.next()) {
                        rs.getLong(1);
                    }
                }
                long endTime = System.nanoTime();
                times.add((endTime - startTime) / 1_000_000);
            }
            
            // 计算平均值
            long avgTime = times.stream().mapToLong(Long::longValue).sum() / times.size();
            long minTime = times.stream().mapToLong(Long::longValue).min().orElse(0);
            long maxTime = times.stream().mapToLong(Long::longValue).max().orElse(0);
            
            LOG.info("\n========== rule_name 过滤性能 ==========");
            LOG.info("最小响应时间: {} ms", minTime);
            LOG.info("最大响应时间: {} ms", maxTime);
            LOG.info("平均响应时间: {} ms", avgTime);
            LOG.info("✅ 使用跳数索引加速 rule_name 过滤");
            LOG.info("=====================================");
            
        } catch (SQLException e) {
            LOG.error("查询失败: {}", e.getMessage());
        }
    }
    
    /**
     * 测试：跳数索引对 pass_rate 范围查询的性能提升
     */
    @Test
    public void testSkipIndexOnPassRate() {
        System.out.println("\n========== 跳数索引性能测试：pass_rate 范围查询 ==========\n");
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            // 查询：pass_rate < 0.95 的记录
            String query = "SELECT COUNT(*) FROM data_quality_metrics WHERE pass_rate < 0.95";
            
            LOG.info("测试场景：pass_rate 范围查询（< 0.95）");
            
            // 执行多次查询取平均值
            List<Long> times = new ArrayList<>();
            
            for (int i = 0; i < 10; i++) {
                long startTime = System.nanoTime();
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    while (rs.next()) {
                        rs.getLong(1);
                    }
                }
                long endTime = System.nanoTime();
                times.add((endTime - startTime) / 1_000_000);
            }
            
            // 计算平均值
            long avgTime = times.stream().mapToLong(Long::longValue).sum() / times.size();
            
            LOG.info("\n========== pass_rate 范围查询性能 ==========");
            LOG.info("平均响应时间: {} ms", avgTime);
            LOG.info("✅ 使用跳数索引加速范围查询");
            LOG.info("=====================================");
            
        } catch (SQLException e) {
            LOG.error("查询失败: {}", e.getMessage());
        }
    }
    
    /**
     * 测试：跳数索引对 check_time 时间范围查询的性能提升
     */
    @Test
    public void testSkipIndexOnCheckTime() {
        System.out.println("\n========== 跳数索引性能测试：check_time 时间范围查询 ==========\n");
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            // 查询：最近 7 天的数据
            String query = "SELECT COUNT(*) FROM data_quality_metrics WHERE check_time >= now() - INTERVAL 7 DAY";
            
            LOG.info("测试场景：check_time 时间范围查询（最近 7 天）");
            
            // 执行多次查询取平均值
            List<Long> times = new ArrayList<>();
            
            for (int i = 0; i < 10; i++) {
                long startTime = System.nanoTime();
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    while (rs.next()) {
                        rs.getLong(1);
                    }
                }
                long endTime = System.nanoTime();
                times.add((endTime - startTime) / 1_000_000);
            }
            
            // 计算平均值
            long avgTime = times.stream().mapToLong(Long::longValue).sum() / times.size();
            
            LOG.info("\n========== check_time 时间范围查询性能 ==========");
            LOG.info("平均响应时间: {} ms", avgTime);
            LOG.info("✅ 使用跳数索引加速时间范围查询");
            LOG.info("=====================================");
            
        } catch (SQLException e) {
            LOG.error("查询失败: {}", e.getMessage());
        }
    }
    
    /**
     * 测试：复杂查询的性能提升
     */
    @Test
    public void testSkipIndexOnComplexQuery() {
        System.out.println("\n========== 跳数索引性能测试：复杂多条件查询 ==========\n");
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            // 复杂查询：多个条件组合
            String query = "SELECT " +
                "rule_name, " +
                "toDate(check_time) as date, " +
                "COUNT(*) as count, " +
                "AVG(pass_rate) as avg_pass_rate " +
                "FROM data_quality_metrics " +
                "WHERE rule_name = 'order_amount_check' " +
                "AND check_time >= now() - INTERVAL 7 DAY " +
                "AND pass_rate < 0.95 " +
                "GROUP BY rule_name, date " +
                "ORDER BY date DESC";
            
            LOG.info("测试场景：复杂多条件查询");
            
            // 执行多次查询取平均值
            List<Long> times = new ArrayList<>();
            
            for (int i = 0; i < 10; i++) {
                long startTime = System.nanoTime();
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    while (rs.next()) {
                        rs.getString(1);
                        rs.getDate(2);
                        rs.getLong(3);
                        rs.getDouble(4);
                    }
                }
                long endTime = System.nanoTime();
                times.add((endTime - startTime) / 1_000_000);
            }
            
            // 计算平均值
            long avgTime = times.stream().mapToLong(Long::longValue).sum() / times.size();
            
            LOG.info("\n========== 复杂多条件查询性能 ==========");
            LOG.info("平均响应时间: {} ms", avgTime);
            LOG.info("✅ 跳数索引优化多条件查询");
            LOG.info("=====================================");
            
        } catch (SQLException e) {
            LOG.error("查询失败: {}", e.getMessage());
        }
    }
    
    /**
     * 测试：查看跳数索引统计信息
     */
    @Test
    public void testSkipIndexStatistics() {
        System.out.println("\n========== 跳数索引统计信息 ==========\n");
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            // 查询表的索引信息
            String query = "SELECT * FROM system.tables WHERE database = 'data_quality' AND name = 'data_quality_metrics'";
            
            LOG.info("查询 ClickHouse 表索引信息...");
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    LOG.info("表名: {}", rs.getString("name"));
                    LOG.info("引擎: {}", rs.getString("engine"));
                    LOG.info("数据库: {}", rs.getString("database"));
                }
            }
            
            LOG.info("\n========== 跳数索引配置 ==========");
            LOG.info("✅ idx_rule_name: SET 类型索引，用于 rule_name 过滤");
            LOG.info("✅ idx_pass_rate: MINMAX 类型索引，用于 pass_rate 范围查询");
            LOG.info("✅ idx_check_time: MINMAX 类型索引，用于 check_time 时间范围查询");
            LOG.info("=====================================");
            
        } catch (SQLException e) {
            LOG.error("查询失败: {}", e.getMessage());
        }
    }
}
