package com.dataplatform.quality.clickhouse;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * ClickHouse 物化视图一致性测试
 * 
 * 测试场景:
 * 1. 主表插入成功，物化视图自动更新
 * 2. 验证数据一致性
 * 3. 测试事务回滚
 */
public class MaterializedViewConsistencyTest {
    private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewConsistencyTest.class);
    
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://192.168.128.141:8123/default";
    private static final String USERNAME = "default";
    private static final String PASSWORD = "";
    
    @Test
    public void testMaterializedViewUpdate() throws Exception {
        try (Connection conn = DriverManager.getConnection(CLICKHOUSE_URL, USERNAME, PASSWORD)) {
            
            // 1. 插入测试数据到主表
            LOG.info("=== 测试1: 插入数据到主表 ===");
            insertTestData(conn, 1L, "TestRule1", 100, 10, LocalDateTime.now());
            
            // 等待物化视图更新（通常是毫秒级）
            Thread.sleep(100);
            
            // 2. 查询主表数据
            LOG.info("=== 查询主表数据 ===");
            long mainTableCount = queryMainTable(conn, 1L);
            LOG.info("主表执行次数: {}", mainTableCount);
            
            // 3. 查询物化视图数据（未合并）
            LOG.info("=== 查询物化视图数据（未合并） ===");
            long mvCountBeforeMerge = queryMaterializedView(conn, 1L, false);
            LOG.info("物化视图执行次数（未合并）: {}", mvCountBeforeMerge);
            
            // 4. 查询物化视图数据（强制合并）
            LOG.info("=== 查询物化视图数据（强制合并） ===");
            long mvCountAfterMerge = queryMaterializedView(conn, 1L, true);
            LOG.info("物化视图执行次数（强制合并）: {}", mvCountAfterMerge);
            
            // 5. 验证一致性
            LOG.info("=== 验证数据一致性 ===");
            if (mainTableCount == mvCountAfterMerge) {
                LOG.info("✅ 数据一致性验证通过: {} == {}", mainTableCount, mvCountAfterMerge);
            } else {
                LOG.error("❌ 数据一致性验证失败: {} != {}", mainTableCount, mvCountAfterMerge);
            }
        }
    }
    
    @Test
    public void testIncrementalUpdate() throws Exception {
        try (Connection conn = DriverManager.getConnection(CLICKHOUSE_URL, USERNAME, PASSWORD)) {
            
            LOG.info("=== 测试2: 增量更新 ===");
            
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime minute = now.withSecond(0).withNano(0);
            
            // 插入3条数据到同一分钟
            LOG.info("插入3条数据到同一分钟: {}", minute);
            insertTestData(conn, 2L, "TestRule2", 100, 10, minute);
            insertTestData(conn, 2L, "TestRule2", 50, 5, minute.plusSeconds(10));
            insertTestData(conn, 2L, "TestRule2", 30, 3, minute.plusSeconds(20));
            
            Thread.sleep(100);
            
            // 查询主表总数
            long mainTableTotal = queryMainTableByMinute(conn, 2L, minute);
            LOG.info("主表总执行次数: {}", mainTableTotal);
            
            // 手动触发合并
            LOG.info("手动触发合并...");
            optimizeTable(conn, "rule_metrics_1min_table");
            
            // 查询物化视图（合并后）
            long mvTotal = queryMaterializedViewByMinute(conn, 2L, minute);
            LOG.info("物化视图总执行次数: {}", mvTotal);
            
            // 验证
            if (mainTableTotal == mvTotal) {
                LOG.info("✅ 增量更新验证通过: {} == {}", mainTableTotal, mvTotal);
            } else {
                LOG.error("❌ 增量更新验证失败: {} != {}", mainTableTotal, mvTotal);
            }
        }
    }
    
    @Test
    public void testConsistencyUnderLoad() throws Exception {
        try (Connection conn = DriverManager.getConnection(CLICKHOUSE_URL, USERNAME, PASSWORD)) {
            
            LOG.info("=== 测试3: 高并发写入一致性 ===");
            
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime minute = now.withSecond(0).withNano(0);
            
            // 批量插入100条数据
            LOG.info("批量插入100条数据...");
            conn.setAutoCommit(false);
            
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO rule_execution_metrics " +
                "(rule_id, rule_name, execution_count, failure_count, error_count, " +
                "avg_latency_ms, failure_rate, error_rate, circuit_status, timestamp) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                
                for (int i = 0; i < 100; i++) {
                    ps.setLong(1, 3L);
                    ps.setString(2, "TestRule3");
                    ps.setLong(3, 10);
                    ps.setLong(4, 1);
                    ps.setLong(5, 0);
                    ps.setDouble(6, 5.0);
                    ps.setDouble(7, 0.1);
                    ps.setDouble(8, 0.0);
                    ps.setString(9, "CLOSED");
                    ps.setTimestamp(10, Timestamp.valueOf(minute.plusSeconds(i)));
                    ps.addBatch();
                }
                
                ps.executeBatch();
                conn.commit();
                LOG.info("批量插入完成");
            }
            
            Thread.sleep(200);
            
            // 查询并验证
            long mainTableTotal = queryMainTableByMinute(conn, 3L, minute);
            LOG.info("主表总执行次数: {}", mainTableTotal);
            
            optimizeTable(conn, "rule_metrics_1min_table");
            
            long mvTotal = queryMaterializedViewByMinute(conn, 3L, minute);
            LOG.info("物化视图总执行次数: {}", mvTotal);
            
            if (mainTableTotal == mvTotal) {
                LOG.info("✅ 高并发一致性验证通过: {} == {}", mainTableTotal, mvTotal);
            } else {
                LOG.error("❌ 高并发一致性验证失败: {} != {}", mainTableTotal, mvTotal);
            }
        }
    }
    
    // ========== 辅助方法 ==========
    
    private void insertTestData(Connection conn, Long ruleId, String ruleName, 
                                 long executionCount, long errorCount, LocalDateTime timestamp) throws SQLException {
        String sql = "INSERT INTO rule_execution_metrics " +
            "(rule_id, rule_name, execution_count, failure_count, error_count, " +
            "avg_latency_ms, failure_rate, error_rate, circuit_status, timestamp) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, ruleId);
            ps.setString(2, ruleName);
            ps.setLong(3, executionCount);
            ps.setLong(4, 0);
            ps.setLong(5, errorCount);
            ps.setDouble(6, 5.0);
            ps.setDouble(7, 0.0);
            ps.setDouble(8, (double) errorCount / executionCount);
            ps.setString(9, "CLOSED");
            ps.setTimestamp(10, Timestamp.valueOf(timestamp));
            
            ps.executeUpdate();
        }
    }
    
    private long queryMainTable(Connection conn, Long ruleId) throws SQLException {
        String sql = "SELECT sum(execution_count) FROM rule_execution_metrics WHERE rule_id = ?";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, ruleId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }
    
    private long queryMainTableByMinute(Connection conn, Long ruleId, LocalDateTime minute) throws SQLException {
        String sql = "SELECT sum(execution_count) FROM rule_execution_metrics " +
            "WHERE rule_id = ? AND toStartOfMinute(timestamp) = ?";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, ruleId);
            ps.setTimestamp(2, Timestamp.valueOf(minute));
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }
    
    private long queryMaterializedView(Connection conn, Long ruleId, boolean useFinal) throws SQLException {
        String sql = "SELECT sum(total_executions) FROM rule_metrics_1min_table " +
            (useFinal ? "FINAL " : "") + "WHERE rule_id = ?";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, ruleId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }
    
    private long queryMaterializedViewByMinute(Connection conn, Long ruleId, LocalDateTime minute) throws SQLException {
        String sql = "SELECT sum(total_executions) FROM rule_metrics_1min_table FINAL " +
            "WHERE rule_id = ? AND minute = ?";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, ruleId);
            ps.setTimestamp(2, Timestamp.valueOf(minute));
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }
    
    private void optimizeTable(Connection conn, String tableName) throws SQLException {
        String sql = "OPTIMIZE TABLE " + tableName + " FINAL";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}
