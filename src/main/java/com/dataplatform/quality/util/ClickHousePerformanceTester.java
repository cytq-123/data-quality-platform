package com.dataplatform.quality.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ClickHouse 性能测试工具
 * 用于测试查询响应时间和物化视图性能
 */
public class ClickHousePerformanceTester {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHousePerformanceTester.class);
    
    private String jdbcUrl;
    private String username;
    private String password;
    
    public ClickHousePerformanceTester(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }
    
    /**
     * 测试查询响应时间
     */
    public QueryPerformanceResult testQueryPerformance(String query, int iterations) {
        QueryPerformanceResult result = new QueryPerformanceResult();
        List<Long> responseTimes = new ArrayList<>();
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            for (int i = 0; i < iterations; i++) {
                long startTime = System.nanoTime();
                
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    // 消费结果集
                    while (rs.next()) {
                        // 读取数据
                    }
                }
                
                long endTime = System.nanoTime();
                long responseTimeMs = (endTime - startTime) / 1_000_000;
                responseTimes.add(responseTimeMs);
            }
            
            // 计算统计信息
            result.iterations = iterations;
            result.responseTimes = responseTimes;
            result.minTime = responseTimes.stream().mapToLong(Long::longValue).min().orElse(0);
            result.maxTime = responseTimes.stream().mapToLong(Long::longValue).max().orElse(0);
            result.avgTime = responseTimes.stream().mapToLong(Long::longValue).average().orElse(0);
            result.p95Time = calculatePercentile(responseTimes, 0.95);
            result.p99Time = calculatePercentile(responseTimes, 0.99);
            
        } catch (SQLException e) {
            LOG.error("Error testing query performance", e);
            result.error = e.getMessage();
        }
        
        return result;
    }
    
    /**
     * 对比物化视图和普通表的性能
     */
    public ComparisonResult comparePerformance(String normalTableQuery, String materializedViewQuery, int iterations) {
        LOG.info("测试普通表查询...");
        QueryPerformanceResult normalResult = testQueryPerformance(normalTableQuery, iterations);
        
        LOG.info("测试物化视图查询...");
        QueryPerformanceResult mvResult = testQueryPerformance(materializedViewQuery, iterations);
        
        ComparisonResult result = new ComparisonResult();
        result.normalTableResult = normalResult;
        result.materializedViewResult = mvResult;
        result.speedup = normalResult.avgTime / mvResult.avgTime;
        result.improvement = (normalResult.avgTime - mvResult.avgTime) / normalResult.avgTime * 100;
        
        return result;
    }
    
    /**
     * 计算百分位数
     */
    private long calculatePercentile(List<Long> values, double percentile) {
        if (values.isEmpty()) return 0;
        
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Long::compareTo);
        
        int index = (int) (sorted.size() * percentile);
        return sorted.get(Math.min(index, sorted.size() - 1));
    }
    
    /**
     * 查询性能结果
     */
    public static class QueryPerformanceResult {
        public int iterations;
        public List<Long> responseTimes;
        public long minTime;      // 最小响应时间 (ms)
        public long maxTime;      // 最大响应时间 (ms)
        public double avgTime;    // 平均响应时间 (ms)
        public long p95Time;      // 95% 百分位响应时间 (ms)
        public long p99Time;      // 99% 百分位响应时间 (ms)
        public String error;
        
        public void printReport(String queryName) {
            if (error != null) {
                LOG.error("查询 {} 执行失败: {}", queryName, error);
                return;
            }
            
            LOG.info("========== {} 性能报告 ==========", queryName);
            LOG.info("执行次数: {}", iterations);
            LOG.info("最小响应时间: {} ms", minTime);
            LOG.info("最大响应时间: {} ms", maxTime);
            LOG.info("平均响应时间: {}", String.format("%.2f ms", avgTime));
            LOG.info("P95 响应时间: {} ms", p95Time);
            LOG.info("P99 响应时间: {} ms", p99Time);
            LOG.info("=====================================");
            
            // 检查是否满足 < 1秒 的要求
            if (avgTime < 1000) {
                LOG.info("✅ 满足 < 1秒 的性能要求");
            } else {
                LOG.warn("❌ 不满足 < 1秒 的性能要求");
            }
        }
    }
    
    /**
     * 对比结果
     */
    public static class ComparisonResult {
        public QueryPerformanceResult normalTableResult;
        public QueryPerformanceResult materializedViewResult;
        public double speedup;      // 加速倍数
        public double improvement;  // 性能改进百分比
        
        public void printReport() {
            LOG.info("========== 物化视图性能对比 ==========");
            LOG.info("普通表平均响应时间: {}", String.format("%.2f ms", normalTableResult.avgTime));
            LOG.info("物化视图平均响应时间: {}", String.format("%.2f ms", materializedViewResult.avgTime));
            LOG.info("加速倍数: {}", String.format("%.2f x", speedup));
            LOG.info("性能改进: {}", String.format("%.2f%%", improvement));
            LOG.info("=====================================");
            
            if (materializedViewResult.avgTime < 1000) {
                LOG.info("✅ 物化视图查询满足 < 1秒 的性能要求");
            } else {
                LOG.warn("❌ 物化视图查询不满足 < 1秒 的性能要求");
            }
        }
    }
}
