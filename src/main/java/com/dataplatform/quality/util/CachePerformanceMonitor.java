package com.dataplatform.quality.util;

import com.dataplatform.quality.cache.RuleCacheManager;
import com.dataplatform.quality.model.QualityRule;
import com.dataplatform.quality.rule.RuleLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 缓存性能监控工具
 * 用于测试三层缓存在 Flink 作业中的实际性能
 */
public class CachePerformanceMonitor {
    
    private static final Logger LOG = LoggerFactory.getLogger(CachePerformanceMonitor.class);
    
    private static final AtomicLong queryCount = new AtomicLong(0);
    private static final AtomicLong totalDuration = new AtomicLong(0);
    private static final AtomicLong maxDuration = new AtomicLong(0);
    private static final AtomicLong minDuration = new AtomicLong(Long.MAX_VALUE);
    
    private static RuleCacheManager cacheManager;
    
    /**
     * 初始化缓存管理器
     */
    public static void init(String mysqlUrl, String mysqlUser, String mysqlPassword,
                           String redisHost, int redisPort) throws Exception {
        RuleLoader ruleLoader = new RuleLoader(mysqlUrl, mysqlUser, mysqlPassword);
        JedisPool jedisPool = new JedisPool(redisHost, redisPort);
        cacheManager = new RuleCacheManager(ruleLoader, jedisPool);
        LOG.info("CachePerformanceMonitor initialized");
    }
    
    /**
     * 执行规则查询并记录性能
     */
    public static List<QualityRule> queryRulesWithMonitoring() {
        if (cacheManager == null) {
            throw new RuntimeException("CachePerformanceMonitor not initialized");
        }
        
        long startTime = System.nanoTime();
        List<QualityRule> rules = cacheManager.getAllRules();
        long endTime = System.nanoTime();
        
        long durationNanos = endTime - startTime;
        long durationMicros = durationNanos / 1000; // 微秒
        long durationMs = durationMicros / 1000; // 毫秒
        
        // 更新统计
        queryCount.incrementAndGet();
        totalDuration.addAndGet(durationMicros);
        maxDuration.updateAndGet(current -> Math.max(current, durationMicros));
        minDuration.updateAndGet(current -> Math.min(current, durationMicros));
        
        // 记录慢查询（> 10ms）
        if (durationMs > 10) {
            LOG.warn("Slow rule query: {} ms ({} μs)", durationMs, durationMicros);
        }
        
        // 每 1000 次查询打印一次统计
        if (queryCount.get() % 1000 == 0) {
            LOG.info("Cache query count: {}, avg duration: {} μs", 
                queryCount.get(), totalDuration.get() / queryCount.get());
        }
        
        return rules;
    }
    
    /**
     * 打印性能统计
     */
    public static void printStats() {
        long count = queryCount.get();
        if (count == 0) {
            LOG.info("No queries executed");
            return;
        }
        
        long avgDuration = totalDuration.get() / count;
        long maxDurationMicros = maxDuration.get();
        long minDurationMicros = minDuration.get();
        
        LOG.info("=== Cache Performance Statistics ===");
        LOG.info("Total queries: {}", count);
        LOG.info("Average duration: {}.{} ms ({} μs)", 
            avgDuration / 1000, avgDuration % 1000, avgDuration);
        LOG.info("Max duration: {}.{} ms ({} μs)", 
            maxDurationMicros / 1000, maxDurationMicros % 1000, maxDurationMicros);
        LOG.info("Min duration: {}.{} ms ({} μs)", 
            minDurationMicros / 1000, minDurationMicros % 1000, minDurationMicros);
        LOG.info("Total time: {} ms", totalDuration.get() / 1000);
        LOG.info("=====================================");
    }
    
    /**
     * 重置统计
     */
    public static void reset() {
        queryCount.set(0);
        totalDuration.set(0);
        maxDuration.set(0);
        minDuration.set(Long.MAX_VALUE);
    }
    
    /**
     * 获取平均查询时间（微秒）
     */
    public static long getAverageDurationMicros() {
        long count = queryCount.get();
        return count == 0 ? 0 : totalDuration.get() / count;
    }
}
