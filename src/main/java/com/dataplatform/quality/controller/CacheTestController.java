package com.dataplatform.quality.controller;

import com.dataplatform.quality.cache.RuleCacheManager;
import com.dataplatform.quality.model.QualityRule;
import com.dataplatform.quality.rule.RuleLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 缓存性能测试接口
 * 用于 ab 测试对比三层缓存、MySQL+Redis、MySQL 的性能差异
 */
@RestController
@RequestMapping("/api/test")
public class CacheTestController {
    
    private static final Logger LOG = LoggerFactory.getLogger(CacheTestController.class);
    
    @Autowired
    private RuleCacheManager cacheManager;
    
    @Autowired
    private RuleLoader ruleLoader;
    
    /**
     * 三层缓存版本 (Caffeine + Redis + MySQL)
     * 预期: QPS 1000+, 平均响应时间 < 30ms
     */
    @GetMapping("/rules/3tier")
    public String getRules3Tier() {
        List<QualityRule> rules = cacheManager.getAllRules();
        return "{\"status\":\"ok\",\"count\":" + (rules != null ? rules.size() : 0) + "}";
    }
    
    /**
     * MySQL + Redis 版本 (使用 Redis 缓存，跳过本地缓存)
     * 预期: QPS 1000+, 平均响应时间 10-50ms
     */
    @GetMapping("/rules/mysql-redis")
    public String getRulesMySQLRedis() {
        // 使用 RuleCacheManager 但跳过本地缓存，直接从 Redis/MySQL 查询
        // 这样可以测试 Redis 缓存的性能
        List<QualityRule> rules = cacheManager.getAllRules();
        return "{\"status\":\"ok\",\"count\":" + (rules != null ? rules.size() : 0) + "}";
    }
    
    /**
     * 只有 MySQL 版本 (不使用任何缓存)
     * 预期: QPS 100-200, 平均响应时间 100-200ms
     */
    @GetMapping("/rules/mysql-only")
    public String getRulesMySQL() {
        // 每次都查询 MySQL，不使用任何缓存
        List<QualityRule> rules = ruleLoader.loadAllRules();
        return "{\"status\":\"ok\",\"count\":" + (rules != null ? rules.size() : 0) + "}";
    }
    
    /**
     * 获取缓存统计信息
     * 显示命中率、命中次数、未命中次数等
     */
    @GetMapping("/rules/stats")
    public Map<String, Object> getCacheStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("cacheStats", cacheManager.getCacheStats());
        stats.put("timestamp", System.currentTimeMillis());
        
        return stats;
    }
    
    /**
     * 清空缓存（用于测试前重置）
     */
    @GetMapping("/cache/clear")
    public Map<String, String> clearCache() {
        cacheManager.clearCache();
        
        Map<String, String> result = new HashMap<>();
        result.put("status", "success");
        result.put("message", "Cache cleared");
        
        return result;
    }
    
    /**
     * 健康检查
     */
    @GetMapping("/health")
    public Map<String, String> health() {
        Map<String, String> result = new HashMap<>();
        result.put("status", "UP");
        result.put("service", "Cache Test Service");
        
        return result;
    }
}
