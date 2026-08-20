package com.dataplatform.quality.cache;

import com.alibaba.fastjson2.JSON;
import com.dataplatform.quality.model.QualityRule;
import com.dataplatform.quality.model.RuleChange;
import com.dataplatform.quality.rule.RuleLoader;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 规则缓存管理器 - 支持版本号检查和增量更新
 * 
 * 三层缓存架构:
 * L1: Caffeine (本地缓存) - 性能最优
 * L2: Redis (分布式缓存) - 多进程共享
 * L3: MySQL (持久化存储) - 数据源
 */
public class RuleCacheManager {
    private static final Logger LOG = LoggerFactory.getLogger(RuleCacheManager.class);
    
    // 缓存键
    private static final String REDIS_KEY_ALL = "quality:rules:all";
    private static final String REDIS_KEY_VERSION = "quality:rules:version";
    private static final String REDIS_KEY_RULE_PREFIX = "quality:rule:";
    
    // 缓存过期时间
    private static final int REDIS_CACHE_EXPIRE_SECONDS = 60;
    private static final int CAFFEINE_CACHE_EXPIRE_MINUTES = 5;
    
    // 本地缓存
    private final Cache<String, List<QualityRule>> localCache;
    private final Cache<String, QualityRule> localRuleCache;
    
    // 规则映射 (用于增量更新)
    private final Map<Long, QualityRule> ruleMap = new ConcurrentHashMap<>();
    
    // 当前规则版本号
    private volatile Long currentRuleVersion = 0L;
    
    // 依赖
    private final RuleLoader ruleLoader;
    private final JedisPool jedisPool;
    
    // 刷新锁 (防止并发刷新)
    private final Object refreshLock = new Object();
    
    public RuleCacheManager(RuleLoader ruleLoader, JedisPool jedisPool) {
        this.ruleLoader = ruleLoader;
        this.jedisPool = jedisPool;
        
        // 初始化 Caffeine 缓存
        this.localCache = Caffeine.newBuilder()
                .expireAfterWrite(CAFFEINE_CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
                .maximumSize(100)
                .recordStats()
                .build();
        
        this.localRuleCache = Caffeine.newBuilder()
                .expireAfterWrite(CAFFEINE_CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
                .maximumSize(10000)
                .recordStats()
                .build();
        
        // 初始化版本号
        initializeVersion();
    }
    
    /**
     * 初始化版本号
     */
    private void initializeVersion() {
        try (Jedis jedis = jedisPool.getResource()) {
            String versionStr = jedis.get(REDIS_KEY_VERSION);
            if (versionStr != null) {
                currentRuleVersion = Long.parseLong(versionStr);
                LOG.info("Initialized rule version from Redis: {}", currentRuleVersion);
            } else {
                currentRuleVersion = ruleLoader.getLatestRuleVersion();
                jedis.setex(REDIS_KEY_VERSION, REDIS_CACHE_EXPIRE_SECONDS, currentRuleVersion.toString());
                LOG.info("Initialized rule version from MySQL: {}", currentRuleVersion);
            }
        } catch (Exception e) {
            LOG.warn("Redis not available during version initialization, using MySQL: {}", e.getMessage());
            try {
                currentRuleVersion = ruleLoader.getLatestRuleVersion();
                LOG.info("Initialized rule version from MySQL (Redis unavailable): {}", currentRuleVersion);
            } catch (Exception ex) {
                LOG.error("Error initializing rule version from MySQL", ex);
                currentRuleVersion = 0L;
            }
        }
    }
    
    /**
     * 获取所有规则 (三层缓存查询)
     */
    public List<QualityRule> getAllRules() {
        // 第一层: 本地缓存 (Caffeine)
        List<QualityRule> rules = localCache.getIfPresent(REDIS_KEY_ALL);
        if (rules != null) {
            LOG.debug("Rules loaded from local cache, size: {}", rules.size());
            return rules;
        }
        
        // 第二层: Redis 缓存（如果Redis不可用，跳过）
        try {
            rules = loadFromRedis(REDIS_KEY_ALL);
            if (rules != null && !rules.isEmpty()) {
                localCache.put(REDIS_KEY_ALL, rules);
                LOG.debug("Rules loaded from Redis cache, size: {}", rules.size());
                return rules;
            }
        } catch (Exception e) {
            LOG.warn("Redis not available, falling back to MySQL: {}", e.getMessage());
        }
        
        // 第三层: MySQL 数据库
        rules = ruleLoader.loadAllRules();
        if (rules != null && !rules.isEmpty()) {
            try {
                saveToRedis(REDIS_KEY_ALL, rules);
            } catch (Exception e) {
                LOG.warn("Failed to save rules to Redis (Redis may not be available): {}", e.getMessage());
            }
            localCache.put(REDIS_KEY_ALL, rules);
            
            // 更新规则映射
            ruleMap.clear();
            for (QualityRule rule : rules) {
                ruleMap.put(rule.getRuleId(), rule);
            }
            
            LOG.info("Rules loaded from MySQL database, size: {}", rules.size());
        }
        
        return rules != null ? rules : new ArrayList<>();
    }
    
    /**
     * 获取单个规则
     */
    public QualityRule getRule(Long ruleId) {
        // 第一层: 本地缓存
        QualityRule rule = localRuleCache.getIfPresent(REDIS_KEY_RULE_PREFIX + ruleId);
        if (rule != null) {
            return rule;
        }
        
        // 第二层: 规则映射 (内存中的规则集合)
        rule = ruleMap.get(ruleId);
        if (rule != null) {
            localRuleCache.put(REDIS_KEY_RULE_PREFIX + ruleId, rule);
            return rule;
        }
        
        // 第三层: Redis 缓存
        rule = loadRuleFromRedis(ruleId);
        if (rule != null) {
            localRuleCache.put(REDIS_KEY_RULE_PREFIX + ruleId, rule);
            return rule;
        }
        
        // 第四层: MySQL 数据库
        rule = ruleLoader.loadRuleById(ruleId);
        if (rule != null) {
            saveRuleToRedis(rule);
            localRuleCache.put(REDIS_KEY_RULE_PREFIX + ruleId, rule);
        }
        
        return rule;
    }
    
    /**
     * 刷新规则 - 支持版本号检查和增量更新
     * 
     * 流程:
     * 1. 检查最新版本号
     * 2. 如果版本号未变，跳过刷新
     * 3. 如果版本号变了，进行增量更新
     * 4. 更新缓存
     */
    public void refresh() {
        synchronized (refreshLock) {
            try {
                // 步骤 1: 获取最新版本号
                Long latestVersion = ruleLoader.getLatestRuleVersion();
                
                // 步骤 2: 检查版本号是否变化
                if (latestVersion.equals(currentRuleVersion)) {
                    LOG.debug("Rule version unchanged ({}), skip refresh", currentRuleVersion);
                    return;
                }
                
                LOG.info("Rule version changed: {} -> {}, starting incremental refresh...", 
                    currentRuleVersion, latestVersion);
                
                // 步骤 3: 进行增量更新
                incrementalRefresh(latestVersion);
                
                // 步骤 4: 更新版本号
                currentRuleVersion = latestVersion;
                updateVersionInRedis(latestVersion);
                
                LOG.info("Rule refresh completed, new version: {}", latestVersion);
                
            } catch (Exception e) {
                LOG.error("Error refreshing rules", e);
            }
        }
    }
    
    /**
     * 增量更新规则
     * 
     * 只加载和处理变更的规则，而不是全量加载
     */
    private void incrementalRefresh(Long latestVersion) {
        // 获取变更日志
        List<RuleChange> changes = ruleLoader.getChangesSince(currentRuleVersion);
        
        if (changes.isEmpty()) {
            LOG.debug("No rule changes found since version {}", currentRuleVersion);
            return;
        }
        
        LOG.info("Processing {} rule changes", changes.size());
        
        // 应用增量变更
        for (RuleChange change : changes) {
            try {
                switch (change.getChangeType()) {
                    case "ADD":
                    case "UPDATE":
                        // 解析新规则
                        QualityRule newRule = JSON.parseObject(change.getNewValue(), QualityRule.class);
                        ruleMap.put(newRule.getRuleId(), newRule);
                        
                        // 更新 Redis 中的单个规则
                        saveRuleToRedis(newRule);
                        
                        // 清除本地缓存
                        localRuleCache.invalidate(REDIS_KEY_RULE_PREFIX + newRule.getRuleId());
                        
                        LOG.debug("Rule {}: {} (version: {})", 
                            change.getChangeType(), newRule.getRuleId(), change.getRuleVersion());
                        break;
                        
                    case "DELETE":
                        // 删除规则
                        ruleMap.remove(change.getRuleId());
                        
                        // 删除 Redis 中的规则
                        deleteRuleFromRedis(change.getRuleId());
                        
                        // 清除本地缓存
                        localRuleCache.invalidate(REDIS_KEY_RULE_PREFIX + change.getRuleId());
                        
                        LOG.debug("Rule DELETED: {} (version: {})", 
                            change.getRuleId(), change.getRuleVersion());
                        break;
                }
            } catch (Exception e) {
                LOG.error("Error processing rule change: {}", change.getChangeId(), e);
            }
        }
        
        // 更新全量规则缓存
        List<QualityRule> allRules = new ArrayList<>(ruleMap.values());
        saveToRedis(REDIS_KEY_ALL, allRules);
        localCache.invalidate(REDIS_KEY_ALL);
        
        LOG.info("Incremental refresh completed, {} changes applied", changes.size());
    }
    
    /**
     * 从 Redis 加载规则列表
     */
    private List<QualityRule> loadFromRedis(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.get(key);
            if (json != null && !json.isEmpty()) {
                return JSON.parseArray(json, QualityRule.class);
            }
        } catch (Exception e) {
            LOG.error("Error loading rules from Redis", e);
        }
        return null;
    }
    
    /**
     * 从 Redis 加载单个规则
     */
    private QualityRule loadRuleFromRedis(Long ruleId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.get(REDIS_KEY_RULE_PREFIX + ruleId);
            if (json != null && !json.isEmpty()) {
                return JSON.parseObject(json, QualityRule.class);
            }
        } catch (Exception e) {
            LOG.error("Error loading rule from Redis: {}", ruleId, e);
        }
        return null;
    }
    
    /**
     * 保存规则列表到 Redis
     */
    private void saveToRedis(String key, List<QualityRule> rules) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(rules);
            jedis.setex(key, REDIS_CACHE_EXPIRE_SECONDS, json);
        } catch (Exception e) {
            LOG.error("Error saving rules to Redis", e);
        }
    }
    
    /**
     * 保存单个规则到 Redis
     */
    private void saveRuleToRedis(QualityRule rule) {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = JSON.toJSONString(rule);
            jedis.setex(REDIS_KEY_RULE_PREFIX + rule.getRuleId(), REDIS_CACHE_EXPIRE_SECONDS, json);
        } catch (Exception e) {
            LOG.error("Error saving rule to Redis: {}", rule.getRuleId(), e);
        }
    }
    
    /**
     * 从 Redis 删除单个规则
     */
    private void deleteRuleFromRedis(Long ruleId) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(REDIS_KEY_RULE_PREFIX + ruleId);
        } catch (Exception e) {
            LOG.error("Error deleting rule from Redis: {}", ruleId, e);
        }
    }
    
    /**
     * 更新 Redis 中的版本号
     */
    private void updateVersionInRedis(Long version) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(REDIS_KEY_VERSION, REDIS_CACHE_EXPIRE_SECONDS, version.toString());
        } catch (Exception e) {
            LOG.error("Error updating rule version in Redis", e);
        }
    }
    
    /**
     * 获取当前规则版本号
     */
    public Long getCurrentRuleVersion() {
        return currentRuleVersion;
    }
    
    /**
     * 获取缓存统计信息
     */
    public Map<String, Object> getCacheStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("currentRuleVersion", currentRuleVersion);
        stats.put("totalRules", ruleMap.size());
        stats.put("localCacheStats", localCache.stats());
        stats.put("localRuleCacheStats", localRuleCache.stats());
        return stats;
    }
    
    /**
     * 清空所有缓存
     */
    public void clearCache() {
        synchronized (refreshLock) {
            localCache.invalidateAll();
            localRuleCache.invalidateAll();
            ruleMap.clear();
            LOG.info("All caches cleared");
        }
    }
}
