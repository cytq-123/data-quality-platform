package com.dataplatform.quality.cache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis 规则变更发布器
 * 
 * 用于发布规则变更通知到 Redis Pub/Sub 频道
 * Flink 任务会订阅这个频道，接收规则变更通知
 */
public class RedisRulePublisher {
    
    private static final Logger logger = LoggerFactory.getLogger(RedisRulePublisher.class);
    
    private final JedisPool jedisPool;
    private static final String RULE_CHANGE_CHANNEL = "rule_changes";
    
    public RedisRulePublisher(String host, int port) {
        this.jedisPool = new JedisPool(host, port);
    }
    
    public RedisRulePublisher(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }
    
    /**
     * 发布规则变更通知
     * 
     * @param message 通知消息，格式：RULE_ADDED:ruleId 或 RULE_UPDATED:ruleId 或 RULE_DELETED:ruleId
     */
    public void publishRuleChange(String message) {
        try (Jedis jedis = jedisPool.getResource()) {
            long subscribers = jedis.publish(RULE_CHANGE_CHANNEL, message);
            logger.info("Published rule change notification: channel={}, message={}, subscribers={}",
                RULE_CHANGE_CHANNEL, message, subscribers);
        } catch (Exception e) {
            logger.error("Failed to publish rule change notification: {}", message, e);
        }
    }
    
    /**
     * 发布规则添加通知
     * 
     * @param ruleId 规则ID
     */
    public void notifyRuleAdded(String ruleId) {
        publishRuleChange("RULE_ADDED:" + ruleId);
    }
    
    /**
     * 发布规则更新通知
     * 
     * @param ruleId 规则ID
     */
    public void notifyRuleUpdated(String ruleId) {
        publishRuleChange("RULE_UPDATED:" + ruleId);
    }
    
    /**
     * 发布规则删除通知
     * 
     * @param ruleId 规则ID
     */
    public void notifyRuleDeleted(String ruleId) {
        publishRuleChange("RULE_DELETED:" + ruleId);
    }
    
    /**
     * 简化版通知方法（用于测试）
     */
    public void notifyRuleUpdate() {
        publishRuleChange("RULE_UPDATED:ALL");
    }
    
    /**
     * 关闭连接池
     */
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            logger.info("RedisRulePublisher closed");
        }
    }
}
