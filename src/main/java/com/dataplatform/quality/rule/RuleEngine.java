package com.dataplatform.quality.rule;

import com.dataplatform.quality.model.Order;
import com.dataplatform.quality.model.QualityRule;
import com.dataplatform.quality.model.RuleMetrics;
import com.dataplatform.quality.model.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 规则引擎 - 管理和执行所有规则
 * 
 * 功能:
 * 1. 规则执行和验证
 * 2. 规则级别监控 (执行次数、失败率、错误率、耗时)
 * 3. 熔断机制 (错误率超阈值自动禁用规则)
 * 4. 支持缓存延迟 (模拟分布式缓存场景)
 */
public class RuleEngine {
    private static final Logger LOG = LoggerFactory.getLogger(RuleEngine.class);
    
    // 使用 volatile 保证可见性
    // 使用不可变 List 保证线程安全
    private volatile List<QualityRule> rules;
    private volatile List<QualityRule> pendingRules;  // 待生效的规则
    private long cacheRefreshDelayMs = 0;    // 缓存刷新延迟（毫秒）
    private ScheduledExecutorService scheduler;
    
    /** 规则监控指标 (key=ruleId) */
    private final Map<Long, RuleMetrics> ruleMetricsMap = new ConcurrentHashMap<>();
    
    public RuleEngine() {
        this.rules = new ArrayList<>();
        this.pendingRules = null;
        this.scheduler = Executors.newScheduledThreadPool(1);
    }
    
    public RuleEngine(List<QualityRule> rules) {
        this.rules = rules != null ? rules : new ArrayList<>();
        this.pendingRules = null;
        this.scheduler = Executors.newScheduledThreadPool(1);
        // 按优先级排序 (数字越小优先级越高)
        this.rules.sort(Comparator.comparing(QualityRule::getPriority, 
            Comparator.nullsLast(Comparator.naturalOrder())));
    }
    
    /**
     * 执行所有规则验证 (带监控)
     * 
     * @param order 订单数据
     * @return 验证结果
     */
    public ValidationResult execute(Order order) {
        ValidationResult result = new ValidationResult(order);
        
        if (order == null) {
            result.addFailure("NULL_CHECK", "Order is null");
            return result;
        }
        
        // 按优先级执行规则
        for (QualityRule rule : getEnabledRules()) {
            // 获取或创建监控指标
            Long ruleId = rule.getRuleId();
            if (ruleId == null) {
                ruleId = (long) rule.getRuleName().hashCode();
            }
            
            final Long finalRuleId = ruleId;
            RuleMetrics metrics = ruleMetricsMap.computeIfAbsent(
                finalRuleId, 
                k -> new RuleMetrics(finalRuleId, rule.getRuleName())
            );
            
            // 熔断检查
            if (!metrics.shouldExecute()) {
                LOG.warn("Rule skipped due to circuit breaker: ruleId={}, ruleName={}", 
                    rule.getRuleId(), rule.getRuleName());
                result.addFailure(rule.getRuleName(), "CIRCUIT_BREAKER_OPEN");
                continue;
            }
            
            // 记录执行
            metrics.incrementExecution();
            long startTime = System.nanoTime();
            
            try {
                boolean isValid = RuleValidator.validate(order, rule);
                
                // 记录耗时
                long latencyNs = System.nanoTime() - startTime;
                metrics.recordLatency(latencyNs);
                
                if (!isValid) {
                    // 记录失败
                    metrics.incrementFailure();
                    
                    String reason = RuleValidator.getFailureReason(order, rule);
                    result.addFailure(rule.getRuleName(), reason);
                    
                    // 如果是 REJECT 动作,直接返回
                    if ("REJECT".equalsIgnoreCase(rule.getAction())) {
                        LOG.debug("Order {} rejected by rule: {}", order.getOrderId(), rule.getRuleName());
                        break;
                    }
                }
            } catch (Exception e) {
                // 记录异常
                metrics.incrementError();
                
                LOG.error("Rule execution error: ruleId={}, ruleName={}, orderId={}, error={}", 
                    rule.getRuleId(), rule.getRuleName(), order.getOrderId(), e.getMessage(), e);
                
                // 关键改进：不再默认通过，而是记录异常
                result.addFailure(rule.getRuleName(), "EXECUTION_ERROR: " + e.getMessage());
                
                // 如果是 REJECT 动作，异常也应该拒绝
                if ("REJECT".equalsIgnoreCase(rule.getAction())) {
                    break;
                }
            }
        }
        
        return result;
    }
    
    /**
     * 批量验证
     */
    public List<ValidationResult> executeBatch(List<Order> orders) {
        return orders.stream()
            .map(this::execute)
            .collect(Collectors.toList());
    }
    
    /**
     * 获取启用的规则
     * 
     * 线程安全：volatile 读 + 创建新 List 避免并发修改
     */
    public List<QualityRule> getEnabledRules() {
        // volatile 读，保证可见性
        List<QualityRule> currentRules = this.rules;
        
        // 创建新 List，避免并发修改异常
        return currentRules.stream()
            .filter(rule -> rule.getEnabled() != null && rule.getEnabled())
            .collect(Collectors.toList());
    }
    
    /**
     * 更新规则列表（支持缓存延迟）
     * 
     * 如果设置了缓存刷新延迟，新规则会在延迟后才生效
     * 这模拟了真实的分布式缓存场景
     * 
     * 线程安全：使用 volatile + 不可变 List 保证可见性和安全性
     */
    public void updateRules(List<QualityRule> newRules) {
        if (cacheRefreshDelayMs > 0) {
            // 异步更新：新规则先保存到 pendingRules，延迟后才应用
            // 创建不可变副本，避免外部修改
            List<QualityRule> immutableRules = new ArrayList<>(newRules != null ? newRules : new ArrayList<>());
            this.pendingRules = immutableRules;
            
            LOG.info("Rules update scheduled with delay: {}ms, total: {}, enabled: {}", 
                cacheRefreshDelayMs, pendingRules.size(), 
                pendingRules.stream().filter(r -> r.getEnabled() != null && r.getEnabled()).count());
            
            // 延迟后应用新规则
            scheduler.schedule(() -> {
                List<QualityRule> sortedRules = new ArrayList<>(this.pendingRules);
                sortedRules.sort(Comparator.comparing(QualityRule::getPriority, 
                    Comparator.nullsLast(Comparator.naturalOrder())));
                
                // volatile 写，保证可见性
                this.rules = sortedRules;
                this.pendingRules = null;
                
                // 初始化新规则的监控指标
                initializeMetricsForNewRules();
                
                LOG.info("Rules applied after delay, total: {}, enabled: {}", 
                    rules.size(), getEnabledRules().size());
            }, cacheRefreshDelayMs, TimeUnit.MILLISECONDS);
        } else {
            // 同步更新：立即应用新规则
            // 创建不可变副本并排序
            List<QualityRule> sortedRules = new ArrayList<>(newRules != null ? newRules : new ArrayList<>());
            sortedRules.sort(Comparator.comparing(QualityRule::getPriority, 
                Comparator.nullsLast(Comparator.naturalOrder())));
            
            // volatile 写，保证可见性
            this.rules = sortedRules;
            this.pendingRules = null;
            
            // 初始化新规则的监控指标
            initializeMetricsForNewRules();
            
            LOG.info("Rules updated, total: {}, enabled: {}", 
                rules.size(), getEnabledRules().size());
        }
    }
    
    /**
     * 初始化新规则的监控指标
     */
    private void initializeMetricsForNewRules() {
        for (QualityRule rule : rules) {
            // 如果规则没有 ID，使用规则名称作为 ID
            Long ruleId = rule.getRuleId();
            if (ruleId == null) {
                ruleId = (long) rule.getRuleName().hashCode();
            }
            
            final Long finalRuleId = ruleId;
            ruleMetricsMap.computeIfAbsent(
                finalRuleId, 
                k -> new RuleMetrics(finalRuleId, rule.getRuleName())
            );
        }
    }
    
    /**
     * 添加规则
     */
    public void addRule(QualityRule rule) {
        if (rule != null) {
            rules.add(rule);
            rules.sort(Comparator.comparing(QualityRule::getPriority, 
                Comparator.nullsLast(Comparator.naturalOrder())));
            LOG.info("Rule added: {}", rule.getRuleName());
        }
    }
    
    /**
     * 移除规则
     */
    public void removeRule(Long ruleId) {
        rules.removeIf(rule -> rule.getRuleId().equals(ruleId));
        LOG.info("Rule removed: {}", ruleId);
    }
    
    /**
     * 获取规则数量
     */
    public int getRuleCount() {
        return rules.size();
    }
    
    /**
     * 获取启用规则数量
     */
    public int getEnabledRuleCount() {
        return getEnabledRules().size();
    }
    
    /**
     * 设置缓存刷新延迟（毫秒）
     * 
     * 用于模拟真实的分布式缓存场景，规则更新后不会立即生效
     * 而是在指定的延迟后才生效
     */
    public void setCacheRefreshDelayMs(long delayMs) {
        this.cacheRefreshDelayMs = delayMs;
        LOG.info("Cache refresh delay set to: {}ms", delayMs);
    }
    
    /**
     * 获取缓存刷新延迟
     */
    public long getCacheRefreshDelayMs() {
        return cacheRefreshDelayMs;
    }
    
    /**
     * 获取规则监控指标
     */
    public Map<Long, RuleMetrics> getRuleMetrics() {
        return ruleMetricsMap;
    }
    
    /**
     * 获取指定规则的监控指标
     */
    public RuleMetrics getRuleMetrics(Long ruleId) {
        return ruleMetricsMap.get(ruleId);
    }
    
    /**
     * 打印监控统计信息
     */
    public void printMetricsStats() {
        LOG.info("========== Rule Execution Metrics ==========");
        for (RuleMetrics metrics : ruleMetricsMap.values()) {
            LOG.info(metrics.toString());
        }
        LOG.info("===========================================");
    }
    
    /**
     * 手动打开规则熔断器
     */
    public void openCircuitBreaker(Long ruleId) {
        RuleMetrics metrics = ruleMetricsMap.get(ruleId);
        if (metrics != null) {
            metrics.openCircuit();
            LOG.warn("Circuit breaker manually opened for rule: ruleId={}", ruleId);
        }
    }
    
    /**
     * 手动关闭规则熔断器
     */
    public void closeCircuitBreaker(Long ruleId) {
        RuleMetrics metrics = ruleMetricsMap.get(ruleId);
        if (metrics != null) {
            metrics.closeCircuit();
            LOG.info("Circuit breaker manually closed for rule: ruleId={}", ruleId);
        }
    }
}
