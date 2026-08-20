package com.dataplatform.quality.function;

import com.dataplatform.quality.model.Order;
import com.dataplatform.quality.model.QualityRule;
import com.dataplatform.quality.model.ValidationResult;
import com.dataplatform.quality.rule.RuleEngine;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * 基于广播流的规则引擎处理函数
 * 
 * 优势：
 * 1. 强一致性：所有 Task 在同一个 Checkpoint 周期内原子性更新规则
 * 2. 可追踪性：可以通过 Checkpoint 追踪规则版本
 * 3. 可回滚性：可以回滚到历史 Checkpoint
 * 4. 顺序性：Kafka 保证分区内有序
 * 5. 可靠性：Kafka 持久化，支持重放
 * 
 * 劣势：
 * 1. 更新延迟：取决于 Checkpoint 间隔（5 分钟）
 * 2. 耦合性：规则管理系统需要写 Kafka
 * 3. 资源开销：每个 Task 需要维护广播状态
 * 
 * 适用场景：
 * - 金融风控：需要强一致性，不能容忍不同 Task 使用不同规则
 * - 合规审计：需要追踪每条数据用了哪个版本的规则
 * - A/B 测试：需要精确控制规则版本切换时间点
 * - 规则回滚：需要支持规则回滚到历史版本
 */
/*
public class RuleEngineWithBroadcastFunction 
    extends KeyedBroadcastProcessFunction<String, Order, QualityRule, Order> {
    
    private static final Logger LOG = LoggerFactory.getLogger(RuleEngineWithBroadcastFunction.class);
    
    // 侧输出标签 - 异常数据
    public static final OutputTag<Order> INVALID_DATA_TAG = 
        new OutputTag<Order>("invalid-data") {};
    
    // 侧输出标签 - 质量指标
    public static final OutputTag<ValidationResult> METRICS_TAG = 
        new OutputTag<ValidationResult>("quality-metrics") {};
    
    // 广播状态描述符
    private final MapStateDescriptor<Long, QualityRule> ruleStateDescriptor;
    
    // 规则引擎（本地缓存，用于快速校验）
    private transient RuleEngine ruleEngine;
    
    // 当前规则版本号
    private transient volatile long currentRuleVersion = 0;
    
    // 统计计数器
    private long totalCount = 0;
    private long validCount = 0;
    private long invalidCount = 0;
    
    // Flink Metrics
    private transient Counter totalCounter;
    private transient Counter validCounter;
    private transient Counter invalidCounter;
    private transient Counter ruleUpdateCounter;  // 规则更新次数
    
    public RuleEngineWithBroadcastFunction(MapStateDescriptor<Long, QualityRule> ruleStateDescriptor) {
        this.ruleStateDescriptor = ruleStateDescriptor;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 初始化规则引擎（空规则列表，等待广播流更新）
        ruleEngine = new RuleEngine(new ArrayList<>());
        
        // 初始化 Flink Metrics
        totalCounter = getRuntimeContext()
            .getMetricGroup()
            .counter("totalRecords");
        
        validCounter = getRuntimeContext()
            .getMetricGroup()
            .counter("validRecords");
        
        invalidCounter = getRuntimeContext()
            .getMetricGroup()
            .counter("invalidRecords");
        
        ruleUpdateCounter = getRuntimeContext()
            .getMetricGroup()
            .addGroup("broadcast")
            .counter("ruleUpdateCount");
        
        // 注册规则版本 Gauge
        getRuntimeContext()
            .getMetricGroup()
            .addGroup("broadcast")
            .gauge("currentRuleVersion", () -> currentRuleVersion);
        
        LOG.info("RuleEngineWithBroadcastFunction initialized, waiting for broadcast rules...");
    }
    
    // 处理数据流元素（订单数据）
    // 注意：这里使用 ReadOnlyBroadcastState，保证线程安全
    @Override
    public void processElement(Order order, ReadOnlyContext ctx, Collector<Order> out) throws Exception {
        totalCount++;
        totalCounter.inc();
        
        // 1. 读取广播状态中的规则（只读，线程安全）
        ReadOnlyBroadcastState<Long, QualityRule> ruleState = 
            ctx.getBroadcastState(ruleStateDescriptor);
        
        // 2. 如果规则引擎为空或版本不一致，重新加载规则
        if (ruleEngine.getRuleCount() == 0 || needReloadRules(ruleState)) {
            reloadRulesFromBroadcastState(ruleState);
        }
        
        // 3. 使用规则引擎校验订单
        ValidationResult result = ruleEngine.execute(order);
        
        // 4. 输出结果
        if (result.getIsValid()) {
            // 正常数据 → 主输出流
            out.collect(order);
            validCount++;
            validCounter.inc();
        } else {
            // 异常数据 → 侧输出流
            ctx.output(INVALID_DATA_TAG, order);
            invalidCount++;
            invalidCounter.inc();
            
            LOG.debug("Invalid order detected: orderId={}, failures={}", 
                order.getOrderId(), result.getFailureSummary());
        }
        
        // 5. 输出质量指标 → 侧输出流
        ctx.output(METRICS_TAG, result);
        
        // 6. 每处理 10000 条数据，输出统计信息
        if (totalCount % 10000 == 0) {
            double passRate = (double) validCount / totalCount * 100;
            LOG.info("Quality Statistics - Total: {}, Valid: {}, Invalid: {}, PassRate: {:.2f}%, RuleVersion: {}",
                totalCount, validCount, invalidCount, passRate, currentRuleVersion);
        }
    }
    
    // 处理广播流元素（规则更新）
    // 注意：这里使用 BroadcastState，可以修改状态
    // 一致性保障：
    // 1. 所有 Task 在同一个 Checkpoint 周期内收到广播事件
    // 2. 所有 Task 在同一个 Checkpoint 周期内更新规则
    // 3. Checkpoint 完成后，所有 Task 都使用新版本规则
    @Override
    public void processBroadcastElement(QualityRule rule, Context ctx, Collector<Order> out) throws Exception {
        // 1. 获取广播状态（可写）
        BroadcastState<Long, QualityRule> ruleState = 
            ctx.getBroadcastState(ruleStateDescriptor);
        
        // 2. 更新广播状态
        if (rule.getIsDeleted() != null && rule.getIsDeleted()) {
            // 删除规则
            ruleState.remove(rule.getRuleId());
            LOG.info("Rule deleted from broadcast state: ruleId={}, ruleName={}", 
                rule.getRuleId(), rule.getRuleName());
        } else {
            // 添加或更新规则
            ruleState.put(rule.getRuleId(), rule);
            LOG.info("Rule updated in broadcast state: ruleId={}, ruleName={}, version={}, enabled={}", 
                rule.getRuleId(), rule.getRuleName(), rule.getVersion(), rule.getIsEnabled());
        }
        
        // 3. 更新规则版本号
        if (rule.getVersion() != null && rule.getVersion() > currentRuleVersion) {
            currentRuleVersion = rule.getVersion();
        }
        
        // 4. 重新加载规则到本地缓存（用于快速校验）
        reloadRulesFromBroadcastState(ruleState);
        
        // 5. 更新 Metrics
        ruleUpdateCounter.inc();
        
        LOG.info("Broadcast rule processed, total rules in state: {}, current version: {}", 
            getRuleCountFromState(ruleState), currentRuleVersion);
    }
    
    // 判断是否需要重新加载规则
    private boolean needReloadRules(ReadOnlyBroadcastState<Long, QualityRule> ruleState) throws Exception {
        // 如果广播状态中的规则数量与本地缓存不一致，需要重新加载
        int stateRuleCount = getRuleCountFromState(ruleState);
        int localRuleCount = ruleEngine.getRuleCount();
        
        return stateRuleCount != localRuleCount;
    }
    
    // 从广播状态重新加载规则到本地缓存
    private void reloadRulesFromBroadcastState(ReadOnlyBroadcastState<Long, QualityRule> ruleState) throws Exception {
        List<QualityRule> rules = new ArrayList<>();
        
        // 遍历广播状态中的所有规则
        for (Map.Entry<Long, QualityRule> entry : ruleState.immutableEntries()) {
            rules.add(entry.getValue());
        }
        
        // 更新规则引擎
        ruleEngine.updateRules(rules);
        
        LOG.info("Rules reloaded from broadcast state: total={}, enabled={}, version={}", 
            ruleEngine.getRuleCount(), ruleEngine.getEnabledRuleCount(), currentRuleVersion);
    }
    
    // 获取广播状态中的规则数量
    private int getRuleCountFromState(ReadOnlyBroadcastState<Long, QualityRule> ruleState) throws Exception {
        int count = 0;
        for (Map.Entry<Long, QualityRule> entry : ruleState.immutableEntries()) {
            count++;
        }
        return count;
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        
        // 输出最终统计
        double passRate = totalCount > 0 ? (double) validCount / totalCount * 100 : 0.0;
        LOG.info("RuleEngineWithBroadcastFunction closed");
        LOG.info("Final Statistics - Total: {}, Valid: {}, Invalid: {}, PassRate: {:.2f}%, RuleVersion: {}",
            totalCount, validCount, invalidCount, passRate, currentRuleVersion);
    }
}
*/
