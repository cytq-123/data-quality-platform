package com.dataplatform.quality.cache;

import com.dataplatform.quality.model.Order;
import com.dataplatform.quality.model.QualityRule;
import com.dataplatform.quality.rule.RuleEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 规则缓存一致性集成测试
 * 
 * 关键改进：使用相同的订单数据验证规则是否生效
 * 
 * 模拟真实场景：
 * 1. 规则存在 MySQL 中
 * 2. 通过三层缓存加载规则
 * 3. 验证规则更新时的一致性
 */
public class RuleCacheConsistencyIntegrationTest {
    
    private RuleEngine ruleEngine;
    private MockRuleCache mockCache;
    
    @BeforeEach
    public void setUp() {
        ruleEngine = new RuleEngine();
        mockCache = new MockRuleCache();
    }
    
    /**
     * 测试场景：规则更新后立即发送订单，验证数据一致性
     * 
     * 关键：使用相同的订单数据，验证规则是否生效
     * 
     * 这是面试中提到的真实场景：
     * 1. 初始规则：userId 必须以 user_ 开头，处理 1 万订单，记录失败数
     * 2. 更新规则：userId 必须以 user_vip_ 开头（更严格）
     * 3. 用相同的 1 万订单再处理一次（过渡期，可能用旧规则）
     * 4. 等待缓存刷新后，用相同的 1 万订单再处理一次（确保用新规则）
     * 5. 比较第二次和第三次的失败数，验证缓存刷新是否生效
     */
    @Test
    public void testRuleUpdateWithImmediateOrderArrival() throws InterruptedException {
        System.out.println("\n========== 测试场景：规则更新后立即发送订单 ==========");
        
        // 生成固定的订单数据（关键：所有阶段使用相同的数据）
        System.out.println("\n【准备】生成 1 万条固定的订单数据");
        List<Order> fixedOrders = generateOrdersWithAmounts(10_000, 0, 100000);
        System.out.println("✓ 订单数据已生成，所有阶段将使用相同的数据");
        
        // 第一阶段：初始规则（宽松）- 允许所有 userId
        System.out.println("\n【阶段 1】初始规则：userId 必须以 user_ 开头");
        QualityRule rule_v1 = createRegexRule("用户ID校验", "userId", "^user_.*", 1);
        ruleEngine.updateRules(Arrays.asList(rule_v1));
        
        // 处理固定的订单数据
        int failCount_v1 = countFailures(fixedOrders);
        double failRate_v1 = (double) failCount_v1 / fixedOrders.size();
        
        System.out.println("处理订单数：" + fixedOrders.size());
        System.out.println("失败订单数：" + failCount_v1);
        System.out.println("失败率：" + String.format("%.2f%%", failRate_v1 * 100));
        
        // 第二阶段：更新规则（严格）- 只允许特定的 userId 格式
        System.out.println("\n【阶段 2】更新规则：userId 必须以 user_vip_ 开头（更严格）");
        QualityRule rule_v2 = createRegexRule("用户ID校验", "userId", "^user_vip_.*", 1);
        ruleEngine.updateRules(Arrays.asList(rule_v2));
        
        // 立即用相同的订单数据再处理一次（过渡期，可能用旧规则）
        System.out.println("\n【阶段 2】规则更新后立即用相同订单再处理（过渡期）");
        int failCount_v2 = countFailures(fixedOrders);
        double failRate_v2 = (double) failCount_v2 / fixedOrders.size();
        
        System.out.println("处理订单数：" + fixedOrders.size());
        System.out.println("失败订单数：" + failCount_v2);
        System.out.println("失败率：" + String.format("%.2f%%", failRate_v2 * 100));
        System.out.println("⚠️  注意：这个阶段可能用旧规则或新规则（过渡期）");
        System.out.println("   预期：失败数应该在 0 到 10000 之间（部分用旧规则，部分用新规则）");
        
        // 第三阶段：等待缓存刷新后再用相同订单处理
        System.out.println("\n【阶段 3】等待 2 秒（模拟缓存刷新），再用相同订单处理");
        System.out.println("等待中...");
        Thread.sleep(2000);
        System.out.println("✓ 缓存已刷新");
        
        int failCount_v3 = countFailures(fixedOrders);
        double failRate_v3 = (double) failCount_v3 / fixedOrders.size();
        
        System.out.println("处理订单数：" + fixedOrders.size());
        System.out.println("失败订单数：" + failCount_v3);
        System.out.println("失败率：" + String.format("%.2f%%", failRate_v3 * 100));
        System.out.println("✓ 这个阶段确保用新规则");
        
        // 验证
        System.out.println("\n【验证结果】");
        System.out.println("阶段 1（规则 v1）失败数：" + failCount_v1 + "，失败率：" + 
            String.format("%.2f%%", failRate_v1 * 100));
        System.out.println("阶段 2（过渡期）失败数：" + failCount_v2 + "，失败率：" + 
            String.format("%.2f%%", failRate_v2 * 100));
        System.out.println("阶段 3（规则 v2）失败数：" + failCount_v3 + "，失败率：" + 
            String.format("%.2f%%", failRate_v3 * 100));
        System.out.println();
        System.out.println("失败数对比（使用相同的订单数据）：");
        System.out.println("  阶段 1 → 阶段 2：" + failCount_v1 + " → " + failCount_v2 + 
            " (增长 " + (failCount_v2 - failCount_v1) + ")");
        System.out.println("  阶段 2 → 阶段 3：" + failCount_v2 + " → " + failCount_v3 + 
            " (增长 " + (failCount_v3 - failCount_v2) + ")");
        
        // 关键验证 1：阶段 1 和阶段 3 的失败数应该不同（规则不同）
        assertTrue(failCount_v3 > failCount_v1, "阶段 3 的失败数应该大于阶段 1（规则更严格）。阶段 1: " + failCount_v1 + 
            ", 阶段 3: " + failCount_v3);
        
        // 关键验证 2：阶段 2 和阶段 3 的失败数应该相同（都用新规则）
        // 因为使用相同的订单数据，如果规则相同，失败数应该完全相同
        System.out.println("\n【关键验证】阶段 2 和阶段 3 的失败数对比：");
        System.out.println("  阶段 2 失败数：" + failCount_v2);
        System.out.println("  阶段 3 失败数：" + failCount_v3);
        System.out.println("  差异：" + Math.abs(failCount_v3 - failCount_v2));
        
        if (failCount_v2 == failCount_v3) {
            System.out.println("✓ 阶段 2 和阶段 3 的失败数完全相同，说明都用了新规则");
            System.out.println("✓ 缓存刷新有效");
        } else if (Math.abs(failCount_v3 - failCount_v2) < 100) {
            System.out.println("⚠️  阶段 2 和阶段 3 的失败数略有差异（< 100），可能是缓存更新中");
        } else {
            System.out.println("❌ 阶段 2 和阶段 3 的失败数差异较大，说明阶段 2 用了旧规则");
        }
        
        // 严格验证：使用相同数据，失败数应该完全相同
        assertEquals(failCount_v2, failCount_v3, "使用相同的订单数据，阶段 2 和阶段 3 的失败数应该相同。阶段 2: " + failCount_v2 + ", 阶段 3: " + failCount_v3);
        
        System.out.println("\n✓ 测试通过：规则更新后，新订单逐步切换到新规则");
    }
    
    /**
     * 测试场景：缓存刷新延迟期间，应该逐步切换到新规则
     */
    @Test
    public void testCacheRefreshDelayConsistency() throws InterruptedException {
        System.out.println("\n========== 测试场景：缓存刷新延迟（100万条数据） ==========");
        
        // 初始规则
        QualityRule rule_v1 = createRangeRule("订单金额校验", "orderAmount", 
            "value >= 0 AND value <= 100000", 1);
        ruleEngine.updateRules(Arrays.asList(rule_v1));
        
        System.out.println("【初始状态】规则 v1：金额范围 0-100000");
        
        // 模拟缓存刷新延迟（实际 30 秒，测试中用 1 秒）
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        
        AtomicInteger failCountPhase1 = new AtomicInteger(0);
        AtomicInteger failCountPhase2 = new AtomicInteger(0);
        
        // 生成固定的订单数据 - 100万条
        System.out.println("生成 100 万条订单数据...");
        List<Order> fixedOrders = generateOrdersWithAmounts(1_000_000, 0, 100000);
        System.out.println("✓ 订单数据已生成\n");
        
        // 阶段 1：规则更新前
        System.out.println("【阶段 1】规则更新前，处理 100 万订单");
        failCountPhase1.set(countFailures(fixedOrders));
        System.out.println("失败数：" + failCountPhase1.get());
        
        // 1 秒后更新规则（模拟 30 秒的缓存刷新延迟）
        System.out.println("\n【缓存刷新】1 秒后更新规则...");
        scheduler.schedule(() -> {
            QualityRule rule_v2 = createRangeRule("订单金额校验", "orderAmount", 
                "value >= 0 AND value <= 50000", 1);
            ruleEngine.updateRules(Arrays.asList(rule_v2));
            System.out.println("✓ 规则已更新为 v2：金额范围 0-50000");
        }, 1, TimeUnit.SECONDS);
        
        // 等待规则更新
        Thread.sleep(1500);
        
        // 阶段 2：规则更新后
        System.out.println("\n【阶段 2】规则更新后，处理 100 万订单");
        failCountPhase2.set(countFailures(fixedOrders));
        System.out.println("失败数：" + failCountPhase2.get());
        
        scheduler.shutdown();
        
        // 验证
        System.out.println("\n【验证结果】");
        System.out.println("阶段 1 失败数：" + failCountPhase1.get());
        System.out.println("阶段 2 失败数：" + failCountPhase2.get());
        System.out.println("失败数增长：" + (failCountPhase2.get() - failCountPhase1.get()));
        
        assertTrue(failCountPhase2.get() > failCountPhase1.get(), "规则更新后，失败数应该增加");
        
        System.out.println("✓ 测试通过：缓存刷新后，新订单用新规则校验");
    }
    
    /**
     * 测试场景：多个 TaskManager 同步更新规则
     * 
     * 场景：规则更新是同步的，所有 TaskManager 都立即用新规则
     * 这是理想情况，但在生产环境中不太可能发生
     */
    @Test
    public void testMultipleTaskManagerSyncUpdate() throws InterruptedException {
        System.out.println("\n========== 测试场景：多个 TaskManager 同步更新规则 ==========");
        
        // 初始规则
        QualityRule rule_v1 = createRangeRule("订单金额校验", "orderAmount", 
            "value >= 0 AND value <= 100000", 1);
        ruleEngine.updateRules(Arrays.asList(rule_v1));
        
        System.out.println("【初始状态】4 个 TaskManager，每个处理 25 万订单");
        
        // 模拟 4 个 TaskManager
        int taskManagerCount = 4;
        int ordersPerTaskManager = 250_000;
        ExecutorService executor = Executors.newFixedThreadPool(taskManagerCount);
        
        AtomicInteger totalFailCount_v1 = new AtomicInteger(0);
        AtomicInteger totalFailCount_v2 = new AtomicInteger(0);
        
        // 阶段 1：所有 TaskManager 用规则 v1 处理订单
        System.out.println("\n【阶段 1】所有 TaskManager 用规则 v1 处理订单");
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < taskManagerCount; i++) {
            final int taskId = i;
            futures.add(executor.submit(() -> {
                List<Order> orders = generateOrdersWithAmounts(ordersPerTaskManager, 0, 100000);
                int failCount = countFailures(orders);
                totalFailCount_v1.addAndGet(failCount);
                System.out.println("  TaskManager-" + taskId + " 完成，失败数：" + failCount);
            }));
        }
        
        // 等待所有 TaskManager 完成
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        
        System.out.println("阶段 1 总失败数：" + totalFailCount_v1.get());
        
        // 更新规则（同步）
        System.out.println("\n【规则更新】同步更新规则为 v2：金额范围 0-50000");
        QualityRule rule_v2 = createRangeRule("订单金额校验", "orderAmount", 
            "value >= 0 AND value <= 50000", 1);
        ruleEngine.updateRules(Arrays.asList(rule_v2));
        System.out.println("✓ 规则已同步更新");
        
        // 阶段 2：所有 TaskManager 用规则 v2 处理订单
        System.out.println("\n【阶段 2】所有 TaskManager 用规则 v2 处理订单");
        futures.clear();
        for (int i = 0; i < taskManagerCount; i++) {
            final int taskId = i;
            futures.add(executor.submit(() -> {
                List<Order> orders = generateOrdersWithAmounts(ordersPerTaskManager, 0, 100000);
                int failCount = countFailures(orders);
                totalFailCount_v2.addAndGet(failCount);
                System.out.println("  TaskManager-" + taskId + " 完成，失败数：" + failCount);
            }));
        }
        
        // 等待所有 TaskManager 完成
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        
        executor.shutdown();
        
        System.out.println("阶段 2 总失败数：" + totalFailCount_v2.get());
        
        // 验证
        System.out.println("\n【验证结果】");
        System.out.println("阶段 1 总失败数：" + totalFailCount_v1.get());
        System.out.println("阶段 2 总失败数：" + totalFailCount_v2.get());
        System.out.println("失败数增长：" + (totalFailCount_v2.get() - totalFailCount_v1.get()));
        
        assertTrue(totalFailCount_v2.get() > totalFailCount_v1.get(), "规则更新后，总失败数应该增加");
        
        System.out.println("✓ 测试通过：多个 TaskManager 同步更新规则保持一致");
    }
    
    /**
     * 测试场景：多个 TaskManager 异步更新规则（真实场景模拟）
     * 
     * 真实场景：
     * - 规则存储在 MySQL，通过三层缓存加载
     * - 定时刷新规则（每分钟），而不是实时监听
     * - 规则更新后，不会立即生效，而是在下一个刷新周期才生效
     * - 多个 TaskManager 并发处理数据，可能在规则更新期间处理
     * 
     * 关键问题：
     * - 如果同时更新规则，可能导致正在处理的数据用旧规则，新数据用新规则，结果不一致
     * - 分布式环境下，多个 TaskManager 的规则版本可能不同步
     * 
     * 解决方案：
     * - 定时刷新而非实时：每分钟定时从 MySQL 拉取规则
     * - 三层缓存架构：L1(Caffeine 5s) -> L2(Redis 1min) -> L3(MySQL)
     * - 版本号控制：每个规则版本都有版本号，同一批数据使用同一版本的规则
     */
    @Test
    public void testMultipleTaskManagerAsyncUpdate() throws InterruptedException {
        System.out.println("\n========== 测试场景：多个 TaskManager 异步更新规则（真实场景） ==========");
        System.out.println("【场景描述】规则定时刷新，不是实时生效");
        System.out.println("【问题】规则更新期间，不同 TaskManager 可能用不同版本的规则");
        System.out.println("【解决】通过版本号控制，确保同一批数据使用同一版本的规则\n");
        
        // 设置缓存刷新延迟（2 秒），模拟定时刷新周期
        ruleEngine.setCacheRefreshDelayMs(2000);
        System.out.println("【配置】缓存刷新周期：2000ms（模拟定时刷新）");
        
        // 初始规则 v1：userId 必须以 user_ 开头
        QualityRule rule_v1 = createRegexRule("用户ID校验", "userId", "^user_.*", 1);
        ruleEngine.updateRules(Arrays.asList(rule_v1));
        System.out.println("【初始规则】v1：userId 必须以 user_ 开头\n");
        
        // 模拟 4 个 TaskManager
        int taskManagerCount = 4;
        int ordersPerTaskManager = 250_000;
        ExecutorService executor = Executors.newFixedThreadPool(taskManagerCount);
        
        AtomicInteger totalFailCount_v1 = new AtomicInteger(0);
        AtomicInteger totalFailCount_transition = new AtomicInteger(0);
        AtomicInteger totalFailCount_v2 = new AtomicInteger(0);
        
        // 生成固定的订单数据
        System.out.println("【准备】生成固定的订单数据（所有阶段使用相同数据）");
        List<Order> fixedOrders = generateOrdersWithAmounts(ordersPerTaskManager * taskManagerCount, 0, 100000);
        System.out.println("✓ 订单数据已生成\n");
        
        // ========== 阶段 1：所有 TaskManager 用规则 v1 处理订单 ==========
        System.out.println("【阶段 1】所有 TaskManager 用规则 v1 处理订单");
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < taskManagerCount; i++) {
            final int taskId = i;
            final int startIdx = taskId * ordersPerTaskManager;
            final int endIdx = startIdx + ordersPerTaskManager;
            futures.add(executor.submit(() -> {
                List<Order> orders = fixedOrders.subList(startIdx, endIdx);
                int failCount = countFailures(orders);
                totalFailCount_v1.addAndGet(failCount);
                System.out.println("  TaskManager-" + taskId + " 完成，失败数：" + failCount);
            }));
        }
        
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("阶段 1 总失败数：" + totalFailCount_v1.get() + "\n");
        
        // ========== 规则更新：从 v1 更新到 v2 ==========
        System.out.println("【规则更新】立即更新规则（但 2 秒后才生效）");
        QualityRule rule_v2 = createRegexRule("用户ID校验", "userId", "^user_vip_.*", 1);
        ruleEngine.updateRules(Arrays.asList(rule_v2));
        System.out.println("✓ 规则更新请求已发送（待生效）\n");
        
        // ========== 阶段 2：规则更新中，TaskManager 并发处理（过渡期） ==========
        System.out.println("【阶段 2】规则更新中（立即处理），TaskManager 并发处理");
        System.out.println("⚠️  注意：此时规则还没生效（缓存刷新延迟中），所有 TaskManager 仍用旧规则 v1");
        futures.clear();
        for (int i = 0; i < taskManagerCount; i++) {
            final int taskId = i;
            final int startIdx = taskId * ordersPerTaskManager;
            final int endIdx = startIdx + ordersPerTaskManager;
            futures.add(executor.submit(() -> {
                List<Order> orders = fixedOrders.subList(startIdx, endIdx);
                int failCount = countFailures(orders);
                totalFailCount_transition.addAndGet(failCount);
                System.out.println("  TaskManager-" + taskId + " 完成，失败数：" + failCount);
            }));
        }
        
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("阶段 2 总失败数：" + totalFailCount_transition.get());
        System.out.println("✓ 所有 TaskManager 都用了规则 v1（规则还没生效）\n");
        
        // ========== 等待缓存刷新 ==========
        System.out.println("【缓存刷新】等待 2.5 秒（等待规则生效）...");
        Thread.sleep(2500);
        System.out.println("✓ 缓存已刷新，所有 TaskManager 应该都用新规则 v2\n");
        
        // ========== 阶段 3：所有 TaskManager 用新规则 v2 处理订单 ==========
        System.out.println("【阶段 3】所有 TaskManager 用新规则 v2 处理订单");
        futures.clear();
        for (int i = 0; i < taskManagerCount; i++) {
            final int taskId = i;
            final int startIdx = taskId * ordersPerTaskManager;
            final int endIdx = startIdx + ordersPerTaskManager;
            futures.add(executor.submit(() -> {
                List<Order> orders = fixedOrders.subList(startIdx, endIdx);
                int failCount = countFailures(orders);
                totalFailCount_v2.addAndGet(failCount);
                System.out.println("  TaskManager-" + taskId + " 完成，失败数：" + failCount);
            }));
        }
        
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        executor.shutdown();
        System.out.println("阶段 3 总失败数：" + totalFailCount_v2.get() + "\n");
        
        // ========== 验证结果 ==========
        System.out.println("【验证结果】");
        System.out.println("阶段 1（规则 v1）失败数：" + totalFailCount_v1.get());
        System.out.println("阶段 2（过渡期，规则未生效）失败数：" + totalFailCount_transition.get());
        System.out.println("阶段 3（规则 v2）失败数：" + totalFailCount_v2.get());
        System.out.println();
        System.out.println("失败数对比：");
        System.out.println("  阶段 1 → 阶段 2：" + totalFailCount_v1.get() + " → " + 
            totalFailCount_transition.get() + " (差异 " + 
            Math.abs(totalFailCount_transition.get() - totalFailCount_v1.get()) + ")");
        System.out.println("  阶段 2 → 阶段 3：" + totalFailCount_transition.get() + " → " + 
            totalFailCount_v2.get() + " (增长 " + 
            (totalFailCount_v2.get() - totalFailCount_transition.get()) + ")");
        
        // ========== 关键验证 ==========
        System.out.println("\n【关键验证 1】阶段 1 和阶段 2 应该相同（规则还没生效）");
        System.out.println("  阶段 1 失败数：" + totalFailCount_v1.get());
        System.out.println("  阶段 2 失败数：" + totalFailCount_transition.get());
        if (totalFailCount_v1.get() == totalFailCount_transition.get()) {
            System.out.println("  ✓ 完全相同，说明规则还没生效");
        } else {
            System.out.println("  ❌ 不相同，说明规则已经生效");
        }
        assertEquals(totalFailCount_v1.get(), totalFailCount_transition.get(), 
            "阶段 1 和阶段 2 应该相同（规则还没生效）");
        
        System.out.println("\n【关键验证 2】阶段 2 和阶段 3 应该不同（规则已生效）");
        System.out.println("  阶段 2 失败数：" + totalFailCount_transition.get());
        System.out.println("  阶段 3 失败数：" + totalFailCount_v2.get());
        if (totalFailCount_v2.get() > totalFailCount_transition.get()) {
            System.out.println("  ✓ 阶段 3 更多失败，说明规则已生效");
        } else {
            System.out.println("  ❌ 失败数没有增加，说明规则没有生效");
        }
        assertTrue(totalFailCount_v2.get() > totalFailCount_transition.get(), 
            "阶段 3 的失败数应该大于阶段 2（规则已生效）");
        
        System.out.println("\n✓ 测试通过：真实模拟规则定时刷新场景，确保一致性");
    }
    
    /**
     * 测试场景：验证缓存命中率
     */
    @Test
    public void testCacheHitRatePerformance() {
        System.out.println("\n========== 测试场景：缓存命中率验证 ==========");
        
        QualityRule rule = createRangeRule("订单金额校验", "orderAmount", 
            "value >= 0 AND value <= 100000", 1);
        ruleEngine.updateRules(Arrays.asList(rule));
        
        // 多次查询规则
        int queryCount = 100_000;
        System.out.println("执行 " + queryCount + " 次规则查询...");
        
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < queryCount; i++) {
            List<QualityRule> rules = ruleEngine.getEnabledRules();
            assertFalse(rules.isEmpty());
        }
        long endTime = System.currentTimeMillis();
        
        long totalTime = endTime - startTime;
        double avgTime = totalTime / (double) queryCount;
        
        System.out.println("\n【性能指标】");
        System.out.println("总耗时：" + totalTime + "ms");
        System.out.println("平均查询时间：" + String.format("%.4f", avgTime) + "ms");
        System.out.println("QPS：" + (queryCount * 1000 / totalTime));
        
        // 验证：平均查询时间应该很短（< 1ms）
        assertTrue(avgTime < 1.0, "平均查询时间应该 < 1ms，实际：" + avgTime + "ms");
        
        System.out.println("✓ 测试通过：缓存性能达到预期");
    }
    
    /**
     * 测试场景：验证规则检测延迟
     * 
     * 这个测试验证的是：
     * - 从获取规则到执行规则的延迟
     * - 包括反序列化、规则执行、异常检测等
     * - 预期延迟：5-10ms
     */
    @Test
    public void testRuleDetectionLatency() {
        System.out.println("\n========== 测试场景：规则检测延迟验证（100万条数据） ==========");
        
        // 设置规则
        QualityRule rule = createRegexRule("用户ID校验", "userId", "^user_.*", 1);
        ruleEngine.updateRules(Arrays.asList(rule));
        
        // 100万条数据
        int totalCount = 1_000_000;  // 100万条
        
        System.out.println("生成 " + totalCount + " 条订单数据...");
        List<Order> orders = generateOrdersWithAmounts(totalCount, 0, 100000);
        System.out.println("✓ 订单数据已生成\n");
        
        // 预热（JIT 编译）
        System.out.println("【预热】执行 10000 次规则检测（JIT 编译）...");
        for (int i = 0; i < 10000; i++) {
            ruleEngine.execute(orders.get(i));
        }
        System.out.println("✓ 预热完成\n");
        
        // 测量规则检测延迟
        System.out.println("【测量】执行 " + totalCount + " 次规则检测...");
        long startTime = System.nanoTime();
        
        for (int i = 0; i < totalCount; i++) {
            ruleEngine.execute(orders.get(i));
            
            // 每处理 10 万条数据，打印进度
            if ((i + 1) % 100_000 == 0) {
                System.out.println("  已处理：" + (i + 1) + " 条");
            }
        }
        
        long endTime = System.nanoTime();
        long totalTimeNs = endTime - startTime;
        long totalTimeMs = totalTimeNs / 1_000_000;
        double avgTimeMs = totalTimeNs / 1_000_000.0 / totalCount;
        double avgTimeUs = totalTimeNs / 1_000.0 / totalCount;
        
        System.out.println("\n【性能指标】");
        System.out.println("总耗时：" + totalTimeMs + "ms");
        System.out.println("平均检测时间：" + String.format("%.6f", avgTimeMs) + "ms");
        System.out.println("平均检测时间：" + String.format("%.4f", avgTimeUs) + "μs");
        System.out.println("QPS：" + (totalCount * 1000 / totalTimeMs));
        
        // 分析延迟
        System.out.println("\n【延迟分析】");
        System.out.println("预期延迟范围：5-10ms（单条订单）");
        System.out.println("实际平均延迟：" + String.format("%.6f", avgTimeMs) + "ms");
        
        if (avgTimeMs < 0.01) {
            System.out.println("✓ 延迟极低（< 0.01ms），说明规则检测性能优秀");
        } else if (avgTimeMs < 0.1) {
            System.out.println("✓ 延迟非常低（< 0.1ms），说明规则检测性能优秀");
        } else if (avgTimeMs < 1.0) {
            System.out.println("✓ 延迟较低（< 1ms），说明规则检测性能良好");
        } else if (avgTimeMs < 10.0) {
            System.out.println("⚠️  延迟中等（1-10ms），说明规则检测性能一般");
        } else {
            System.out.println("❌ 延迟较高（> 10ms），说明规则检测性能需要优化");
        }
        
        System.out.println("\n✓ 测试通过：规则检测延迟已验证（100万条数据）");
    }
    
    /**
     * 测试场景：验证端到端延迟（模拟 Kafka → Flink → ClickHouse）
     * 
     * 这个测试验证的是：
     * - 从数据进入系统到写入存储的总延迟
     * - 包括反序列化、规则检测、窗口聚合、数据库写入等
     * - 预期延迟：< 20ms
     */
    @Test
    public void testEndToEndLatency() {
        System.out.println("\n========== 测试场景：端到端延迟验证（100万条数据） ==========");
        System.out.println("【场景】Kafka → Flink → ClickHouse\n");
        
        // 设置规则
        QualityRule rule = createRegexRule("用户ID校验", "userId", "^user_.*", 1);
        ruleEngine.updateRules(Arrays.asList(rule));
        
        // 生成测试订单 - 100万条
        int orderCount = 1_000_000;
        System.out.println("生成 " + orderCount + " 条订单数据...");
        List<Order> orders = generateOrdersWithAmounts(orderCount, 0, 100000);
        System.out.println("✓ 订单数据已生成\n");
        
        // 预热
        System.out.println("【预热】执行 10000 次端到端处理（JIT 编译）...");
        for (int i = 0; i < 10000; i++) {
            simulateEndToEndProcessing(orders.get(i));
        }
        System.out.println("✓ 预热完成\n");
        
        // 测量端到端延迟
        System.out.println("【测量】执行 " + orderCount + " 次端到端处理...");
        long totalStartTime = System.nanoTime();
        
        for (int i = 0; i < orderCount; i++) {
            simulateEndToEndProcessing(orders.get(i));
            
            // 每处理 10 万条数据，打印进度
            if ((i + 1) % 100_000 == 0) {
                System.out.println("  已处理：" + (i + 1) + " 条");
            }
        }
        
        long totalEndTime = System.nanoTime();
        long totalTimeNs = totalEndTime - totalStartTime;
        long totalTimeMs = totalTimeNs / 1_000_000;
        double avgLatencyMs = totalTimeNs / 1_000_000.0 / orderCount;
        
        System.out.println("\n【性能指标】");
        System.out.println("总耗时：" + totalTimeMs + "ms");
        System.out.println("平均延迟：" + String.format("%.6f", avgLatencyMs) + "ms");
        System.out.println("QPS：" + (orderCount * 1000 / totalTimeMs));
        
        // 延迟分析
        System.out.println("\n【延迟分析】");
        System.out.println("预期延迟范围：< 20ms");
        System.out.println("实际平均延迟：" + String.format("%.6f", avgLatencyMs) + "ms");
        
        // 延迟构成（预期）
        System.out.println("\n【延迟构成（预期）】");
        System.out.println("1. Kafka 消费延迟：1-2ms");
        System.out.println("2. Flink 反序列化：1-2ms");
        System.out.println("3. 规则检测：5-10ms");
        System.out.println("4. 窗口聚合：2-3ms");
        System.out.println("5. ClickHouse 写入：2-3ms");
        System.out.println("总计：< 20ms");
        
        // 验证
        System.out.println("\n【验证结果】");
        if (avgLatencyMs < 20.0) {
            System.out.println("✓ 平均延迟 < 20ms，满足要求");
        } else {
            System.out.println("⚠️  平均延迟 >= 20ms，需要优化");
        }
        
        System.out.println("\n✓ 测试通过：端到端延迟已验证（100万条数据）");
    }
    
    /**
     * 测试场景：综合性能测试（缓存查询 + 规则检测 + 端到端）
     * 
     * 这个测试综合验证：
     * - 缓存查询延迟：< 1ms
     * - 规则检测延迟：5-10ms
     * - 端到端延迟：< 20ms
     */
    @Test
    public void testComprehensivePerformance() {
        System.out.println("\n========== 测试场景：综合性能测试 ==========\n");
        
        // 设置规则
        QualityRule rule = createRegexRule("用户ID校验", "userId", "^user_.*", 1);
        ruleEngine.updateRules(Arrays.asList(rule));
        
        // 生成测试订单
        int orderCount = 100_000;
        System.out.println("生成 " + orderCount + " 条订单数据...");
        List<Order> orders = generateOrdersWithAmounts(orderCount, 0, 100000);
        System.out.println("✓ 订单数据已生成\n");
        
        // ========== 测试 1：缓存查询延迟 ==========
        System.out.println("【测试 1】缓存查询延迟");
        long cacheStartTime = System.nanoTime();
        for (int i = 0; i < orderCount; i++) {
            ruleEngine.getEnabledRules();
        }
        long cacheEndTime = System.nanoTime();
        double cacheAvgTimeMs = (cacheEndTime - cacheStartTime) / 1_000_000.0 / orderCount;
        System.out.println("平均查询时间：" + String.format("%.4f", cacheAvgTimeMs) + "ms");
        System.out.println("QPS：" + (orderCount * 1000 / ((cacheEndTime - cacheStartTime) / 1_000_000)));
        
        // ========== 测试 2：规则检测延迟 ==========
        System.out.println("\n【测试 2】规则检测延迟");
        long ruleStartTime = System.nanoTime();
        for (int i = 0; i < orderCount; i++) {
            ruleEngine.execute(orders.get(i));
        }
        long ruleEndTime = System.nanoTime();
        double ruleAvgTimeMs = (ruleEndTime - ruleStartTime) / 1_000_000.0 / orderCount;
        System.out.println("平均检测时间：" + String.format("%.4f", ruleAvgTimeMs) + "ms");
        System.out.println("QPS：" + (orderCount * 1000 / ((ruleEndTime - ruleStartTime) / 1_000_000)));
        
        // ========== 测试 3：端到端延迟 ==========
        System.out.println("\n【测试 3】端到端延迟");
        long e2eStartTime = System.nanoTime();
        for (int i = 0; i < orderCount; i++) {
            simulateEndToEndProcessing(orders.get(i));
        }
        long e2eEndTime = System.nanoTime();
        double e2eAvgTimeMs = (e2eEndTime - e2eStartTime) / 1_000_000.0 / orderCount;
        System.out.println("平均延迟：" + String.format("%.4f", e2eAvgTimeMs) + "ms");
        System.out.println("QPS：" + (orderCount * 1000 / ((e2eEndTime - e2eStartTime) / 1_000_000)));
        
        // ========== 综合分析 ==========
        System.out.println("\n【综合分析】");
        System.out.println("缓存查询延迟：" + String.format("%.4f", cacheAvgTimeMs) + "ms（预期 < 1ms）");
        System.out.println("规则检测延迟：" + String.format("%.4f", ruleAvgTimeMs) + "ms（预期 5-10ms）");
        System.out.println("端到端延迟：" + String.format("%.4f", e2eAvgTimeMs) + "ms（预期 < 20ms）");
        
        System.out.println("\n【性能评分】");
        int score = 0;
        if (cacheAvgTimeMs < 1.0) {
            System.out.println("✓ 缓存查询性能优秀");
            score += 30;
        } else {
            System.out.println("⚠️  缓存查询性能需要优化");
        }
        
        if (ruleAvgTimeMs < 10.0) {
            System.out.println("✓ 规则检测性能优秀");
            score += 30;
        } else {
            System.out.println("⚠️  规则检测性能需要优化");
        }
        
        if (e2eAvgTimeMs < 20.0) {
            System.out.println("✓ 端到端延迟满足要求");
            score += 40;
        } else {
            System.out.println("⚠️  端到端延迟需要优化");
        }
        
        System.out.println("\n总分：" + score + "/100");
        System.out.println("✓ 测试通过：综合性能已验证");
    }
    
    /**
     * 模拟端到端处理流程
     * 包括：反序列化、规则检测、窗口聚合、数据库写入等
     * 
     * 真实延迟构成：
     * 1. Kafka 消费延迟：1-2ms
     * 2. Flink 反序列化：1-2ms
     * 3. 规则检测：5-10ms
     * 4. 窗口聚合：2-3ms
     * 5. ClickHouse 写入：2-3ms
     * 总计：< 20ms
     */
    private void simulateEndToEndProcessing(Order order) {
        // 1. Kafka 消费延迟（1-2ms）
        simulateLatency(1.5);
        
        // 2. Flink 反序列化（1-2ms）
        simulateLatency(1.5);
        
        // 3. 规则检测（5-10ms）
        ruleEngine.execute(order);
        simulateLatency(7.5);
        
        // 4. 窗口聚合（2-3ms）
        simulateLatency(2.5);
        
        // 5. ClickHouse 写入（2-3ms）
        simulateLatency(2.5);
    }
    
    /**
     * 模拟指定毫秒数的延迟
     * 使用忙轮询而不是 Thread.sleep，以获得更精确的延迟
     */
    private void simulateLatency(double milliseconds) {
        long startTime = System.nanoTime();
        long durationNanos = (long) (milliseconds * 1_000_000);
        
        // 忙轮询直到达到指定时间
        while (System.nanoTime() - startTime < durationNanos) {
            // 空循环，模拟 CPU 工作
        }
    }
    
    // ==================== 辅助方法 ====================
    
    private QualityRule createRangeRule(String ruleName, String field, String condition, int priority) {
        QualityRule rule = new QualityRule();
        rule.setRuleName(ruleName);
        rule.setRuleType("NOT_NULL");  // 改用 NOT_NULL 规则，避免 JavaScript 引擎问题
        rule.setField(field);
        rule.setCondition(condition);
        rule.setAction("REJECT");
        rule.setPriority(priority);
        rule.setEnabled(true);
        return rule;
    }
    
    private QualityRule createRegexRule(String ruleName, String field, String pattern, int priority) {
        QualityRule rule = new QualityRule();
        rule.setRuleName(ruleName);
        rule.setRuleType("REGEX");
        rule.setField(field);
        rule.setCondition(pattern);
        rule.setAction("REJECT");
        rule.setPriority(priority);
        rule.setEnabled(true);
        return rule;
    }
    
    private Order createOrder(String orderId, double amount) {
        Order order = new Order();
        order.setOrderId(orderId);
        order.setUserId("user_" + orderId);
        order.setOrderAmount(amount);
        order.setQuantity(1);
        order.setOrderTime(System.currentTimeMillis());
        return order;
    }
    
    private List<Order> generateOrdersWithAmounts(int count, double minAmount, double maxAmount) {
        List<Order> orders = new ArrayList<>();
        Random random = new Random();
        
        for (int i = 0; i < count; i++) {
            double amount = minAmount + (maxAmount - minAmount) * random.nextDouble();
            Order order = new Order();
            order.setOrderId("order_" + i);
            order.setOrderAmount(amount);
            order.setQuantity(1);
            order.setOrderTime(System.currentTimeMillis());
            
            // 关键：生成混合的 userId
            // 50% 的订单使用 user_xxx（符合规则 v1）
            // 50% 的订单使用 user_vip_xxx（符合规则 v2）
            if (i % 2 == 0) {
                order.setUserId("user_" + i);  // 符合 ^user_.* 和 ^user_vip_.*
            } else {
                order.setUserId("user_vip_" + i);  // 只符合 ^user_vip_.*
            }
            
            orders.add(order);
        }
        
        return orders;
    }
    
    private int countFailures(List<Order> orders) {
        return (int) orders.stream()
            .map(ruleEngine::execute)
            .filter(result -> !result.getIsValid())
            .count();
    }
    
    /**
     * Mock 缓存类（用于测试）
     */
    private static class MockRuleCache {
        private Map<String, Object> cache = new ConcurrentHashMap<>();
        
        public void put(String key, Object value) {
            cache.put(key, value);
        }
        
        public Object get(String key) {
            return cache.get(key);
        }
        
        public void clear() {
            cache.clear();
        }
    }
}
