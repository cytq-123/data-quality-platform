package com.dataplatform.quality.test;

/**
 * 恶意刷单检测 ROI 计算器
 * 
 * 基于项目真实数据：
 * - DataGenerator.java: 60 异常单/分钟
 * - 性能测试: 延迟 350ms → 210ms
 */
public class FraudROICalculator {
    
    // ========== 项目真实参数 ==========
    
    /** 异常订单生成速率（来自 DataGenerator.java）*/
    private static final int FRAUD_ORDERS_PER_MINUTE = 60;  // 每秒100单 × 1%异常率 = 60单/分钟
    
    /** 单笔订单金额（合理假设）*/
    private static final double ORDER_AMOUNT = 100.0;  // 元
    
    /** 优化前延迟（来自性能测试）*/
    private static final double LATENCY_BEFORE_MS = 350.0;  // 毫秒
    
    /** 优化后延迟（来自性能测试）*/
    private static final double LATENCY_AFTER_MS = 210.0;  // 毫秒
    
    public static void main(String[] args) {
        System.out.println("===============================================");
        System.out.println("  抖音电商恶意刷单检测 - ROI 分析");
        System.out.println("  基于项目真实数据");
        System.out.println("===============================================\n");
        
        // 第一部分：基础数据
        printBasicData();
        
        // 第二部分：延迟窗口内刷单量计算
        printFraudCalculation();
        
        // 第三部分：业务价值计算
        printBusinessValue();
        
        // 第四部分：ROI 分析
        printROIAnalysis();
        
        System.out.println("\n===============================================");
        System.out.println("  结论");
        System.out.println("===============================================");
        System.out.println("通过三层缓存优化，将延迟从 350ms 降到 210ms");
        System.out.println("提前了 140ms 拦截恶意订单");
        System.out.println("在抖音规模下，年节省 511 万元，ROI 50倍");
        System.out.println("===============================================\n");
    }
    
    /**
     * 打印基础数据
     */
    private static void printBasicData() {
        System.out.println("【1. 项目真实数据】");
        System.out.println("数据来源：DataGenerator.java");
        System.out.println("  - 订单发送速率: 100 单/秒");
        System.out.println("  - 异常订单比例: 1% (每100单中1单异常)");
        System.out.printf("  - 异常订单速率: %d 单/分钟 (相当于羊毛党刷单速度)\n", FRAUD_ORDERS_PER_MINUTE);
        System.out.printf("  - 单笔订单金额: %.0f 元\n", ORDER_AMOUNT);
        System.out.println();
        
        System.out.println("性能测试数据：");
        System.out.printf("  - 优化前延迟(P95): %.0f ms\n", LATENCY_BEFORE_MS);
        System.out.printf("  - 优化后延迟(P95): %.0f ms\n", LATENCY_AFTER_MS);
        System.out.printf("  - 延迟降低: %.0f ms (%.1f%%)\n", 
                         LATENCY_BEFORE_MS - LATENCY_AFTER_MS,
                         (LATENCY_BEFORE_MS - LATENCY_AFTER_MS) / LATENCY_BEFORE_MS * 100);
        System.out.println();
    }
    
    /**
     * 打印刷单量计算
     */
    private static void printFraudCalculation() {
        System.out.println("【2. 延迟窗口内刷单量计算】");
        System.out.println("公式：刷单量 = 刷单速度 × (延迟时间 / 60,000 ms)");
        System.out.println();
        
        // 优化前
        double fraudOrdersBefore = calculateFraudOrders(LATENCY_BEFORE_MS);
        System.out.printf("优化前 (延迟 %.0f ms):\n", LATENCY_BEFORE_MS);
        System.out.printf("  刷单量 = %d × (%.0f / 60,000)\n", 
                         FRAUD_ORDERS_PER_MINUTE, LATENCY_BEFORE_MS);
        System.out.printf("         = %.2f 单\n", fraudOrdersBefore);
        System.out.printf("  → 羊毛党能在被检测前完成第1单的 %.0f%%\n", fraudOrdersBefore * 100);
        System.out.println();
        
        // 优化后
        double fraudOrdersAfter = calculateFraudOrders(LATENCY_AFTER_MS);
        System.out.printf("优化后 (延迟 %.0f ms):\n", LATENCY_AFTER_MS);
        System.out.printf("  刷单量 = %d × (%.0f / 60,000)\n", 
                         FRAUD_ORDERS_PER_MINUTE, LATENCY_AFTER_MS);
        System.out.printf("         = %.2f 单\n", fraudOrdersAfter);
        System.out.printf("  → 羊毛党能在被检测前完成第1单的 %.0f%%\n", fraudOrdersAfter * 100);
        System.out.println();
        
        // 对比
        double improvement = (fraudOrdersBefore - fraudOrdersAfter) / fraudOrdersBefore * 100;
        System.out.println("对比：");
        System.out.printf("  - 刷单量减少: %.2f 单 (%.1f%%)\n", 
                         fraudOrdersBefore - fraudOrdersAfter, improvement);
        System.out.printf("  - 拦截提前: %.0f%% → %.0f%% (提前 %.0f%%)\n",
                         fraudOrdersBefore * 100, fraudOrdersAfter * 100,
                         (fraudOrdersBefore - fraudOrdersAfter) * 100);
        System.out.println();
    }
    
    /**
     * 打印业务价值
     */
    private static void printBusinessValue() {
        System.out.println("【3. 业务价值计算】");
        
        double fraudOrdersBefore = calculateFraudOrders(LATENCY_BEFORE_MS);
        double fraudOrdersAfter = calculateFraudOrders(LATENCY_AFTER_MS);
        
        // 单次攻击损失
        double lossBefore = fraudOrdersBefore * ORDER_AMOUNT;
        double lossAfter = fraudOrdersAfter * ORDER_AMOUNT;
        double savingsPerAttack = lossBefore - lossAfter;
        
        System.out.println("单次攻击对比：");
        System.out.printf("  - 优化前损失: %.2f 单 × %.0f 元 = %.2f 元\n", 
                         fraudOrdersBefore, ORDER_AMOUNT, lossBefore);
        System.out.printf("  - 优化后损失: %.2f 单 × %.0f 元 = %.2f 元\n", 
                         fraudOrdersAfter, ORDER_AMOUNT, lossAfter);
        System.out.printf("  - 单次节省: %.2f 元\n", savingsPerAttack);
        System.out.println();
        
        // 小规模场景（10次/天）
        int attacksPerDaySmall = 10;
        double yearSavingsSmall = savingsPerAttack * attacksPerDaySmall * 365;
        System.out.printf("小规模场景（%d 次攻击/天）：\n", attacksPerDaySmall);
        System.out.printf("  - 年节省: %.2f 元 ≈ %.1f 万元\n", 
                         yearSavingsSmall, yearSavingsSmall / 10000);
        System.out.println();
        
        // 大规模场景（1000次/天，抖音级别）
        int attacksPerDayLarge = 1000;
        double yearSavingsLarge = savingsPerAttack * attacksPerDayLarge * 365;
        System.out.printf("大规模场景（%d 次攻击/天，抖音级别）：\n", attacksPerDayLarge);
        System.out.printf("  - 每天节省: %.2f 元 = %.2f 万元\n", 
                         savingsPerAttack * attacksPerDayLarge,
                         savingsPerAttack * attacksPerDayLarge / 10000);
        System.out.printf("  - 年节省: %.2f 元 ≈ %.0f 万元\n", 
                         yearSavingsLarge, yearSavingsLarge / 10000);
        System.out.println();
    }
    
    /**
     * 打印 ROI 分析
     */
    private static void printROIAnalysis() {
        System.out.println("【4. ROI 分析】");
        
        double fraudOrdersBefore = calculateFraudOrders(LATENCY_BEFORE_MS);
        double fraudOrdersAfter = calculateFraudOrders(LATENCY_AFTER_MS);
        double savingsPerAttack = (fraudOrdersBefore - fraudOrdersAfter) * ORDER_AMOUNT;
        
        // 系统建设成本
        double developmentCost = 60000;  // 开发成本（2人月）
        double serverCost = 20000;       // 服务器成本（1年）
        double maintenanceCost = 20000;  // 测试与维护
        double totalCost = developmentCost + serverCost + maintenanceCost;
        
        System.out.println("系统建设成本：");
        System.out.printf("  - 开发人力（2人月）: %.0f 元\n", developmentCost);
        System.out.printf("  - 服务器成本（1年）: %.0f 元\n", serverCost);
        System.out.printf("  - 测试与维护: %.0f 元\n", maintenanceCost);
        System.out.printf("  - 总成本: %.0f 元 = %.0f 万元\n", totalCost, totalCost / 10000);
        System.out.println();
        
        // 小规模 ROI
        int attacksPerDaySmall = 10;
        double yearSavingsSmall = savingsPerAttack * attacksPerDaySmall * 365;
        double roiSmall = (yearSavingsSmall - totalCost) / totalCost * 100;
        double paybackSmall = totalCost / yearSavingsSmall;
        
        System.out.printf("小规模场景（%d 次攻击/天）：\n", attacksPerDaySmall);
        System.out.printf("  - 年节省: %.0f 万元\n", yearSavingsSmall / 10000);
        System.out.printf("  - 总成本: %.0f 万元\n", totalCost / 10000);
        System.out.printf("  - ROI: %.1f%%", roiSmall);
        if (roiSmall < 0) {
            System.out.println(" ❌ 不划算");
            System.out.printf("  - 回本周期: %.1f 年\n", paybackSmall);
        } else {
            System.out.println(" ✅ 划算");
            System.out.printf("  - 回本周期: %.1f 年\n", paybackSmall);
        }
        System.out.println();
        
        // 大规模 ROI
        int attacksPerDayLarge = 1000;
        double yearSavingsLarge = savingsPerAttack * attacksPerDayLarge * 365;
        double roiLarge = (yearSavingsLarge - totalCost) / totalCost * 100;
        double paybackLarge = totalCost / yearSavingsLarge;
        
        System.out.printf("大规模场景（%d 次攻击/天，抖音级别）：\n", attacksPerDayLarge);
        System.out.printf("  - 年节省: %.0f 万元\n", yearSavingsLarge / 10000);
        System.out.printf("  - 总成本: %.0f 万元\n", totalCost / 10000);
        System.out.printf("  - ROI: %.0f%% (%.0f 倍) ✅ 非常划算\n", 
                         roiLarge, (roiLarge / 100 + 1));
        System.out.printf("  - 回本周期: %.2f 年 ≈ %.0f 天\n", 
                         paybackLarge, paybackLarge * 365);
        System.out.println();
    }
    
    /**
     * 计算延迟窗口内的刷单量
     * 
     * @param latencyMs 延迟时间（毫秒）
     * @return 刷单量（单）
     */
    private static double calculateFraudOrders(double latencyMs) {
        return FRAUD_ORDERS_PER_MINUTE * (latencyMs / 60000.0);
    }
}
