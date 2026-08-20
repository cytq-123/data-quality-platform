package com.dataplatform.quality.util;

import com.dataplatform.quality.cache.RedisRulePublisher;
import com.dataplatform.quality.model.QualityRule;
import com.dataplatform.quality.rule.RuleLoader;

import java.util.List;
import java.util.Scanner;

/**
 * 规则管理命令行工具
 */
public class RuleManagerCLI {
    
    private static RuleLoader ruleLoader;
    private static RedisRulePublisher rulePublisher;
    private static Scanner scanner;
    
    public static void main(String[] args) {
        // 初始化
        ruleLoader = new RuleLoader(
            "jdbc:mysql://localhost:3306/data_quality?useSSL=false&serverTimezone=UTC",
            "root",
            "your_password"
        );
        rulePublisher = new RedisRulePublisher("localhost", 6379);
        scanner = new Scanner(System.in);
        
        System.out.println("=== 数据质量规则管理工具 ===");
        
        try {
            while (true) {
                printMenu();
                String choice = scanner.nextLine().trim();
                
                switch (choice) {
                    case "1":
                        listAllRules();
                        break;
                    case "2":
                        addRule();
                        break;
                    case "3":
                        updateRule();
                        break;
                    case "4":
                        deleteRule();
                        break;
                    case "5":
                        viewRuleDetails();
                        break;
                    case "0":
                        System.out.println("退出程序");
                        return;
                    default:
                        System.out.println("无效选项,请重新选择");
                }
            }
        } finally {
            // 清理资源
            if (rulePublisher != null) {
                rulePublisher.close();
            }
        }
    }
    
    private static void printMenu() {
        System.out.println("\n=== 菜单 ===");
        System.out.println("1. 查看所有规则");
        System.out.println("2. 添加规则");
        System.out.println("3. 更新规则");
        System.out.println("4. 删除规则");
        System.out.println("5. 查看规则详情");
        System.out.println("0. 退出");
        System.out.print("请选择: ");
    }
    
    private static void listAllRules() {
        List<QualityRule> rules = ruleLoader.loadAllRules();
        
        System.out.println("\n=== 所有规则 ===");
        System.out.printf("%-5s %-30s %-15s %-20s %-10s %-10s%n", 
            "ID", "规则名称", "规则类型", "字段", "动作", "启用");
        System.out.println("-".repeat(100));
        
        for (QualityRule rule : rules) {
            System.out.printf("%-5d %-30s %-15s %-20s %-10s %-10s%n",
                rule.getRuleId(),
                rule.getRuleName(),
                rule.getRuleType(),
                rule.getField(),
                rule.getAction(),
                rule.getEnabled() ? "是" : "否"
            );
        }
    }
    
    private static void addRule() {
        System.out.println("\n=== 添加规则 ===");
        
        QualityRule rule = new QualityRule();
        
        System.out.print("规则名称: ");
        rule.setRuleName(scanner.nextLine().trim());
        
        System.out.print("规则类型 (RANGE/NOT_NULL/REGEX/CUSTOM): ");
        rule.setRuleType(scanner.nextLine().trim().toUpperCase());
        
        System.out.print("校验字段: ");
        rule.setField(scanner.nextLine().trim());
        
        System.out.print("校验条件: ");
        rule.setCondition(scanner.nextLine().trim());
        
        System.out.print("处理动作 (REJECT/ALERT/PASS): ");
        rule.setAction(scanner.nextLine().trim().toUpperCase());
        
        System.out.print("优先级 (数字越小优先级越高): ");
        rule.setPriority(Integer.parseInt(scanner.nextLine().trim()));
        
        System.out.print("是否启用 (true/false): ");
        rule.setEnabled(Boolean.parseBoolean(scanner.nextLine().trim()));
        
        System.out.print("规则描述: ");
        rule.setDescription(scanner.nextLine().trim());
        
        if (ruleLoader.saveRule(rule)) {
            System.out.println("规则添加成功! ID: " + rule.getRuleId());
            // 发布规则变更通知
            rulePublisher.publishRuleChange("RULE_ADDED:" + rule.getRuleId());
        } else {
            System.out.println("规则添加失败!");
        }
    }
    
    private static void updateRule() {
        System.out.println("\n=== 更新规则 ===");
        
        System.out.print("请输入规则ID: ");
        Long ruleId = Long.parseLong(scanner.nextLine().trim());
        
        QualityRule rule = ruleLoader.loadRuleById(ruleId);
        if (rule == null) {
            System.out.println("规则不存在!");
            return;
        }
        
        System.out.println("当前规则: " + rule.getRuleName());
        System.out.println("留空表示不修改");
        
        System.out.print("规则名称 [" + rule.getRuleName() + "]: ");
        String input = scanner.nextLine().trim();
        if (!input.isEmpty()) rule.setRuleName(input);
        
        System.out.print("是否启用 (true/false) [" + rule.getEnabled() + "]: ");
        input = scanner.nextLine().trim();
        if (!input.isEmpty()) rule.setEnabled(Boolean.parseBoolean(input));
        
        if (ruleLoader.updateRule(rule)) {
            System.out.println("规则更新成功!");
            // 发布规则变更通知
            rulePublisher.publishRuleChange("RULE_UPDATED:" + ruleId);
        } else {
            System.out.println("规则更新失败!");
        }
    }
    
    private static void deleteRule() {
        System.out.println("\n=== 删除规则 ===");
        
        System.out.print("请输入规则ID: ");
        Long ruleId = Long.parseLong(scanner.nextLine().trim());
        
        System.out.print("确认删除? (yes/no): ");
        String confirm = scanner.nextLine().trim();
        
        if ("yes".equalsIgnoreCase(confirm)) {
            if (ruleLoader.deleteRule(ruleId)) {
                System.out.println("规则删除成功!");
                // 发布规则变更通知
                rulePublisher.publishRuleChange("RULE_DELETED:" + ruleId);
            } else {
                System.out.println("规则删除失败!");
            }
        } else {
            System.out.println("取消删除");
        }
    }
    
    private static void viewRuleDetails() {
        System.out.println("\n=== 查看规则详情 ===");
        
        System.out.print("请输入规则ID: ");
        Long ruleId = Long.parseLong(scanner.nextLine().trim());
        
        QualityRule rule = ruleLoader.loadRuleById(ruleId);
        if (rule == null) {
            System.out.println("规则不存在!");
            return;
        }
        
        System.out.println("\n规则详情:");
        System.out.println("ID: " + rule.getRuleId());
        System.out.println("名称: " + rule.getRuleName());
        System.out.println("类型: " + rule.getRuleType());
        System.out.println("字段: " + rule.getField());
        System.out.println("条件: " + rule.getCondition());
        System.out.println("动作: " + rule.getAction());
        System.out.println("优先级: " + rule.getPriority());
        System.out.println("启用: " + rule.getEnabled());
        System.out.println("描述: " + rule.getDescription());
    }
}
