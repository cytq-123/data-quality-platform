package com.dataplatform.quality.rule;

import com.dataplatform.quality.model.Order;
import com.dataplatform.quality.model.QualityRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.lang.reflect.Field;
import java.util.regex.Pattern;

/**
 * 规则验证器 - 核心校验逻辑
 */
public class RuleValidator {
    private static final Logger LOG = LoggerFactory.getLogger(RuleValidator.class);
    private static final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
    
    /**
     * 验证订单数据是否符合规则
     * 
     * @param order 订单数据
     * @param rule 质量规则
     * @return 是否通过验证
     */
    public static boolean validate(Order order, QualityRule rule) {
        if (order == null || rule == null || !rule.getEnabled()) {
            if (rule == null) {
                LOG.warn("Rule is null, skipping validation");
            } else if (!rule.getEnabled()) {
                LOG.debug("Rule '{}' is disabled, skipping validation", rule.getRuleName());
            }
            return true;
        }
        
        LOG.debug("Validating order {} with rule: {} (type: {}, field: {})", 
            order.getOrderId(), rule.getRuleName(), rule.getRuleType(), rule.getField());
        
        try {
            switch (rule.getRuleType().toUpperCase()) {
                case "RANGE":
                    return validateRange(order, rule);
                case "NOT_NULL":
                    return validateNotNull(order, rule);
                case "REGEX":
                    return validateRegex(order, rule);
                case "CUSTOM":
                    return validateCustom(order, rule);
                default:
                    LOG.warn("Unknown rule type: {}", rule.getRuleType());
                    return true;
            }
        } catch (Exception e) {
            LOG.error("Error validating rule: {}, error: {}", rule.getRuleName(), e.getMessage(), e);
            return true; // 验证失败时默认通过,避免影响业务
        }
    }
    
    /**
     * 范围校验
     * 示例: value >= 0 AND value <= 100000
     */
    private static boolean validateRange(Order order, QualityRule rule) throws Exception {
        Object fieldValue = getFieldValue(order, rule.getField());
        if (fieldValue == null) {
            LOG.debug("Range validation failed: field '{}' is null for order {}", 
                rule.getField(), order.getOrderId());
            return false;
        }
        
        // 将字段值转换为数值
        double value;
        if (fieldValue instanceof Number) {
            value = ((Number) fieldValue).doubleValue();
        } else {
            value = Double.parseDouble(fieldValue.toString());
        }
        
        // 解析条件并验证
        // 替换 AND/OR 为 JavaScript 语法
        String originalCondition = rule.getCondition();
        String condition = originalCondition
            .replace("value", String.valueOf(value))
            .replace(" AND ", " && ")
            .replace(" OR ", " || ");
        
        LOG.debug("Range validation: field='{}', value={}, originalCondition='{}', jsCondition='{}'", 
            rule.getField(), value, originalCondition, condition);
        
        boolean isValid = evaluateExpression(condition);
        
        LOG.debug("Range validation result: field='{}', value={}, isValid={}", 
            rule.getField(), value, isValid);
        
        return isValid;
    }
    
    /**
     * 非空校验
     */
    private static boolean validateNotNull(Order order, QualityRule rule) throws Exception {
        Object fieldValue = getFieldValue(order, rule.getField());
        
        if (fieldValue == null) {
            return false;
        }
        
        if (fieldValue instanceof String) {
            return !((String) fieldValue).trim().isEmpty();
        }
        
        return true;
    }
    
    /**
     * 正则校验
     * 示例: ^1[3-9]\d{9}$ (手机号)
     */
    private static boolean validateRegex(Order order, QualityRule rule) throws Exception {
        Object fieldValue = getFieldValue(order, rule.getField());
        if (fieldValue == null) {
            return false;
        }
        
        String value = fieldValue.toString();
        Pattern pattern = Pattern.compile(rule.getCondition());
        return pattern.matcher(value).matches();
    }
    
    /**
     * 自定义校验
     * 示例: order_time <= current_time AND order_time >= current_time - 7*24*3600*1000
     */
    private static boolean validateCustom(Order order, QualityRule rule) throws Exception {
        String condition = rule.getCondition();
        
        // 替换字段名为实际值
        for (Field field : Order.class.getDeclaredFields()) {
            field.setAccessible(true);
            String fieldName = field.getName();
            Object fieldValue = field.get(order);
            
            if (fieldValue != null) {
                String valueStr = fieldValue instanceof String ? 
                    "\"" + fieldValue + "\"" : fieldValue.toString();
                condition = condition.replaceAll("\\b" + fieldName + "\\b", valueStr);
            }
        }
        
        // 替换特殊变量
        condition = condition.replace("current_time", String.valueOf(System.currentTimeMillis()));
        
        // 替换 AND/OR 为 JavaScript 语法
        condition = condition.replace(" AND ", " && ").replace(" OR ", " || ");
        
        LOG.debug("Custom validation: originalCondition='{}', jsCondition='{}'", 
            rule.getCondition(), condition);
        
        return evaluateExpression(condition);
    }
    
    /**
     * 获取字段值 (支持反射)
     */
    private static Object getFieldValue(Order order, String fieldName) throws Exception {
        Field field = Order.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(order);
    }
    
    /**
     * 执行表达式计算 (使用 JavaScript 引擎)
     */
    private static boolean evaluateExpression(String expression) {
        try {
            ScriptEngine engine = scriptEngineManager.getEngineByName("JavaScript");
            
            if (engine == null) {
                // Fallback: 尝试使用 "nashorn"
                engine = scriptEngineManager.getEngineByName("nashorn");
            }
            
            if (engine == null) {
                LOG.error("JavaScript engine not available! Expression: {}", expression);
                // 作为降级方案，如果引擎不可用，默认通过验证（避免误杀）
                return true;
            }
            
            Object result = engine.eval(expression);
            boolean isValid = Boolean.TRUE.equals(result);
            
            LOG.debug("Expression: {}, Result: {}, Valid: {}", expression, result, isValid);
            
            return isValid;
        } catch (ScriptException e) {
            LOG.error("Error evaluating expression: {}, error: {}", expression, e.getMessage());
            return false;
        } catch (Exception e) {
            LOG.error("Unexpected error evaluating expression: {}", expression, e);
            return false;
        }
    }
    
    /**
     * 获取验证失败原因
     */
    public static String getFailureReason(Order order, QualityRule rule) {
        try {
            Object fieldValue = getFieldValue(order, rule.getField());
            return String.format("Rule '%s' failed: field '%s' = %s, condition: %s",
                rule.getRuleName(), rule.getField(), fieldValue, rule.getCondition());
        } catch (Exception e) {
            return String.format("Rule '%s' failed: %s", rule.getRuleName(), e.getMessage());
        }
    }
}
