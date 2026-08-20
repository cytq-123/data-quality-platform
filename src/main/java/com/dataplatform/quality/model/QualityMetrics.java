package com.dataplatform.quality.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 数据质量指标模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QualityMetrics implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /** 规则名称 */
    private String ruleName;
    
    /** 检查时间 */
    private Long checkTime;
    
    /** 总记录数 */
    private Long totalCount;
    
    /** 有效记录数 */
    private Long validCount;
    
    /** 无效记录数 */
    private Long invalidCount;
    
    /** 通过率 */
    private Double passRate;
    
    /** 窗口开始时间 */
    private Long windowStart;
    
    /** 窗口结束时间 */
    private Long windowEnd;
    
    /**
     * 计算通过率
     */
    public void calculatePassRate() {
        if (totalCount != null && totalCount > 0) {
            this.passRate = (double) validCount / totalCount;
        } else {
            this.passRate = 0.0;
        }
    }
    
    /**
     * 转换为 ClickHouse 插入语句
     */
    public String toClickHouseInsert() {
        return String.format(
            "INSERT INTO data_quality_metrics (rule_name, check_time, total_count, valid_count, invalid_count, pass_rate) " +
            "VALUES ('%s', toDateTime(%d), %d, %d, %d, %.4f)",
            ruleName, checkTime / 1000, totalCount, validCount, invalidCount, passRate
        );
    }
    
    /**
     * 从 JSON 字符串创建指标对象
     */
    public static QualityMetrics fromJson(String json) {
        return com.alibaba.fastjson2.JSON.parseObject(json, QualityMetrics.class);
    }
    
    /**
     * 转换为 JSON 字符串
     */
    public String toJson() {
        return com.alibaba.fastjson2.JSON.toJSONString(this);
    }
}
