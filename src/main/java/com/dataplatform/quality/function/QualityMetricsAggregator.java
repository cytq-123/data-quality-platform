package com.dataplatform.quality.function;

import com.dataplatform.quality.model.QualityMetrics;
import com.dataplatform.quality.model.ValidationResult;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 质量指标聚合器 - Flink AggregateFunction
 * 
 * 功能: 聚合窗口内的质量指标
 */
public class QualityMetricsAggregator implements AggregateFunction<ValidationResult, QualityMetrics, QualityMetrics> {
    
    @Override
    public QualityMetrics createAccumulator() {
        QualityMetrics metrics = new QualityMetrics();
        metrics.setTotalCount(0L);
        metrics.setValidCount(0L);
        metrics.setInvalidCount(0L);
        metrics.setPassRate(0.0);
        return metrics;
    }
    
    @Override
    public QualityMetrics add(ValidationResult value, QualityMetrics accumulator) {
        // 累加计数
        accumulator.setTotalCount(accumulator.getTotalCount() + 1);
        
        if (value.getIsValid()) {
            accumulator.setValidCount(accumulator.getValidCount() + 1);
        } else {
            accumulator.setInvalidCount(accumulator.getInvalidCount() + 1);
        }
        
        // 记录时间范围
        if (accumulator.getWindowStart() == null || 
            value.getValidationTime() < accumulator.getWindowStart()) {
            accumulator.setWindowStart(value.getValidationTime());
        }
        
        if (accumulator.getWindowEnd() == null || 
            value.getValidationTime() > accumulator.getWindowEnd()) {
            accumulator.setWindowEnd(value.getValidationTime());
        }
        
        return accumulator;
    }
    
    @Override
    public QualityMetrics getResult(QualityMetrics accumulator) {
        // 计算通过率
        accumulator.calculatePassRate();
        
        // 设置检查时间为窗口结束时间
        accumulator.setCheckTime(accumulator.getWindowEnd());
        
        return accumulator;
    }
    
    @Override
    public QualityMetrics merge(QualityMetrics a, QualityMetrics b) {
        QualityMetrics merged = new QualityMetrics();
        
        merged.setTotalCount(a.getTotalCount() + b.getTotalCount());
        merged.setValidCount(a.getValidCount() + b.getValidCount());
        merged.setInvalidCount(a.getInvalidCount() + b.getInvalidCount());
        
        // 合并时间范围
        merged.setWindowStart(Math.min(
            a.getWindowStart() != null ? a.getWindowStart() : Long.MAX_VALUE,
            b.getWindowStart() != null ? b.getWindowStart() : Long.MAX_VALUE
        ));
        
        merged.setWindowEnd(Math.max(
            a.getWindowEnd() != null ? a.getWindowEnd() : Long.MIN_VALUE,
            b.getWindowEnd() != null ? b.getWindowEnd() : Long.MIN_VALUE
        ));
        
        // 计算通过率
        merged.calculatePassRate();
        merged.setCheckTime(merged.getWindowEnd());
        
        return merged;
    }
}
