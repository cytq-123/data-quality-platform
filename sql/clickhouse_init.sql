-- 创建数据库
CREATE DATABASE IF NOT EXISTS data_quality;

USE data_quality;

-- 质量指标表
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    rule_name String COMMENT '规则名称',
    check_time DateTime COMMENT '检查时间',
    total_count UInt64 COMMENT '总记录数',
    valid_count UInt64 COMMENT '有效记录数',
    invalid_count UInt64 COMMENT '无效记录数',
    pass_rate Float64 COMMENT '通过率',
    
    -- 跳数索引：按 rule_name 分组，粒度为 1
    INDEX idx_rule_name rule_name TYPE set(1) GRANULARITY 1,
    
    -- 跳数索引：按 pass_rate 分组，粒度为 1
    INDEX idx_pass_rate pass_rate TYPE minmax GRANULARITY 1,
    
    -- 跳数索引：按 check_time 分组，粒度为 1
    INDEX idx_check_time check_time TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(check_time)
ORDER BY (rule_name, check_time)
SETTINGS index_granularity = 8192
COMMENT '数据质量指标表';

-- 物化视图: 每小时质量趋势
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_quality_hourly
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(hour)
ORDER BY (rule_name, hour)
AS SELECT
    rule_name,
    toStartOfHour(check_time) as hour,
    sum(total_count) as total_count,
    sum(valid_count) as valid_count,
    sum(invalid_count) as invalid_count,
    avg(pass_rate) as avg_pass_rate
FROM data_quality_metrics
GROUP BY rule_name, hour;

-- 物化视图: 每天质量趋势
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_quality_daily
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (rule_name, day)
AS SELECT
    rule_name,
    toDate(check_time) as day,
    sum(total_count) as total_count,
    sum(valid_count) as valid_count,
    sum(invalid_count) as invalid_count,
    avg(pass_rate) as avg_pass_rate
FROM data_quality_metrics
GROUP BY rule_name, day;

-- 查询示例

-- 1. 查询最近1小时的质量指标
-- SELECT 
--     rule_name,
--     check_time,
--     total_count,
--     valid_count,
--     invalid_count,
--     pass_rate * 100 as pass_rate_percent
-- FROM data_quality_metrics
-- WHERE check_time >= now() - INTERVAL 1 HOUR
-- ORDER BY check_time DESC;

-- 2. 查询每小时质量趋势
-- SELECT 
--     rule_name,
--     hour,
--     total_count,
--     valid_count,
--     invalid_count,
--     avg_pass_rate * 100 as avg_pass_rate_percent
-- FROM mv_quality_hourly
-- WHERE hour >= today() - INTERVAL 1 DAY
-- ORDER BY hour DESC;

-- 3. 查询每天质量趋势
-- SELECT 
--     rule_name,
--     day,
--     total_count,
--     valid_count,
--     invalid_count,
--     avg_pass_rate * 100 as avg_pass_rate_percent
-- FROM mv_quality_daily
-- WHERE day >= today() - INTERVAL 30 DAY
-- ORDER BY day DESC;

-- 4. 查询质量通过率低于95%的时段
-- SELECT 
--     rule_name,
--     check_time,
--     pass_rate * 100 as pass_rate_percent
-- FROM data_quality_metrics
-- WHERE pass_rate < 0.95
-- ORDER BY check_time DESC
-- LIMIT 100;
