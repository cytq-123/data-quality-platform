-- 创建数据库
CREATE DATABASE IF NOT EXISTS data_quality DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE data_quality;

-- 质量规则表
CREATE TABLE IF NOT EXISTS quality_rules (
    rule_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '规则ID',
    rule_name VARCHAR(100) NOT NULL COMMENT '规则名称',
    rule_type VARCHAR(50) NOT NULL COMMENT '规则类型: RANGE, NOT_NULL, REGEX, CUSTOM',
    field VARCHAR(100) NOT NULL COMMENT '校验字段',
    `condition` TEXT NOT NULL COMMENT '校验条件',
    action VARCHAR(20) NOT NULL DEFAULT 'REJECT' COMMENT '处理动作: REJECT, ALERT, PASS',
    priority INT NOT NULL DEFAULT 100 COMMENT '优先级 (数字越小优先级越高)',
    enabled BOOLEAN NOT NULL DEFAULT TRUE COMMENT '是否启用',
    description VARCHAR(500) COMMENT '规则描述',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_enabled (enabled),
    INDEX idx_priority (priority)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据质量规则表';

-- 插入示例规则
INSERT INTO quality_rules (rule_name, rule_type, field, `condition`, action, priority, enabled, description) VALUES
('订单金额范围校验', 'RANGE', 'orderAmount', 'value >= 0 AND value <= 100000', 'REJECT', 1, TRUE, '订单金额必须在0-100000之间'),
('用户ID非空校验', 'NOT_NULL', 'userId', '', 'REJECT', 2, TRUE, '用户ID不能为空'),
('订单ID非空校验', 'NOT_NULL', 'orderId', '', 'REJECT', 3, TRUE, '订单ID不能为空'),
('手机号格式校验', 'REGEX', 'phone', '^1[3-9]\\d{9}$', 'ALERT', 4, TRUE, '手机号必须是11位数字,以1开头'),
('商品数量范围校验', 'RANGE', 'quantity', 'value >= 1 AND value <= 999', 'REJECT', 5, TRUE, '商品数量必须在1-999之间'),
('订单时间合理性校验', 'CUSTOM', '', 'orderTime <= current_time AND orderTime >= current_time - 7*24*3600*1000', 'ALERT', 6, TRUE, '订单时间不能晚于当前时间,且不能早于7天前');

-- 规则执行日志表 (可选)
CREATE TABLE IF NOT EXISTS rule_execution_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '日志ID',
    order_id VARCHAR(100) COMMENT '订单ID',
    rule_id BIGINT COMMENT '规则ID',
    rule_name VARCHAR(100) COMMENT '规则名称',
    is_valid BOOLEAN COMMENT '是否通过',
    failure_reason TEXT COMMENT '失败原因',
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '执行时间',
    INDEX idx_order_id (order_id),
    INDEX idx_rule_id (rule_id),
    INDEX idx_execution_time (execution_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='规则执行日志表';
