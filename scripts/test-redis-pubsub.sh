#!/bin/bash

# Redis Pub/Sub 测试脚本
# 用于测试规则变更通知机制

REDIS_HOST="192.168.128.141"
REDIS_PORT="6379"
CHANNEL="rule_changes"

echo "=== Redis Pub/Sub 测试工具 ==="
echo "Redis 地址: $REDIS_HOST:$REDIS_PORT"
echo "频道名称: $CHANNEL"
echo ""

# 检查 redis-cli 是否安装
if ! command -v redis-cli &> /dev/null; then
    echo "错误: redis-cli 未安装"
    echo "请先安装 Redis 客户端工具"
    exit 1
fi

# 菜单
while true; do
    echo "=== 菜单 ==="
    echo "1. 订阅规则变更通知 (模拟 Flink 订阅)"
    echo "2. 发布规则变更通知 (模拟规则更新)"
    echo "3. 查看当前订阅者数量"
    echo "0. 退出"
    echo ""
    read -p "请选择: " choice
    
    case $choice in
        1)
            echo "开始订阅频道: $CHANNEL"
            echo "按 Ctrl+C 停止订阅"
            redis-cli -h $REDIS_HOST -p $REDIS_PORT SUBSCRIBE $CHANNEL
            ;;
        2)
            read -p "请输入通知消息 (例如: RULE_UPDATED:123): " message
            if [ -z "$message" ]; then
                message="RULE_UPDATED"
            fi
            
            subscribers=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT PUBLISH $CHANNEL "$message")
            echo "通知已发送: $message"
            echo "接收订阅者数量: $subscribers"
            ;;
        3)
            # 使用 PUBSUB NUMSUB 查看订阅者数量
            result=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT PUBSUB NUMSUB $CHANNEL)
            echo "频道订阅者数量: $result"
            ;;
        0)
            echo "退出程序"
            exit 0
            ;;
        *)
            echo "无效选项,请重新选择"
            ;;
    esac
    
    echo ""
done
