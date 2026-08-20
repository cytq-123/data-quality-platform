#!/bin/bash

# Kafka 单节点副本因子修复脚本
# 解决 "Replication factor: 3 larger than available brokers: 1" 错误

echo "=== Kafka 单节点配置修复 ==="

# 1. 停止 Kafka
echo "1. 停止 Kafka..."
pkill -f kafka.Kafka
sleep 3

# 2. 停止 Zookeeper
echo "2. 停止 Zookeeper..."
pkill -f zookeeper
sleep 3

# 3. 清理 Kafka 日志（可选，谨慎使用）
read -p "是否清理 Kafka 数据？这会删除所有主题和消息 (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "3. 清理 Kafka 数据..."
    rm -rf /data/kafka/kafka-logs/*
    rm -rf /data/kafka/zookeeper/*
else
    echo "3. 跳过数据清理"
fi

# 4. 更新配置文件
echo "4. 更新 Kafka 配置..."
cat > /data/kafka/config/server.properties << 'EOF'
broker.id=0
listeners=PLAINTEXT://192.168.128.141:9092
advertised.listeners=PLAINTEXT://192.168.128.141:9092
log.dirs=/data/kafka/kafka-logs
num.partitions=4
log.retention.hours=168
zookeeper.connect=localhost:2181

# 单节点配置 - 设置副本因子为1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
default.replication.factor=1
min.insync.replicas=1
EOF

echo "配置文件已更新"

# 5. 启动 Zookeeper
echo "5. 启动 Zookeeper..."
nohup /home/hsy/app/kafka/bin/zookeeper-server-start.sh \
  /data/kafka/config/zookeeper.properties > /data/kafka/zookeeper.log 2>&1 &

echo "等待 Zookeeper 启动..."
sleep 5

# 6. 启动 Kafka
echo "6. 启动 Kafka..."
nohup /home/hsy/app/kafka/bin/kafka-server-start.sh \
  /data/kafka/config/server.properties > /data/kafka/kafka.log 2>&1 &

echo "等待 Kafka 启动..."
sleep 10

# 7. 验证
echo "7. 验证 Kafka 状态..."
/home/hsy/app/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 192.168.128.141:9092

if [ $? -eq 0 ]; then
    echo "✓ Kafka 启动成功！"
else
    echo "✗ Kafka 启动失败，请检查日志："
    echo "  tail -f /data/kafka/kafka.log"
    exit 1
fi

# 8. 重新创建主题
echo ""
echo "8. 重新创建主题..."
echo "执行以下命令创建主题："
echo ""
echo "/home/hsy/app/kafka/bin/kafka-topics.sh --create \\"
echo "  --topic orders \\"
echo "  --bootstrap-server 192.168.128.141:9092 \\"
echo "  --partitions 4 \\"
echo "  --replication-factor 1"
echo ""
echo "/home/hsy/app/kafka/bin/kafka-topics.sh --create \\"
echo "  --topic orders_valid \\"
echo "  --bootstrap-server 192.168.128.141:9092 \\"
echo "  --partitions 4 \\"
echo "  --replication-factor 1"
echo ""
echo "/home/hsy/app/kafka/bin/kafka-topics.sh --create \\"
echo "  --topic orders_invalid \\"
echo "  --bootstrap-server 192.168.128.141:9092 \\"
echo "  --partitions 4 \\"
echo "  --replication-factor 1"
echo ""
echo "=== 修复完成 ==="
