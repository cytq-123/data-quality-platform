#!/bin/bash

# 启动所有服务

echo "=== 启动数据质量监控平台服务 ==="

# 1. 启动 Zookeeper
echo "启动 Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties

sleep 5

# 2. 启动 Kafka
echo "启动 Kafka..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

sleep 10

# 3. 创建 Kafka Topics
echo "创建 Kafka Topics..."
$KAFKA_HOME/bin/kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1 --if-not-exists
$KAFKA_HOME/bin/kafka-topics.sh --create --topic orders_valid --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1 --if-not-exists
$KAFKA_HOME/bin/kafka-topics.sh --create --topic orders_invalid --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1 --if-not-exists

# 4. 启动 Redis
echo "启动 Redis..."
redis-server --daemonize yes

# 5. 启动 MySQL (假设已安装)
echo "MySQL 应该已经在运行..."

# 6. 启动 ClickHouse (假设已安装)
echo "ClickHouse 应该已经在运行..."

# 7. 初始化数据库
echo "初始化 MySQL 数据库..."
mysql -u root -p < ../sql/mysql_init.sql

echo "初始化 ClickHouse 数据库..."
clickhouse-client < ../sql/clickhouse_init.sql

# 8. 启动 Prometheus
echo "启动 Prometheus..."
cd ../config
prometheus --config.file=prometheus.yml &

echo "=== 所有服务启动完成 ==="
echo "Kafka: localhost:9092"
echo "Redis: localhost:6379"
echo "MySQL: localhost:3306"
echo "ClickHouse: localhost:8123"
echo "Prometheus: localhost:9090"
