# CentOS 7/8 完整环境搭建指南

> 适用于全新的 CentOS 虚拟机，一步步安装所有依赖

---

## 📋 目录

1. [系统准备](#1-系统准备)
2. [安装 JDK 11](#2-安装-jdk-11)
3. [安装 Maven](#3-安装-maven)
4. [安装 Kafka](#4-安装-kafka)
5. [安装 Redis](#5-安装-redis)
6. [安装 MySQL 8](#6-安装-mysql-8)
7. [安装 ClickHouse](#7-安装-clickhouse)
8. [安装 Flink](#8-安装-flink)
9. [安装 Prometheus](#9-安装-prometheus)
10. [验证安装](#10-验证安装)

---

## 1. 系统准备

### 1.1 更新系统
```bash
# 更新 yum 源
sudo yum update -y

# 安装基础工具
sudo yum install -y wget curl vim net-tools
```

### 1.2 关闭防火墙（开发环境）
```bash
# 关闭防火墙
sudo systemctl stop firewalld
sudo systemctl disable firewalld

# 或者开放端口（生产环境推荐）
sudo firewall-cmd --permanent --add-port=9092/tcp  # Kafka
sudo firewall-cmd --permanent --add-port=6379/tcp  # Redis
sudo firewall-cmd --permanent --add-port=3306/tcp  # MySQL
sudo firewall-cmd --permanent --add-port=8123/tcp  # ClickHouse
sudo firewall-cmd --permanent --add-port=8081/tcp  # Flink
sudo firewall-cmd --permanent --add-port=9090/tcp  # Prometheus
sudo firewall-cmd --reload
```

---

## 2. 安装 JDK 11

### 下载链接
```
Oracle JDK 11: https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html
OpenJDK 11: https://jdk.java.net/archive/
```

### 安装步骤
```bash
# 方式1: 使用 yum 安装 OpenJDK（推荐）
sudo yum install -y java-11-openjdk java-11-openjdk-devel

# 验证安装
java -version

# 配置环境变量
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

### 方式2: 手动安装
```bash
# 下载 JDK
cd /opt
sudo wget https://download.oracle.com/java/11/archive/jdk-11.0.20_linux-x64_bin.tar.gz

# 解压
sudo tar -xzf jdk-11.0.20_linux-x64_bin.tar.gz
sudo mv jdk-11.0.20 /usr/local/java11

# 配置环境变量
echo 'export JAVA_HOME=/usr/local/java11' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

---

## 3. 安装 Maven

### 下载链接
```
Maven 3.9.5: https://dlcdn.apache.org/maven/maven-3/3.9.5/binaries/apache-maven-3.9.5-bin.tar.gz
镜像站: https://mirrors.tuna.tsinghua.edu.cn/apache/maven/maven-3/
```

### 安装步骤
```bash
# 下载 Maven
cd /opt
sudo wget https://dlcdn.apache.org/maven/maven-3/3.9.5/binaries/apache-maven-3.9.5-bin.tar.gz

# 解压
sudo tar -xzf apache-maven-3.9.5-bin.tar.gz
sudo mv apache-maven-3.9.5 /usr/local/maven

# 配置环境变量
echo 'export MAVEN_HOME=/home/hsy/app/maven' >> ~/.bashrc
echo 'export PATH=$MAVEN_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# 验证
mvn -version

# 配置国内镜像（阿里云）
mkdir -p ~/.m2
cat > ~/.m2/settings.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<settings>
  <mirrors>
    <mirror>
      <id>aliyun</id>
      <mirrorOf>central</mirrorOf>
      <name>Aliyun Maven</name>
      <url>https://maven.aliyun.com/repository/public</url>
    </mirror>
  </mirrors>
</settings>
EOF
```

---

## 4. 安装 Kafka

### 下载链接
```
Kafka 3.4.0: https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz
镜像站: https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/
```

### 安装步骤
```bash
# 下载 Kafka
cd /opt
sudo wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz

# 解压
sudo tar -xzf kafka_2.13-3.4.0.tgz
sudo mv kafka_2.13-3.4.0 /home/hsy/app/kafka  

# 配置环境变量
echo 'export KAFKA_HOME=/home/hsy/app/kafka' >> ~/.bashrc
echo 'export PATH=$KAFKA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# 创建数据目录
sudo mkdir -p /data/kafka/zookeeper
sudo mkdir -p /data/kafka/kafka-logs
sudo mkdir -p /data/kafka/config  
sudo chown -R $USER:$USER /data/kafka

# 配置 Zookeeper
cat > /data/kafka/config/zookeeper.properties << 'EOF'
dataDir=/data/kafka/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
EOF

# 配置 Kafka
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
transact[hsy@localhost data-quality-platform]$ jps
119603 TaskManagerRunner
97143 Kafka
100151 Jps
119117 StandaloneSessionClusterEntrypoint
96623 QuorumPeerMain
transaction.state.log.min.isr=1
default.replication.factor=1
min.insync.replicas=1
EOF

# 启动 Zookeeper
nohup /home/hsy/app/kafka/bin/zookeeper-server-start.sh \
  /data/kafka/config/zookeeper.properties > /data/kafka/zookeeper.log 2>&1 &

# 等待5秒
sleep 5

# 启动 Kafka
nohup /home/hsy/app/kafka/bin/kafka-server-start.sh \
  /data/kafka/config/server.properties > /data/kafka/kafka.log 2>&1 &

# 验证
/home/hsy/app/kafka/bin/kafka-topics.sh --list --bootstrap-server 192.168.128.141:9092
---

## 5. 安装 Redis

### 下载链接
```
Redis 7.2: https://download.redis.io/releases/redis-7.2.3.tar.gz
```

### 安装步骤
```bash
# 安装编译工具
sudo yum install -y gcc make

# 下载 Redis
cd /opt
sudo wget https://download.redis.io/releases/redis-7.2.3.tar.gz

# 解压并编译
sudo tar -xzf redis-7.2.3.tar.gz
cd redis-7.2.3
sudo make
sudo make install PREFIX=/home/hsy/app/redis

# 创建配置文件
sudo mkdir -p /etc/redis
sudo cp redis.conf /etc/redis/

# 修改配置
sudo sed -i 's/bind 127.0.0.1/bind 0.0.0.0/g' /etc/redis/redis.conf
sudo sed -i 's/daemonize no/daemonize yes/g' /etc/redis/redis.conf
sudo sed -i 's/# requirepass foobared/requirepass your_password/g' /etc/redis/redis.conf

# 启动 Redis
/home/hsy/app/redis/bin/redis-server /etc/redis/redis.conf

# 验证
/home/hsy/app/redis/bin/redis-cli ping
```

---

## 6. 安装 MySQL 8

### 下载链接
```
MySQL 8.0: https://dev.mysql.com/downloads/mysql/
RPM Repository: https://dev.mysql.com/downloads/repo/yum/
```

### 安装步骤
```bash
# 下载 MySQL Yum Repository
cd /opt
sudo wget https://dev.mysql.com/get/mysql80-community-release-el7-7.noarch.rpm

# 安装 Repository
sudo rpm -ivh mysql80-community-release-el7-7.noarch.rpm

# 安装 MySQL
sudo yum install -y mysql-community-server

# 启动 MySQL
sudo systemctl start mysqld
sudo systemctl enable mysqld

# 获取临时密码
sudo grep 'temporary password' /var/log/mysqld.log

# 登录并修改密码
mysql -u root -p
# 输入临时密码后执行:
ALTER USER 'root'@'localhost' IDENTIFIED BY 'Yinghuayu680';
FLUSH PRIVILEGES;
EXIT;

# 创建远程访问用户
mysql -u root -p
SET GLOBAL validate_password.policy=LOW;
SET GLOBAL validate_password.length=6;
CREATE USER 'root'@'%' IDENTIFIED BY '123456';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
EXIT;
```

---

## 7. 安装 ClickHouse

### 下载链接
```
ClickHouse: https://packages.clickhouse.com/rpm/stable/
官方文档: https://clickhouse.com/docs/en/install
```

### 安装步骤
```bash
# 添加 ClickHouse Repository
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://packages.clickhouse.com/rpm/clickhouse.repo

# 安装 ClickHouse
sudo yum install -y clickhouse-server clickhouse-client

# 启动 ClickHouse
sudo systemctl start clickhouse-server
sudo systemctl enable clickhouse-server

# 验证
clickhouse-client --query "SELECT 1"

# 配置远程访问（可选）
sudo sed -i 's/<listen_host>127.0.0.1<\/listen_host>/<listen_host>0.0.0.0<\/listen_host>/g' \
  /etc/clickhouse-server/config.xml
sudo systemctl restart clickhouse-server
```

---

## 8. 安装 Flink

### 下载链接
```
Flink 1.17.1: https://archive.apache.org/dist/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.12.tgz
镜像站: https://mirrors.tuna.tsinghua.edu.cn/apache/flink/
```

### 安装步骤
```bash
# 下载 Flink
cd /opt
sudo wget https://archive.apache.org/dist/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.12.tgz

# 解压
sudo tar -xzf flink-1.17.1-bin-scala_2.12.tgz
sudo mv flink-1.17.1 /home/hsy/app/flink

# 配置环境变量
echo 'export FLINK_HOME=/home/hsy/app/flink' >> ~/.bashrc
echo 'export PATH=$FLINK_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# 配置 Flink
cat >> /home/hsy/app/flink/conf/flink-conf.yaml << 'EOF'
jobmanager.memory.process.size: 2048m
taskmanager.memory.process.size: 4096m
taskmanager.numberOfTaskSlots: 4
parallelism.default: 4
state.backend: rocksdb
EOF

# 启动 Flink 集群
/home/hsy/app/flink/bin/start-cluster.sh

# 验证（访问 Web UI）
# http://your-ip:8081
```

---

## 9. 安装 Prometheus

### 下载链接
```
Prometheus 2.48: https://github.com/prometheus/prometheus/releases/download/v2.48.0/prometheus-2.48.0.linux-amd64.tar.gz
```

### 安装步骤
```bash
# 下载 Prometheus
cd /opt
sudo wget https://github.com/prometheus/prometheus/releases/download/v2.48.0/prometheus-2.48.0.linux-amd64.tar.gz

# 解压
sudo tar -xzf prometheus-2.48.0.linux-amd64.tar.gz
sudo mv prometheus-2.48.0.linux-amd64 /usr/local/prometheus

# 启动 Prometheus
cd /usr/local/prometheus
nohup ./prometheus --config.file=prometheus.yml > prometheus.log 2>&1 &

# 验证（访问 Web UI）
# http://your-ip:9090
```

---

## 10. 验证安装

### 创建验证脚本
```bash
cat > ~/check_services.sh << 'EOF'
#!/bin/bash

echo "=== 服务状态检查 ==="

# Java
echo -n "Java: "
java -version 2>&1 | head -1

# Maven
echo -n "Maven: "
mvn -version 2>&1 | head -1

# Kafka
echo -n "Kafka: "
if /usr/local/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "Running ✓"
else
    echo "Not Running ✗"
fi

# Redis
echo -n "Redis: "
if /usr/local/redis/bin/redis-cli ping > /dev/null 2>&1; then
    echo "Running ✓"
else
    echo "Not Running ✗"
fi

# MySQL
echo -n "MySQL: "
if sudo systemctl is-active mysqld > /dev/null 2>&1; then
    echo "Running ✓"
else
    echo "Not Running ✗"
fi

# ClickHouse
echo -n "ClickHouse: "
if clickhouse-client --query "SELECT 1" > /dev/null 2>&1; then
    echo "Running ✓"
else
    echo "Not Running ✗"
fi

# Flink
echo -n "Flink: "
if curl -s http://localhost:8081 > /dev/null 2>&1; then
    echo "Running ✓"
else
    echo "Not Running ✗"
fi

# Prometheus
echo -n "Prometheus: "
if curl -s http://localhost:9090 > /dev/null 2>&1; then
    echo "Running ✓"
else
    echo "Not Running ✗"
fi
EOF

chmod +x ~/check_services.sh
~/check_services.sh
```

---

## 11. 初始化项目数据库

### 上传项目文件
```bash
# 在本地 Windows 上传文件到 CentOS
# 使用 WinSCP 或 scp 命令

# 或者在 CentOS 上克隆项目
cd ~
# 假设你已经上传了项目到 ~/data-quality-platform
```

### 初始化 MySQL
```bash
cd ~/data-quality-platform

# 初始化数据库
mysql -u root -p < sql/mysql_init.sql
# 输入密码: YourPassword123!

# 验证
mysql -u root -p
use data_quality;
show tables;
select * from quality_rules;
exit;
```

### 初始化 ClickHouse
```bash
# 初始化数据库
clickhouse-client < sql/clickhouse_init.sql

# 验证
clickhouse-client
use data_quality;
show tables;
exit;
```

---

## 12. 配置项目

### 修改配置文件
```bash
cd ~/data-quality-platform

# 编辑配置文件
vim src/main/resources/application.properties
```

修改以下内容:
```properties
# Kafka
kafka.bootstrap.servers=localhost:9092

# MySQL
mysql.url=jdbc:mysql://localhost:3306/data_quality?useSSL=false&serverTimezone=UTC
mysql.username=root
mysql.password=YourPassword123!

# Redis
redis.host=localhost
redis.port=6379

# ClickHouse
clickhouse.url=jdbc:clickhouse://localhost:8123/default
clickhouse.username=default
clickhouse.password=
```

---

## 13. 编译运行项目

### 编译项目
```bash
cd ~/data-quality-platform

# 编译打包
mvn clean package -DskipTests

# 查看生成的 jar
ls -lh target/*.jar
```

### 创建 Kafka Topics
```bash
# 创建 Topics
/home/hsy/app/kafka/bin/kafka-topics.sh --create \
  --topic orders \
  --bootstrap-server 192.168.128.141:9092 \
  --partitions 4 \
  --replication-factor 1

/home/hsy/app/kafka/bin/kafka-topics.sh --create \
  --topic orders_valid \
  --bootstrap-server 192.168.128.141:9092 \
  --partitions 4 \
  --replication-factor 1

/home/hsy/app/kafka/bin/kafka-topics.sh --create \
  --topic orders_invalid \
  --bootstrap-server 192.168.128.141:9092 \
  --partitions 4 \
  --replication-factor 1

# 验证
/home/hsy/app/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server 192.168.128.141:9092
```

### 运行 Flink 作业
```bash
# 提交到 Flink
/home/hsy/app/flink/bin/flink run \
  -c com.dataplatform.quality.job.DataQualityMonitorJob \
  target/data-quality-platform-flink.jar

# 查看 Flink Web UI
# http://your-centos-ip:8081
```

### 生成测试数据
```bash
# 新开一个终端，运行数据生成器
cd ~/data-quality-platform
mvn exec:java -Dexec.mainClass="com.dataplatform.quality.util.DataGenerator"
mvn exec:java -Dexec.mainClass="com.dataplatform.quality.util.DataGenerator" 2>&1 | grep -v "DEBUG"

# 等待1-2分钟后查看
clickhouse-client --query "
SELECT 
    rule_name,
    toDateTime(check_time) as time,
    total_count,
    valid_count,
    invalid_count,
    round(pass_rate * 100, 2) as pass_rate_percent
FROM data_quality.data_quality_metrics
ORDER BY check_time DESC
"

clickhouse-client --query "SELECT rule_name, toDateTime(check_time) as time, total_count, valid_count, invalid_count, round(pass_rate * 100, 2) as pass_rate_percent FROM data_quality.data_quality_metrics ORDER BY check_time DESC" --format PrettyCompact

SELECT rule_name, toDateTime(check_time) as time, total_count, valid_count, invalid_count, round(pass_rate * 100, 2) as pass_rate_percent FROM data_quality.data_quality_metrics ORDER BY check_time DESC


clickhouse-client --query "TRUNCATE TABLE data_quality.data_quality_metrics"

```

## 14. 常用命令

```bash

# 启动应用
java -jar target/data-quality-platform-1.0-SNAPSHOT-spring-boot.jar


# 清空缓存
curl -s http://192.168.128.141:8080/api/test/cache/clear

# 预热三层缓存
for i in {1..100}; do curl -s http://192.168.128.141:8080/api/test/rules/3tier > /dev/null; done

# 测试三层缓存
ab -n 10000 -c 50 http://192.168.128.141:8080/api/test/rules/3tier

# 清空缓存
curl -s http://192.168.128.141:8080/api/test/cache/clear

# 预热 MySQL + Redis
for i in {1..100}; do curl -s http://192.168.128.141:8080/api/test/rules/mysql-redis > /dev/null; done

# 测试 MySQL + Redis
ab -n 10000 -c 50 http://192.168.128.141:8080/api/test/rules/mysql-redis

# 清空缓存
curl -s http://192.168.128.141:8080/api/test/cache/clear

# 预热 MySQL Only
for i in {1..100}; do curl -s http://192.168.128.141:8080/api/test/rules/mysql-only > /dev/null; done

# 测试 MySQL Only
ab -n 10000 -c 50 http://192.168.128.141:8080/api/test/rules/mysql-only



tail -f /home/hsy/app/flink/log/flink-*-taskmanager-*.log | grep -E "Cache query|Cache Performance"



cd ~/data-quality-platform

# 编译
mvn clean compile

# 运行 RocksDB 写入性能测试
mvn test -Dtest=FlinkStatePerformanceTest#testRocksDBWritePerformance -DfailIfNoTests=false

# 运行 RocksDB 读取性能测试
mvn test -Dtest=FlinkStatePerformanceTest#testRocksDBReadPerformance -DfailIfNoTests=false

# 运行存储容量测试
mvn test -Dtest=FlinkStatePerformanceTest#testRocksDBStorageCapacity -DfailIfNoTests=false

# 运行状态访问延迟测试
mvn test -Dtest=FlinkStatePerformanceTest#testStateAccessLatency -DfailIfNoTests=false

# 运行内存使用测试
mvn test -Dtest=FlinkStatePerformanceTest#testStateBackendMemoryUsage -DfailIfNoTests=false


# 运行所有 RocksDB 性能测试
mvn test -Dtest=FlinkStatePerformanceTest -DfailIfNoTests=false 2>&1 | tail -150


# 索引
mvn test -Dtest=ClickHouseQueryOptimizationTest -DfailIfNoTests=false 2>&1 | tail -150


```






