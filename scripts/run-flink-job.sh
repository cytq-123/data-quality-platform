#!/bin/bash

# 运行 Flink 作业

echo "=== 运行数据质量监控 Flink 作业 ==="

# 1. 编译打包
echo "编译打包项目..."
cd ..
mvn clean package -DskipTests

# 2. 提交到 Flink
echo "提交 Flink 作业..."
flink run \
  -c com.dataplatform.quality.job.DataQualityMonitorJob \
  -p 4 \
  target/data-quality-platform-1.0-SNAPSHOT.jar

echo "=== Flink 作业已提交 ==="
