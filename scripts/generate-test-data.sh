#!/bin/bash

# 生成测试数据

echo "=== 生成测试数据 ==="

cd ..
mvn exec:java -Dexec.mainClass="com.dataplatform.quality.util.DataGenerator"
