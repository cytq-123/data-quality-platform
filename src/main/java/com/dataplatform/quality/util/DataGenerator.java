package com.dataplatform.quality.util;

import com.dataplatform.quality.model.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 测试数据生成器 - 生成订单数据发送到 Kafka
 */
public class DataGenerator {
    
    private static final Random random = new Random();
    
    public static void main(String[] args) throws Exception {
        // Kafka 配置
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.128.141:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 禁用事务性 Producer 和幂等性，避免 Producer ID 分配超时
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topic = "orders";
        
        System.out.println("开始生成测试数据...");
        
        int count = 0;
        while (true) {
            // 生成订单数据
            Order order = generateOrder();
            
            // 每100条数据中插入1条异常数据
            if (count % 100 == 0) {
                order = generateInvalidOrder();
                System.out.println("生成异常数据: " + order.toJson());
            }
            
            // 发送到 Kafka
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(topic, order.getOrderId(), order.toJson());
            producer.send(record);
            
            count++;
            if (count % 1000 == 0) {
                System.out.println("已生成 " + count + " 条数据");
            }
            
            // 控制发送速率
            // 100 QPS:  TimeUnit.MILLISECONDS.sleep(10);
            // 10000 QPS: TimeUnit.MICROSECONDS.sleep(100);
            // 20000 QPS: TimeUnit.MICROSECONDS.sleep(50);
            TimeUnit.MILLISECONDS.sleep(10);  // 当前：100 QPS
        }
    }
    
    /**
     * 生成正常订单数据
     */
    private static Order generateOrder() {
        Order order = new Order();
        
        order.setOrderId("ORD" + System.currentTimeMillis() + random.nextInt(1000));
        order.setUserId("USER" + (10000 + random.nextInt(90000)));
        order.setOrderAmount(50.0 + random.nextDouble() * 450.0); // 50-500元
        order.setOrderTime(System.currentTimeMillis() - random.nextInt(3600000)); // 过去1小时内
        order.setProductId("PROD" + (1000 + random.nextInt(9000)));
        order.setQuantity(1 + random.nextInt(10)); // 1-10件
        order.setStatus(randomChoice("PENDING", "PAID", "SHIPPED", "COMPLETED"));
        order.setPaymentMethod(randomChoice("ALIPAY", "WECHAT", "CREDIT_CARD"));
        order.setAddress("北京市朝阳区xxx路" + random.nextInt(100) + "号");
        order.setPhone("1" + (30 + random.nextInt(70)) + String.format("%08d", random.nextInt(100000000)));
        order.setEventTime(System.currentTimeMillis());
        
        return order;
    }
    
    /**
     * 生成异常订单数据 (用于测试)
     */
    private static Order generateInvalidOrder() {
        Order order = generateOrder();
        
        int errorType = random.nextInt(6);
        
        switch (errorType) {
            case 0:
                // 订单金额为负数
                order.setOrderAmount(-100.0);
                break;
            case 1:
                // 订单金额超过上限
                order.setOrderAmount(150000.0);
                break;
            case 2:
                // 用户ID为空
                order.setUserId(null);
                break;
            case 3:
                // 订单ID为空
                order.setOrderId(null);
                break;
            case 4:
                // 手机号格式错误
                order.setPhone("12345");
                break;
            case 5:
                // 商品数量超过上限
                order.setQuantity(1000);
                break;
        }
        
        return order;
    }
    
    /**
     * 随机选择
     */
    private static String randomChoice(String... options) {
        return options[random.nextInt(options.length)];
    }
}
