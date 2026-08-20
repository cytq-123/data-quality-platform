package com.dataplatform.quality.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 订单数据模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /** 订单ID */
    private String orderId;
    
    /** 用户ID */
    private String userId;
    
    /** 订单金额 */
    private Double orderAmount;
    
    /** 订单时间 (时间戳) */
    private Long orderTime;
    
    /** 商品ID */
    private String productId;
    
    /** 商品数量 */
    private Integer quantity;
    
    /** 订单状态 */
    private String status;
    
    /** 支付方式 */
    private String paymentMethod;
    
    /** 收货地址 */
    private String address;
    
    /** 手机号 */
    private String phone;
    
    /** 事件时间 (用于 Flink 水位线) */
    private Long eventTime;
    
    /**
     * 从 JSON 字符串创建订单对象
     */
    public static Order fromJson(String json) {
        return com.alibaba.fastjson2.JSON.parseObject(json, Order.class);
    }
    
    /**
     * 转换为 JSON 字符串
     */
    public String toJson() {
        return com.alibaba.fastjson2.JSON.toJSONString(this);
    }
}
