package com.health.alibaba.flink_demo_ch6;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: wjy
 * @Date: 2020/4/2 0:14
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Sales {
    private String transactionId;
    private String customerId;
    private String itemId;
    private double amount;
}
