package com.health.alibaba.flink_demo_ch4;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: wjy
 * @Date: 2020/3/29 22:08
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Employee {
    private String name;
    private Integer age;
    private String work;
}
