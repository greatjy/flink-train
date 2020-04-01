package com.health.alibaba.flink_demo_ch5;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: wjy
 * @Date: 2020/4/1 19:43
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Student {

    /**
     * 学生的id
     */
    private String id;

    /**
     * 学生的姓名
     */
    private String name;

    /**
     * 学生的年龄
     */
    private Integer age;
}
