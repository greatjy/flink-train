package com.health.alibaba.flink_demo_ch2;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: wjy
 * @Date: 2020/3/29 18:04
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WordCount {
        String word;
        Integer count;
}
