package com.health.alibaba.flink_demo_ch2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 *  Java API 开发flink的批处理应用程序
 * @Author: wjy
 * @Date: 2020/3/27 16:12
 */
public class BatchWordCountJavaApp {
    public static void main(String[] args) throws Exception {

        String input = "C:\\Users\\MrWang\\Desktop\\new 1.txt";

        // 1. 获取执行环境环境的上下文
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据
        DataSource<String> myText = env.readTextFile(input);

        // 3.业务逻辑处理
        // 每一行按照指定分隔符进行拆分，使用map来进行统计
        // 按照第一个tuple中的第一个字符进行group，然后按照第二个字符进行sum求和
        myText.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 将字符串拆分，并为每一个附上一个1
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        })
                .groupBy(0)
                .sum(1)
                .print();
    }
}
