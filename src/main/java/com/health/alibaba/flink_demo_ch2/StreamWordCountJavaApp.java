package com.health.alibaba.flink_demo_ch2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用java api 处理实时应用程序word count
 * 所以wordcount的数据会从一个socket来获得，所以是源源不断的
 * @Author: wjy
 * @Date: 2020/3/27 23:53
 */
public class StreamWordCountJavaApp {
    public static void main(String[] args) {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String canshu = parameterTool.get("canshu");
        Integer port = parameterTool.getInt("port");
        // step1 获取流处理环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // step2 读取流文件（例如socke文件）
        DataStreamSource<String> streamText = executionEnvironment.socketTextStream("localhost", 9999);
        // step3 对流文件进行业务操作
        streamText.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for(String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }).filter((FilterFunction<Tuple2<String, Integer>>) value -> {
            if(value == null) {
                return false;
            }
            return true;
        }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1)
                .print()
                .setParallelism(1);
        try {
            executionEnvironment.execute("first-stream-ontime-job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
