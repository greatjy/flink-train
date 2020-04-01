package com.health.alibaba.flink_demo_ch5;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: wjy
 * @Date: 2020/4/1 1:17
 */
public class DataStreamTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // basicTransformationOperation(env);
        // unionTransformation(env);
        splitTransformationTest(env);
        env.execute("datastream transformation");
    }

    private static void basicTransformationOperation(StreamExecutionEnvironment env) {
        env.addSource(new MySourceFunction())
                .map((MapFunction<String, String>) value -> value.substring(5))
                .filter((FilterFunction<String>) value -> Integer.parseInt(value.substring(value.length() - 1)) % 2 == 0)
                .print();
    }

    public static void unionTransformation(StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStream1 = env.addSource(new MySourceFunction());
        DataStreamSource<String> dataStream2 = env.addSource(new MySourceFunction());
        dataStream1.union(dataStream2).print().setParallelism(1);
    }

    public static void splitTransformationTest(StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStreamSource = env.addSource(new MySourceFunction());
        SplitStream<String> splits = dataStreamSource.split(new OutputSelector<String>() {
            @Override
            public Iterable<String> select(String value) {
                List<String> list = new ArrayList<>();
                int last = Integer.parseInt(value.substring(value.length() - 1));
                if (last % 2 == 0) {
                    list.add("even");
                } else {
                    list.add("odd");
                }
                return list;
            }
        });
        splits.select("even").print().setParallelism(1);
    }
}
