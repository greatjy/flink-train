package com.health.alibaba.flink_demo_ch2;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: wjy
 * @Date: 2020/3/28 18:48
 */
public class StreamWordCountFieldExpressionApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.readTextFile("C:\\Users\\MrWang\\Desktop\\new 1.txt");
        text.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                String[] words = value.split(" ");
                for(String word : words) {
                    out.collect(new WordCount(word,1));
                }
            }
        })//.keyBy("word")
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount value) throws Exception {
                        return value.word;
                    }
                })
                .timeWindow(Time.seconds(5))
                .sum("count")
                .print()
                .setParallelism(1)
        ;
    }
}

