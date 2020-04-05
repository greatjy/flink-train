package com.health.alibaba.flink_demo_ch7;

import com.health.alibaba.flink_demo_ch5.MySourceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: wjy
 * @Date: 2020/4/3 23:42
 */
public class TimeDataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        slideWindowFunction(dataStreamSource, env);
    }

    private static void gundongWIindowFunction(StreamExecutionEnvironment env,
                                               DataStreamSource<String> dataStreamSource) throws Exception {
        dataStreamSource.map( new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        })
                .keyBy(new KeySelector<Tuple2<String,Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .timeWindow(Time.seconds(5))
                .sum(1)
                .print()
                .setParallelism(1);
    }

    private static void slideWindowFunction(DataStreamSource<String> dataStreamSource,
                                            StreamExecutionEnvironment env) throws Exception {
        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] values = value.split(",");
                for(String word : values) {
                    if(word != null && !("".equals(word.trim()))) {
                        out.collect(new Tuple2<String, Integer>(word, 1));
                    }
                }
            }
        })
                .keyBy(0)
                //.timeWindow(Time.seconds(10), Time.seconds(5))
                .countWindow(5,3)
                .sum(1)
                .print()
                .setParallelism(1);
        env.execute("slidetimeAPP");
    }

}
