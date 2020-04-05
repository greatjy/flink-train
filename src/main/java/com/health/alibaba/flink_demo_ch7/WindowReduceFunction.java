package com.health.alibaba.flink_demo_ch7;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: wjy
 * @Date: 2020/4/4 17:05
 */
public class WindowReduceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        // reduceFunction(env, dataStreamSource);
        processWindowFunction(env, dataStreamSource);
    }

    private static void reduceFunction(StreamExecutionEnvironment env, DataStreamSource<String> dataStreamSource) throws Exception {
        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                try{
                    int n = Integer.parseInt(value);
                    out.collect(new Tuple2<>(1,n));
                }
                catch (Exception e) {
                    out.collect(new Tuple2<>(1,999));
                }
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        Tuple2<Integer, Integer> result = new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                        System.out.println(value1.f1+" and "+value2.f1);
                        return result;
                    }
                })
                .print()
                .setParallelism(1);
        env.execute("windowFunctionApp");
    }


    public static void processWindowFunction(StreamExecutionEnvironment env, DataStreamSource<String> dataStreamSource) throws Exception {
        SingleOutputStreamOperator<String> process = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                int n = Integer.parseInt(value);
                out.collect(new Tuple2<>(1, n));
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).process(new ProcessWindowFunction<Tuple2<Integer, Integer>, String, Tuple, TimeWindow>() {


            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<String> out) throws Exception {
                // 简单统计窗口内元素个数
                long count = 0;
                for(Tuple2<Integer, Integer> in : elements){
                    count++;
                }
                out.collect("Window: " + context.window() + "count:" + count);
            }
        });
        process.print();
        process.setParallelism(1);
        env.execute("processFunctionApp");
    }
}
