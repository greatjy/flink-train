package com.health.alibaba.flink_demo_ch5;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: wjy
 * @Date: 2020/3/31 11:43
 */
public class SocketDataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // socketFunction(env);
        // sourceFunctionTest(env);
        // parallelSourceFunctionTest(env);
        richParallelSourceFunctionTest(env);
        env.execute("SocketDataStream");
    }

    public static void socketFunction(StreamExecutionEnvironment env) {
        DataStreamSource<String> datastream = env.socketTextStream("localhost", 9999);
        datastream.print().setParallelism(1);
    }

    private static void sourceFunctionTest(StreamExecutionEnvironment env) {
        env.addSource(new MySourceFunction()).print().setParallelism(1);
    }

    private static void parallelSourceFunctionTest(StreamExecutionEnvironment senv) {
        senv.addSource(new MyParallelSourceFunction()).print().setParallelism(2);
    }

    private static void richParallelSourceFunctionTest(StreamExecutionEnvironment senv) {
        senv.addSource(new MyRichParallelSourceFunction()).print().setParallelism(3);
    }
}
