package com.health.alibaba.flink_demo_ch8;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author: wjy
 * @Date: 2020/4/7 0:02
 */
public class KafkaConnectorProducerApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从socket接收数据，通过flink处理之后 数据发送给kafka
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9999);
        String topic = "flinkTest";
        SingleOutputStreamOperator<String> mappedStream = socketStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + "!";
            }
        });
        mappedStream.addSink(new FlinkKafkaProducer<String>("192.168.18.139:9092",
                topic, new SimpleStringSchema()));

        env.execute("kafka-sink");
    }
}
