package com.health.alibaba.flink_demo_ch8;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: wjy
 * @Date: 2020/4/6 22:31
 */
public class KafkaConnectorConsumerApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 把kafka和 flink进行整合，把kafka 作为flink的source
        String topic = "flinkTest";
        Properties propertie = new Properties();
        propertie.setProperty("bootstrap.servers","192.168.18.139:9092");
        propertie.setProperty("group.id","test");


        DataStreamSource<String> datasource = env.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), propertie));
        datasource.print();

        env.execute("kafka connector consumer app");
    }
}
