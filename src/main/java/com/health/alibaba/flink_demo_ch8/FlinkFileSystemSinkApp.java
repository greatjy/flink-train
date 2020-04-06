package com.health.alibaba.flink_demo_ch8;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

import java.time.ZoneId;

/**
 * @Author: wjy
 * @Date: 2020/4/5 18:15
 * 测试需求，将socket数据写在hdfs上面  不推荐使用，因为都是小文件再hdfs上面。
 */
public class FlinkFileSystemSinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        BucketingSink<String> stringBucketingSink = new BucketingSink<>("C:\\Users\\MrWang\\Desktop\\test2");

        // 设置桶的时间格式
        stringBucketingSink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HHmm"));
        // 设置滚动时间
        stringBucketingSink.setBatchRolloverInterval(20);

        DataStreamSink<String> sink = dataStreamSource.addSink(stringBucketingSink);
        env.execute("filesystemApp");

    }
}
