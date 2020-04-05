package com.health.alibaba.flink_demo_ch7;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sun.security.util.ArrayUtil;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: wjy
 * @Date: 2020/4/5 0:13
 */
public class FlinkWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] values = value.split(",");
                String code = values[0];
                long eventTime = Long.parseLong(values[1]);
                out.collect(new Tuple2<>(code, eventTime));
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {


            //当前时间
            long currentMaxTimeStamp = 0L;
            // 最大延迟的事件
            long maxOutOfOrders = 3000L;
            // 初始化水印时间 水印时间一直在增长
            long lastEmitterWatermark = Long.MIN_VALUE;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                // System.out.println("----------------1");
                // watermark 就是 当前的时间戳  减去 延迟的时间
                long l = currentMaxTimeStamp - maxOutOfOrders;
                if(l >= lastEmitterWatermark) {
                    lastEmitterWatermark = l;
                }
                // System.out.println("----------------附上水印的时间"+lastEmitterWatermark);
                return new Watermark(lastEmitterWatermark);
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {

                System.out.println("--------------2");
                long time  =element.f1;
                if(time > currentMaxTimeStamp) {
                    // currentMaxTimeStamp 一直在增长
                    currentMaxTimeStamp = time;
                }


                return element.f1;
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(10))
                .apply(new WindowFunction<Tuple2<String,Long>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        System.out.println("?????");
                        Iterator<Tuple2<String, Long>> iterator = input.iterator();
                        List<String> builder = new ArrayList<>();
                        while(iterator.hasNext()) {
                            Tuple2<String, Long> element = iterator.next();
                            FastDateFormat instance = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss:SSS");
                            builder.add(instance.format(element.f1));
                            String result = String.format("key: %s   data : %s startTime: %s endTime : %s", tuple.toString(), builder.toString(), instance.format(window.getStart()),
                                    instance.format(window.getEnd()));
                            out.collect(result);
                        }
                    }
                }).print("windows 计算结果");
        env.execute("watermark");


    }
}
