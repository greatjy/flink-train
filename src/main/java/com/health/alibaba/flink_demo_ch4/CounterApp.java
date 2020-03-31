package com.health.alibaba.flink_demo_ch4;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * @Author: wjy
 * @Date: 2020/3/30 21:54
 */
public class CounterApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> classDataSource = env.fromElements("flink", "spark", "es", "hbase", "zookeeper");
        // 需要将任务sink到目的地 或者进行打印
        String path = "C:\\Users\\MrWang\\Desktop\\test2.txt";
        classDataSource.map(new RichMapFunction<String, String>() {

            // step1 定义一个累加器
            LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("counter", counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        }).writeAsText(path, FileSystem.WriteMode.OVERWRITE).setParallelism(2);
        JobExecutionResult result = env.execute("counter-app");
        long num = result.getAccumulatorResult("counter");
        System.out.println(num);
    }
}
