package com.health.alibaba.flink_demo_ch4;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.List;

/**
 * @Author: wjy
 * @Date: 2020/3/31 0:23
 */
public class DistributeCacheApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // step1 注册分布式缓存文件
        String path = "C:\\Users\\MrWang\\Desktop\\test1.txt";
        env.registerCachedFile(path, "test-cached-file");

        DataSource<String> data = env.fromElements("kafka", "zookeeper", "hbase", "hadoop复习", "大数据框架");
        data.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File file = getRuntimeContext().getDistributedCache().getFile("test-cached-file");
                List<String> strings = FileUtils.readLines(file);
                for(String string : strings) {
                    System.out.println(string);
                }
            }
        }).print();
    }
}
