package com.health.alibaba.flink_demo_ch4;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @Author: wjy
 * @Date: 2020/3/29 23:37
 */
public class DataSetTransformationApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> list = new ArrayList<>();
        for(int i=0; i<10; i++) {
            list.add(i);
        }
        DataSource<Integer> datasource = env.fromCollection(list);
        testMapFunction(datasource);


    }

    public static void testMapFunction(DataSource<Integer> dataSource) throws Exception {
        dataSource.map((MapFunction<Integer, Integer>) value -> value + 1).print();
    }
}
