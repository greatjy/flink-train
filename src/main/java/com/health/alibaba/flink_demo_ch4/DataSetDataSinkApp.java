package com.health.alibaba.flink_demo_ch4;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: wjy
 * @Date: 2020/3/30 21:17
 */
public class DataSetDataSinkApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        for(int i=0; i<10; i++) {
            list.add(new Tuple2<>("wjy studies", i));
        }
        DataSource<Tuple2<String, Integer>> listDataSource = env.fromCollection(list);
        listDataSource.writeAsCsv("C:\\Users\\MrWang\\Desktop\\test1.txt","\t","*",FileSystem.WriteMode.OVERWRITE);
        env.execute("testsink");
    }
}
