package com.health.alibaba.flink_demo_ch4;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: wjy
 * @Date: 2020/3/29 19:34
 */
public class DataSetDataSourceApp {
    public static void main(String[] args) throws Exception {
        // step1 获取执行环境上下文
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // step2 读取Dataset datasource
        // readFromCollection(env);
        // readFromLocalFile(env);
        // readFromCsvFile(env);
        readFromCompressFile(env);
    }

    private static void readFromCollection(ExecutionEnvironment env) throws Exception {
        ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        env.fromCollection(list).print();
        env.fromElements(1,2,3,4,5,6,7,8,9,0).print();
    }

    private static void readFromLocalFile(ExecutionEnvironment env) throws Exception {
        String path = "C:\\Users\\MrWang\\Desktop\\testDir";
        env.readTextFile(path,"utf-8").print();
    }

    private static void readFromCsvFile(ExecutionEnvironment env) throws  Exception {
        String path = "C:\\Users\\MrWang\\Desktop\\test.csv";
        DataSource<Tuple2<Integer, String>> csvDataset = env.readCsvFile(path).ignoreFirstLine().includeFields(5).types(Integer.class, String.class);
        // DataSource<Employee> employeeDataSource = env.readCsvFile(path).ignoreFirstLine().pojoType(Employee.class, "name", "age","work");
        // employeeDataSource.print();
        csvDataset.print();
    }

    private static void readFromCompressFile(ExecutionEnvironment env) throws Exception {
        String path = "C:\\Users\\MrWang\\Desktop\\Desktop.zip";
        env.readTextFile(path).print();

    }
}
