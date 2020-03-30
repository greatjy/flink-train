package com.health.alibaba.flink_demo_ch4;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Filter;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

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

        // test flatmap
//        List<String> words = new ArrayList<>();
//        words.add("hadooop,spark");
//        words.add("flink,spark");
//        words.add("hdfs,hive,flink,maxcompute,blink");
//        DataSource<String> stringDataSource = env.fromCollection(words);
        //testFlatMap(stringDataSource);
        //test distinctt
        // testDistinct(stringDataSource);
//        List<Tuple2<Integer, String>> nameList = new ArrayList<>();
//        nameList.add(new Tuple2<>(1, "wangjiayang"));
//        nameList.add(new Tuple2<>(2, "fangxinyi"));
//        nameList.add(new Tuple2<>(3, "liuyan"));
//        nameList.add(new Tuple2<>(4, "maxinyi"));
//        DataSource<Tuple2<Integer, String>> nameDataSource = env.fromCollection(nameList);
//
//        List<Tuple2<Integer, String>> birthPlace = new ArrayList<>();
//        birthPlace.add(new Tuple2<>(1,"baoji"));
//        birthPlace.add(new Tuple2<>(2,"yantai"));
//        birthPlace.add(new Tuple2<>(3,"baoji"));
//        birthPlace.add(new Tuple2<>(5,"british"));
//        DataSource<Tuple2<Integer, String>> placeDataSource = env.fromCollection(birthPlace);
//        testJoin(nameDataSource, placeDataSource);

        List<String> teams = new ArrayList<>();
        teams.add("china");
        teams.add("brazil");
        teams.add("usa");
        List<Integer> points = new ArrayList<>();
        points.add(0);
        points.add(1);
        points.add(3);
        DataSource<String> teamDataSource = env.fromCollection(teams);
        DataSource<Integer> pointsDataSource = env.fromCollection(points);
        testCross(teamDataSource, pointsDataSource);
    }

    public static void testMapFunction(DataSource<Integer> dataSource) throws Exception {
        dataSource.map((MapFunction<Integer, Integer>) value -> value + 1)
                .filter((FilterFunction<Integer>)value -> value > 5)
                .print();
    }

    public static void testFilterFunction(DataSource<Integer> dataSource) throws Exception {
        dataSource.filter((FilterFunction<Integer>) value -> value > 5).print();
    }

    public static void testMapPartition(DataSource<Integer> dataSource) throws Exception {
        dataSource.mapPartition((MapPartitionFunction<Integer, Integer>) (values, out) -> {
            System.out.println("hello world");
            for(Integer value : values) {
                out.collect(value + 1);
            }
        }).print();

    }

    public static void testFirst(DataSource<Integer> dataSource) throws Exception {
        dataSource.first(3).print();
    }

    public static void testFlatMap(DataSource<String> dataSource) throws Exception {
        dataSource.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            String[] strings = value.split(",");
            for(String s : strings) {
                out.collect(new Tuple2<String, Integer>(s, 1));
            }
        }).print();
    }

    public static void testDistinct(DataSource<String> dataSource) throws Exception {
        dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(",");
                for(String word : words) {
                    out.collect(word);
                }
            }
        }).distinct().print();
    }

    public static void testJoin(DataSource<Tuple2<Integer, String>> dataSoure1,
                                DataSource<Tuple2<Integer,String>> dataSource2) throws Exception {
        dataSoure1.leftOuterJoin(dataSource2).where(new KeySelector<Tuple2<Integer,String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                return value.f0;
            }
        }).equalTo(new KeySelector<Tuple2<Integer,String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                return value.f0;
            }
        }).with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if(second != null) {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }
                else {
                    return new Tuple3<>(first.f0, first.f1, "null");
                }
            }
        }).print();
    }

    public static void testCross(DataSource dataSource1, DataSource dataSource2) throws Exception {
        dataSource1.cross(dataSource2).print();
    }
}
