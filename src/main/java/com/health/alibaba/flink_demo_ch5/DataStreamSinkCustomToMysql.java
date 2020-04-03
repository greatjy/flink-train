package com.health.alibaba.flink_demo_ch5;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: wjy
 * @Date: 2020/4/1 19:54
 */
public class DataStreamSinkCustomToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        writeToMysql(env);
//        env.execute("DataStreamSinkCustomToMysql job");

    }

    public static void writeToMysql(StreamExecutionEnvironment env) {
         // step 1 从socket中获取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Student> streamStudentSource = dataStreamSource.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                Student student = new Student();
                String[] values = value.split(",");
                String id = values[0];
                String name = values[1];
                int age = Integer.parseInt(values[2]);
                student.setId(id);
                student.setName(name);
                student.setAge(age);
                return student;
            }
        });
        streamStudentSource.addSink(new CustomSinkToMysql());
    }

}
