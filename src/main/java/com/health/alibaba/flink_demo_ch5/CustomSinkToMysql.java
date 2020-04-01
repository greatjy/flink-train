package com.health.alibaba.flink_demo_ch5;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: wjy
 * @Date: 2020/4/1 20:16
 */
public class CustomSinkToMysql extends RichSinkFunction<Student> {

    /**
     * 数据库的连接
     */
    Connection connection;

    /**
     * 执行连接
     */
    PreparedStatement preparedStatement;


    /**
     * 获取连接
     * @return  数据库的连接
     */
    private Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://localhost:3306/myfirstdb";
            conn = DriverManager.getConnection(url,"root","123456");

        } catch (ClassNotFoundException e) {
            System.out.println("clssnot found");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("sql excetption");
            e.printStackTrace();
        }

        return conn;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into student(student_id, student_name, student_age)" +
                "values(?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(Student value, Context context) throws SQLException {
        preparedStatement.setString(1,value.getId());
        preparedStatement.setString(2,value.getName());
        preparedStatement.setInt(3,value.getAge());
        preparedStatement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(connection != null){
            connection.close();
        }
        if(preparedStatement != null) {
            preparedStatement.close();
        }
    }
}
