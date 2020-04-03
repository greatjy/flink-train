package com.health.alibaba.flink_demo_ch6;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author: wjy
 * @Date: 2020/4/1 23:52
 */
public class FlinkSqlTableTest {
    public static void main(String[] args) throws Exception {
        // step1 拿到执行环境 批处理ExecutionEnvironment/ 流处理 StreamExecutionEnviroment
        // 根据执行环境创建对应的tableEnvironment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
        // step2 拿到dataset
        CsvReader csvReader = env.readCsvFile("C:\\Users\\MrWang\\Desktop\\flink-train-master\\people.csv");
        CsvReader reader = csvReader;
        reader.ignoreFirstLine();
        DataSource<Sales> tDataSource = reader.pojoType(Sales.class,"transactionId",
                "customerId" ,"itemId", "amount");

        //step3 tenv将dataset/datasteam 注册一个内存表
        tableEnv.registerDataSet("sales", tDataSource,
                "transactionId, customerId, itemId, amount");

        // step5 tableenv通过sql方式进行查询
        Table table1 = tableEnv.sqlQuery("select customerId, sum(amount) as money from sales " +
                "group by customerId");

        // step6 将返回的table转成dataset进行护理
        DataSet<Row> rowDataSet = tableEnv.toDataSet(table1, Row.class);
        rowDataSet.print();
    }
}
