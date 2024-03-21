package com.atguigu.sql;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import static org.apache.flink.table.api.Expressions.$;


public class TestSQL {
    public static void main(String[] args) {
        //todo: 1.创建表环境——方式1
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);


        //todo: 2.创建一个 source table
        TableResult tableSource = tEnv.executeSql("CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT, \n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ");\n");


        //todo: 2.执行查询(use sql)
        Table table = tEnv.sqlQuery(
                "SELECT \n" +
                "    id, \n" +
                "    ts, \n" +
                "    row_time, \n" +
                "    vc, \n" +
                "    count(vc) over(partition by id order by row_time range between interval '10' second preceding and current row) cnt \n" +
                "FROM source"
        );
        table.execute().print();
//        tEnv.createTemporaryView("tmp",table);
//
//        //todo: 4.1 执行查询(table api)
//        Table table1 = tEnv.from("source");
//        Table result = table1
//                .groupBy($("id"))
//                .aggregate($("vc").sum().as("sumVc"))
//                .select($("id"), $("sumVc"));
//
//        //todo: 5.将表结果表发送到sink
//        // tEnv.executeSql("insert into sink select * from tmp");
//        result.executeInsert("sink");
    }
}
