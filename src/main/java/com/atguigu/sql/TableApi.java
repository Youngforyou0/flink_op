package com.atguigu.sql;


import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;


public class TableApi {
    public static void main(String[] args) {
        //todo: 1.创建表环境——方式1
        /**
         * TableEnvironment 是 Table API 和 SQL 的核心概念。它负责:
         * 在内部的 catalog 中注册 Table
         * 注册外部的 catalog
         * 加载可插拔模块
         * 执行 SQL 查询
         * 注册自定义函数 （scalar、table 或 aggregation）
         * DataStream 和 Table 之间的转换(面向 StreamTableEnvironment )
         */
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //todo: 1.1 创建表环境——方式2
        //从现有的 StreamExecutionEnvironment 创建一个 StreamTableEnvironment 与 DataStream API 互操作。
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //todo: 2.创建一个 source table
        TableResult tableSource = tEnv.executeSql("CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
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

        //todo: 3.创建一个 sink table
        TableResult tableSink = tEnv.executeSql("CREATE TABLE sink (\n" +
                "    id INT, \n" +
                "    sumVc INT\n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ");\n");

        //todo: 4.执行查询(use sql)
        /**
         * 从传统数据库系统的角度来看，Table 对象与 VIEW 视图非常像。也就是，定义了 Table 的查询是没有被优化的，而且会被内嵌到另一个引用了这个
         * 注册了的 Table的查询中。如果多个查询都引用了同一个注册了的Table，那么它会被内嵌每个查询中并被执行多次， 也就是说注册了的Table的结果
         * 不会被共享。
         */
        Table table = tEnv.sqlQuery("select id,sum(vc) as sumVc from source group by id");
        tEnv.createTemporaryView("tmp",table);

        //todo: 4.1 执行查询(table api)
        Table table1 = tEnv.from("source");
        Table result = table1
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sumVc"))
                .select($("id"), $("sumVc"));

        //todo: 5.将表结果表发送到sink
        // tEnv.executeSql("insert into sink select * from tmp");
        result.executeInsert("sink");
    }
}
