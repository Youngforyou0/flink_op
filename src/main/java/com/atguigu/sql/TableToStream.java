package com.atguigu.sql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableToStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> sensorDS = env
                .fromElements(
                        new WaterSensor("s1", 1L, 1),
                        new WaterSensor("s2", 2L, 2),
                        new WaterSensor("s3", 3L, 3),
                        new WaterSensor("s3", 4L, 4)
                );

        //todo: 1.流转表
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table sensorTable = tableEnv.fromDataStream(sensorDS);
        tableEnv.createTemporaryView("sensor",sensorTable);

        Table filterTable = tableEnv.sqlQuery("select id,ts,vc from sensor where ts >= 1");
        Table sumTable = tableEnv.sqlQuery("select id,sum(vc) as sumVc from sensor group by id");

        //todo: 2.标转流
        //2.1 只追加流
        tableEnv.toDataStream(filterTable, WaterSensor.class).print("sensorTable");
        tableEnv.toChangelogStream(sumTable).print("sumTable");

        env.execute();
    }
}
