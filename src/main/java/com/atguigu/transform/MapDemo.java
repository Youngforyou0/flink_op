package com.atguigu.transform;


import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // 方式一：匿名类部类
        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        // 方式二：lambda表达式
        SingleOutputStreamOperator<String> map1 = sensorDS.map(sensor -> sensor.getId());
        map1.print();

        env.execute();
    }
}
