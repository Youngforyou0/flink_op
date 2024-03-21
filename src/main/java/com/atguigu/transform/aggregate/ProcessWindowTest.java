package com.atguigu.transform.aggregate;


import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("localhost",7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs() * 1000L)
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<WaterSensor> out) throws Exception {
                        Integer sumVC = 0;
                        long maxTs = Long.MIN_VALUE;

                        for (WaterSensor element : elements) {
                            sumVC += element.getVc();
                            maxTs = Math.max(maxTs,element.getTs());
                        }
                        out.collect(new WaterSensor(s,maxTs,sumVC));
                    }
                })
                .print();
        env.execute();
    }
}
