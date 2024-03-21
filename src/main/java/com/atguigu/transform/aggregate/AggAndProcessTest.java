package com.atguigu.transform.aggregate;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class AggAndProcessTest {
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
                .aggregate(
                        new MyAggFunction(),
                        new MyProcessWindowFunction()
                )
                .print();

        env.execute();
    }
    public static class MyAggFunction implements AggregateFunction<WaterSensor,WaterSensor,WaterSensor>{

        @Override
        public WaterSensor createAccumulator() {
            return null;
        }

        @Override
        public WaterSensor add(WaterSensor value, WaterSensor accumulator) {
            System.out.println("aggregate方法被调用," + "value:" + value + ",accumulator:" + accumulator);
            if (accumulator == null){
                return new WaterSensor(value.getId(),value.getTs(), value.getVc());
            }else {
                // return new WaterSensor(value.getId(), value.getTs(), value.getVc() + accumulator.getVc());
                accumulator.setTs(value.getTs());
                accumulator.setVc(accumulator.getVc() + value.getVc());
                return accumulator;
            }
        }

        @Override
        public WaterSensor getResult(WaterSensor accumulator) {
            return accumulator;
        }

        @Override
        public WaterSensor merge(WaterSensor a, WaterSensor b) {
            return null;
        }
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<WaterSensor,String,String, TimeWindow>{
    @Override
    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        for (WaterSensor element : elements) {
            stringBuilder.append(elements.toString() +",");
        }
        out.collect(s+ ":" + stringBuilder);
    }
}
}
