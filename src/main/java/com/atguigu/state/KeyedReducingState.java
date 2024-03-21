package com.atguigu.state;


import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedReducingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs() * 1000L)
                )
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    ReducingState<Integer> vcReducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        vcReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>(
                                "vcReducingState",
                                Integer::sum,
                                Types.INT)
                        );
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        vcReducingState.add(value.getVc());
                        out.collect("key:" + ctx.getCurrentKey() + ",vcSum:" + vcReducingState.get());
                    }
                })
                .print();
        env.execute();
    }
}
