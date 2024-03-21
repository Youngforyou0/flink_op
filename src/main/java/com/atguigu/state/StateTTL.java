package com.atguigu.state;


import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTTL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env
                .socketTextStream("localhost",7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs() * 1000L)
                )
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        StateTtlConfig stateTtlConfig = StateTtlConfig
                                .newBuilder(Time.seconds(10))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();

                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.INT);
                        stateDescriptor.enableTimeToLive(stateTtlConfig);

                        lastVcState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        int lastVc = lastVcState.value() == null ? 0 : lastVcState.value();
                        Integer vc = value.getVc();
                        if (Math.abs(vc -lastVc) > 10){
                            out.collect("lastVc:" + lastVc + ",vc:" + vc);
                        }
                        lastVcState.update(vc);
                    }
                })
                .print();
        env.execute();
    }
}
