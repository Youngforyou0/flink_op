package com.atguigu.state;


import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

public class KeyedListState {
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
                    ListState<Integer> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("listState",Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        listState.add(value.getVc());
                        ArrayList<Integer> vcList = new ArrayList<>();
                        for (Integer integer : listState.get()) {
                            vcList.add(integer);
                        }
                        vcList.sort((o1, o2) -> -(o1 -o2));

                        if (vcList.size() > 3){
                            vcList.remove(3);
                        }
                        out.collect("top3:" + vcList.toString());
                        listState.update(vcList);
                    }
                })
                .print();
        env.execute();
    }
}
