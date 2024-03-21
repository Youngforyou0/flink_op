package com.atguigu.state;


import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;

public class KeyedMapState {
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
                    MapState<Integer,Integer> vcMapState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer,Integer>("vcMapState",Types.INT,Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer elemVc = value.getVc();
                        if(vcMapState.contains(elemVc)){
                            int count = vcMapState.get(elemVc) + 1;
                            vcMapState.put(elemVc,count);
                        }else {
                            vcMapState.put(elemVc,1);
                        }
                        StringBuilder stringBuilder = new StringBuilder();
                        Iterator<Map.Entry<Integer, Integer>> iterator = vcMapState.iterator();
                        while (iterator.hasNext()){
                            Map.Entry<Integer, Integer> next = iterator.next();
                            stringBuilder.append(next.toString() + "\n");
                        }
                        out.collect("key:" + ctx.getCurrentKey() + "\n" + stringBuilder.toString() + "\n");
                    }
                })
                .print();
        env.execute();
    }
}
