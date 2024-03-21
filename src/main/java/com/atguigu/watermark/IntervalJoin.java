package com.atguigu.watermark;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> ds1 = env
                .socketTextStream("localhost",7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L)
                );

        SingleOutputStreamOperator<WaterSensor> ds2 = env
                .socketTextStream("localhost",8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000L;
                                    }
                                })
                );

        OutputTag<WaterSensor> ds1lateTag = new OutputTag<>("ds1late", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> ds2lateTag = new OutputTag<>("ds2late", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<String> intervalJoin = ds1
                .keyBy(WaterSensor::getId)
                .intervalJoin(ds2.keyBy(WaterSensor::getId))
                .between(Time.seconds(-2), Time.seconds(1))
                .sideOutputLeftLateData(ds1lateTag)
                .sideOutputRightLateData(ds2lateTag)
                .process(new ProcessJoinFunction<WaterSensor, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor left, WaterSensor right, ProcessJoinFunction<WaterSensor, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "--->" + right);
                    }
                });

        intervalJoin.print("主流");
        intervalJoin.getSideOutput(ds1lateTag).print("ds1流");
        intervalJoin.getSideOutput(ds2lateTag).print("ds2流");
        env.execute();
    }
}
