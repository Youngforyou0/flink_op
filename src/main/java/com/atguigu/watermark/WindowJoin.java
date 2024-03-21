package com.atguigu.watermark;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.stream.Stream;

public class WindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> ds1 = env
                .fromElements(
                        new WaterSensor("s1", 1L, 0),
                        new WaterSensor("s1", 2L, 0),
                        new WaterSensor("s2", 11L, 0),
                        new WaterSensor("s3", 22L, 0)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L)
                );

        SingleOutputStreamOperator<WaterSensor> ds2 = env
                .fromElements(
                        new WaterSensor("s1", 9L, 9),
                        new WaterSensor("s1", 11L, 11),
                        new WaterSensor("s2", 12L, 12),
                        new WaterSensor("s3", 32L, 32)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000L;
                                    }
                                })
                );
        DataStream<String> join = ds1.join(ds2)
                .where(WaterSensor::getId)
                .equalTo(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<WaterSensor, WaterSensor, String>() {
                    @Override
                    public String join(WaterSensor first, WaterSensor second) throws Exception {
                        return first.toString() + "---->" + second.toString();
                    }
                });

        join.print();
        env.execute();
    }
}
