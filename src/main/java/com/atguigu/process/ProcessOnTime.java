package com.atguigu.process;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;


public class ProcessOnTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> process = env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000L;
                                    }
                                })
                )
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //注册一个在处理时间5秒后触发的定时器。当定时器触发时，onTimer方法会被调用，并输出一条消息。
                        // 提取事件时间
                        Long timestamp = ctx.timestamp();
                        // 定时器
                        TimerService timerService = ctx.timerService();
                        timerService.registerEventTimeTimer(5000);
                        // timerService.registerProcessingTimeTimer(5000);
                        // long currentProcessingTime = timerService.currentProcessingTime();
                        // long currentWatermark = timerService.currentWatermark();

                        System.out.println("当前时间是：" + timestamp + ",注册了一个定时器");
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        System.out.println("当前时间是: " + timestamp + ",定时器触发");
                    }
                });
        env.execute();
    }
}
