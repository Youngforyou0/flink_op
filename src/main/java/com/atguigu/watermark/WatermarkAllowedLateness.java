package com.atguigu.watermark;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.MyPartitioner;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * ClassName: WatermarkOutOfOrderness
 * Package: com.atguigu.watermark
 * Description:
 *
 * @Author rd_cey
 * @Create 2023/9/13 14:29
 * @Version 1.0
 */
public class WatermarkAllowedLateness {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction());

        // TODO 1.定义Watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 1.1 指定watermark生成：乱序的，等待3s
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 1.2 指定 时间戳分配器，从数据中提取
                .withTimestampAssigner(
                        (element, recordTimestamp) -> {
                            // 返回的时间戳，要 毫秒
                            return element.getTs() * 1000L;
                        });


        // TODO 2. 指定 watermark策略
        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        int parallelismIndex = getRuntimeContext().getIndexOfThisSubtask();
                        System.out.println("Data:" + value + ", parallelism" + parallelismIndex);
                        out.collect(value);
                    }
                });

        OutputTag<WaterSensor> lateData = new OutputTag<>("late_data", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<String> process = sensorDSwithWatermark
                .keyBy(WaterSensor::getId)
                // TODO 3.使用 事件时间语义 的窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2)) // 指定延迟关窗的时间，注意为事件事件
                .sideOutputLateData(lateData)  // 迟到数据的处理
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                long startTs = context.window().getStart();
                                long endTs = context.window().getEnd();
                                String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                                String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                                long count = elements.spliterator().estimateSize();

                                out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                            }
                        }
                );
        process.print("main");
        process.getSideOutput(lateData).print("lateData");

        env.execute();
    }

}
