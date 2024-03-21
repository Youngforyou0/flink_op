package com.atguigu.watermark;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.MyPartitioner;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


public class PartitionWatermarkOutOfOrderness {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Integer> socketDS = env
                .socketTextStream("localhost", 7777)
                .map(r -> Integer.parseInt(r))
                .partitionCustom(new MyPartitioner(), r -> r);

        socketDS
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                            .<Integer>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                            .withTimestampAssigner(((element, recordTimestamp) -> element * 1000L))
                            .withIdleness(Duration.ofSeconds(5))
                )
                .process(new ProcessFunction<Integer, Integer>() {
                    @Override
                    public void processElement(Integer value, ProcessFunction<Integer, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                        int parallelismIndex = getRuntimeContext().getIndexOfThisSubtask();
                        System.out.println("Data:" + value + ", parallelism" + parallelismIndex);
                        out.collect(value);
                    }
                })
                .keyBy(r -> r % 3)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<Integer, String, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + integer + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                    }
                })
                .print();

        env.execute();
    }
}
