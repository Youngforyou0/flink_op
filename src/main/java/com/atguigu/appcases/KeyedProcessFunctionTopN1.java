package com.atguigu.appcases;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;


public class KeyedProcessFunctionTopN1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env
                .socketTextStream("localhost", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                )
                .keyBy(WaterSensor::getVc)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                        new VcCountAgg(),
                        new WindowResult()  // 事件时间达到5+3s被调用
                )
                 .keyBy(t -> t.f2)
                 .process(new TopN())  // processElement:来一个处理一个， onTimer：事件时间为windowTime + 1(5001)s时被调用
                 .print();


        env.execute();
    }

    public static class VcCountAgg implements AggregateFunction<WaterSensor, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Integer, Tuple3<Integer,Integer,Long>, Integer, TimeWindow> {

        @Override
        public void process(Integer key, Context context, Iterable<Integer> elements, Collector<Tuple3<Integer,Integer,Long>> out) throws Exception {
            // 迭代器里面只有一条数据，next一次即可
            Integer count = elements.iterator().next();
            long windowEnd = context.window().getEnd();
            out.collect(Tuple3.of(key,count,windowEnd));
        }
    }

    public static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {
        ArrayList<Tuple3<Integer, Integer, Long>> tuple3s = new ArrayList<>();
        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            tuple3s.add(value);
            System.out.println(tuple3s.toString());
            Long time = value.f2;
            ctx.timerService().registerEventTimeTimer(time + 1);

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            tuple3s.sort(new Comparator<Tuple3<Integer, Integer, Long>>() {
                @Override
                public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
                    return -(o1.f1 - o2.f1);
                }
            });

            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < Math.min(2, tuple3s.size()); i++) {
                stringBuilder.append(tuple3s.get(i).toString() + "\n");
            }
            tuple3s.clear();
            out.collect(stringBuilder.toString());
        }
    }
}
