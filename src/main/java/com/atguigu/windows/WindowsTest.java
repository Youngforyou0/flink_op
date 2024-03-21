package com.atguigu.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * ClassName: WindowsTest
 * Package: com.atguigu.windows
 * Description:
 *
 * @Author rd_cey
 * @Create 2023/9/13 9:12
 * @Version 1.0
 */
public class WindowsTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Integer> map = stream.map(e -> Integer.parseInt(e));
        KeyedStream<Integer, String> keyBy = map.keyBy(e -> e % 2 == 0 ? "偶数" : "奇数");

        // map.windowAll();

        // todo 1.指定窗口分配器  时间 or 计算， 滚动 滑动 会话 自定义
        // todo 1.1 基于时间
        // keyBy.window(TumblingProcessingTimeWindows.of(Time.milliseconds(10)))  // 滚动窗口
        // keyBy.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2)))  // 滑动窗口
        // keyBy.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))  // 会话窗口

        // todo 1.2 基于计数
        // keyBy.countWindow(5) // 滚动窗口
        // keyBy.countWindow(5,2) // 滑动窗口
        // keyBy.window(GlobalWindows.create())  // 全局窗口，窗口的底层，需要自定义一个触发器

        // todo 2.指定窗口函数
        // todo 2.1 增量聚合：reduce,aggregate
//         WindowedStream<Integer, String, TimeWindow> timeTumbWindowStream = keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        // SingleOutputStreamOperator<Integer> reduce = timeTumbWindowStream.reduce((e1,e2) -> (e1 + e2));
        // reduce.print();
//         SingleOutputStreamOperator<String> aggregate = timeTumbWindowStream.aggregate(new AggregateFunction<Integer, Integer, String>() {
//             @Override
//             public Integer createAccumulator() {
//                 return 0;
//             }
//             @Override
//             public Integer add(Integer integer, Integer integer2) {
//                 return integer + integer2;
//             }
//             @Override
//             public String getResult(Integer integer) {
//                 return integer.toString();
//             }
//             @Override
//             public Integer merge(Integer integer, Integer acc1) {
//                 // 只有会话窗口能用到
//                 return null;
//             }
//         });
//        aggregate.print();

        // todo 2.2 全量聚合：process
//         WindowedStream<Integer, String, TimeWindow> timeTumbWindowStream = keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
//         SingleOutputStreamOperator<String> process = timeTumbWindowStream.process(new ProcessWindowFunction<Integer, String, String, TimeWindow>() {
//             /**
//              *
//              * @param s The key for which this window is evaluated.
//              * @param context The context in which the window is being evaluated.
//              * @param elements The elements in the window being evaluated.
//              * @param out A collector for emitting elements.
//              * @throws Exception
//              */
//             @Override
//             public void process(String s, ProcessWindowFunction<Integer, String, String, TimeWindow>.Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
//                 long start = context.window().getStart();
//                 long end = context.window().getEnd();
//                 SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//                 String windowStart = dateFormat.format(start);
//                 String windowEnd = dateFormat.format(end);
//                 long count = elements.spliterator().estimateSize();
//                 out.collect("key：" + s + "[(" + windowStart + "-" + windowEnd + ")包含" + count + "条数据===>" + elements.toString() + "]");
//             }
//         });
//         process.print();

//        // todo 2.3 增量与全窗口结合使用
        WindowedStream<Integer, String, TimeWindow> timeTumbWindowStream = keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        SingleOutputStreamOperator<String> reduceProcess = timeTumbWindowStream.reduce((e1, e2) -> (e1 + e2), new ProcessWindowFunction<Integer, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<Integer, String, String, TimeWindow>.Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                out.collect("key：" + s + elements.toString());
            }
        });
        reduceProcess.print();

        env.execute();
    }
}
