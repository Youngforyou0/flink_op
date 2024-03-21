package com.atguigu.process;


import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class KeyedProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        SingleOutputStreamOperator<String> process = env
                .socketTextStream("localhost", 7777)
                .keyBy((KeySelector<String, String>) value -> {
                    if (value.startsWith("a")) {
                        return "a";
                    } else if (value.startsWith("b")) {
                        return "b";
                    } else {
                        return "s";
                    }
                })
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, KeyedProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                        String currentKey = ctx.getCurrentKey();
                        System.out.println("key:" + currentKey + "--->" +value);
                        out.collect(value);
                    }
                });
        env.execute();
    }
}
