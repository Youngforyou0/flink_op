package com.atguigu.transform.split;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.AnnotationIntrospector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutPut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 7777);

        /**
         * todo: 创建OutputTag对象
         * 第一个参数：标签名
         * 第二个参数：放入侧输出流的数据类型，必须为Typeinformation
         */
        OutputTag<String> aTag = new OutputTag<>("a", Types.STRING);
        OutputTag<String> bTag = new OutputTag<>("b", Types.STRING);

        SingleOutputStreamOperator<String> process = socketStream
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                        if (value.startsWith("a")) {
                            /**
                             * todo: 上下文调用output
                             * 参数一：Tag对象
                             * 参数二：放入侧输出流的数据
                             */
                            ctx.output(aTag, value);
                        } else if (value.startsWith("b")) {
                            ctx.output(bTag, value);
                        } else {
                            out.collect(value);
                        }
                    }
                });
        SideOutputDataStream<String> aStream = process.getSideOutput(aTag);
        SideOutputDataStream<String> bStream = process.getSideOutput(bTag);

        process.print("main");
        aStream.print("aTag");
        bStream.print("bTag");

        env.execute();
    }
}
