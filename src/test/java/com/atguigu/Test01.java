package com.atguigu;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.fromElements("aa", "{\"common\":\"ba\"}", "cc");
        SingleOutputStreamOperator<String> filter = stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                boolean flag = false;
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    flag = true; // Keep the element if parsing succeeds
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return flag;
            }
        });
        filter.print();
        env.execute();
    }
}
