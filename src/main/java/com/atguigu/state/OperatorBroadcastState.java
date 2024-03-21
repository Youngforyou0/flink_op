package com.atguigu.state;


import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class OperatorBroadcastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());

        DataStreamSource<String> configDS = env.socketTextStream("hadoop102", 8888);

        //todo: 1.配置广播流
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<String> configBS = configDS.broadcast(broadcastMapState);

        //todo: 2.connect数据流和广播流
        BroadcastConnectedStream<WaterSensor, String> sensorDBS = sensorDS.connect(configBS);

        //todo: 3.调用process
        sensorDBS
                .process(new BroadcastProcessFunction<WaterSensor, String, String>() {
                    @Override
                    public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        //todo: 5.通过上下文获取广播状态，取出里面的值
                        ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                        Integer threshold = broadcastState.get("threshold") == null ? 0 : broadcastState.get("threshold");
                        if (value.getVc() >= threshold) {
                            out.collect(value + ",水位超过指定阈值：" + threshold + "!!!");
                        }

                    }

                    @Override
                    public void processBroadcastElement(String value, BroadcastProcessFunction<WaterSensor, String, String>.Context ctx, Collector<String> out) throws Exception {
                        //todo: 4.通过上下文获取广播状态，往里写数据
                        BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                        broadcastState.put("threshold", Integer.valueOf(value));
                    }
                })
                .print();
        env.execute();
    }
}
