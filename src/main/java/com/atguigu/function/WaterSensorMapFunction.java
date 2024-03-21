package com.atguigu.function;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;


public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {

    @Override
    public WaterSensor map(String s) throws Exception {
        String[] strings = s.split(",");
        return new WaterSensor(strings[0],Long.valueOf(strings[1]),Integer.valueOf(strings[2]));
    }
}
