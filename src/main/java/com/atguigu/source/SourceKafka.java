package com.atguigu.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: SourceKafka
 * Package: com.atguigu.source
 * Description:
 *
 * @Author rd_cey
 * @Create 2023/9/12 16:03
 * @Version 1.0
 */
public class SourceKafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("ws")
                .setGroupId("atguigu")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        stream.print("Kafka");

        env.execute();
    }

}
