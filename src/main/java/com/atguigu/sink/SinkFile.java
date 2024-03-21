package com.atguigu.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/**
 * ClassName: SinkFile
 * Package: com.atguigu.sink
 * Description:
 *
 * @Author rd_cey
 * @Create 2023/9/12 15:48
 * @Version 1.0
 */
public class SinkFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        // 从socket端口读取数据
        DataStream<String> dataStream = env.socketTextStream("localhost", 9999);
        // 配置输出目录
        String outputDir = "./output";
        FileSink<String> sink = FileSink
                .forRowFormat(new Path(outputDir), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("atguigu")
                                .withPartSuffix(".log")
                                .build())
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .withRolloverInterval(Duration.ofMillis(60))
                                .build()
                )
                .build();
        // 将数据写入文件
        dataStream.sinkTo(sink).setParallelism(1);
        env.execute("Socket Stream to File");
    }

}
