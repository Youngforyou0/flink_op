package com.atguigu.sql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class myDefinedFunction {

    //todo: 标量函数测试
    @Test
    public void testHashFunction() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> sensorDS = env
                .fromElements(
                        new WaterSensor("s1", 1L, 1),
                        new WaterSensor("s2", 2L, 2),
                        new WaterSensor("s3", 3L, 3),
                        new WaterSensor("s3", 4L, 4)
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table sensorTable = tableEnv.fromDataStream(sensorDS);
        tableEnv.createTemporaryView("sensor", sensorTable);

        // 注册函数
        tableEnv.createTemporarySystemFunction("HashFunction", HashFunction.class);
        // 在 SQL 里调用注册好的函数
        tableEnv.sqlQuery("select HashFunction(id) from sensor").execute().print();
    }

    //todo: 表值函数测试
    @Test
    public void testSplitFunction() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> strDS = env
                .fromElements(
                        "hello hadoop",
                        "hello flink",
                        "hello spark"
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table strTable = tableEnv.fromDataStream(strDS, $("words"));
        tableEnv.createTemporaryView("str", strTable);

        // 注册函数
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        // 在 SQL 里调用注册好的函数
        tableEnv.sqlQuery("select words,word,length from str,lateral table(SplitFunction(words))").execute().print();

    }

    //todo: 聚合函数测试
    @Test
    public void testWeightedAvg(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, Integer>> tuple2DS = env
                .fromElements(
                        Tuple2.of(80, 4),
                        Tuple2.of(90, 4),
                        Tuple2.of(100, 2)
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table tuple2Table = tableEnv.fromDataStream(tuple2DS);
        tableEnv.createTemporaryView("tuple2",tuple2Table);

        tableEnv.createTemporarySystemFunction("WeightedAvg",WeightedAvg.class);
        tableEnv.sqlQuery("select WeightedAvg(f0,f1) from tuple2").execute().print();
    }

    //todo: 表值聚合函数测试
    @Test
    public void testTop2(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> intDS = env.fromElements(123, 454, 23435, 654, 67, 78, 45, 679, 74, 5, 3, 453);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table intTable = tableEnv.fromDataStream(intDS,$("score"));
        tableEnv.createTemporaryView("ints",intTable);

        tableEnv.createTemporarySystemFunction("Top2",Top2.class);
        // 使用Table API进行查询和处理,在Flink中，目前不支持在SQL中直接使用自定义的TableAggregateFunction。
        intTable
                .groupBy()
                .flatAggregate(
                        call("Top2",$("score")).as("value","rank")
                )
                .select($("value"), $("rank"))
                .execute().print();
    }



    /*todo: 1.标量函数
      自定义标量函数可以把 0 到多个标量值映射成 1 个标量值，数据类型里列出的任何数据类型都可作为求值方法的参数和返回值类型
     */
    public static class HashFunction extends ScalarFunction {
        // 接受任意类型输入，返回 INT 型输出
        public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return o.hashCode();
        }
    }

    /*todo: 2.表值函数
     定义表值函数的输入参数也可以是 0 到多个标量。但是跟标量函数只能返回一个值不同的是，它可以返回任意多行。多行数据实际上就构成了表
     */
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        public void eval(String str) {
            for (String s : str.split(" ")) {
                // use collect(...) to emit a row
                collect(Row.of(s, s.length()));
            }
        }
    }

    /*todo: 3.聚合函数
      自定义聚合函数（UDAGG）是把一个表（一行或者多行，每行可以有一列或者多列）聚合成一个标量值。
     */
    public static class WeightedAvgAccum {
        public long sum = 0;
        public int count = 0;

    }
    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {

        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }

        @Override
        public Long getValue(WeightedAvgAccum acc) {
            if (acc.count == 0) {
                return null;
            } else {
                return acc.sum / acc.count;
            }
        }
        public void accumulate(WeightedAvgAccum acc, long iValue, int iWeight) {
            acc.sum += iValue * iWeight;
            acc.count += iWeight;
        }

        public void retract(WeightedAvgAccum acc, long iValue, int iWeight) {
            acc.sum -= iValue * iWeight;
            acc.count -= iWeight;
        }

        public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
            Iterator<WeightedAvgAccum> iter = it.iterator();
            while (iter.hasNext()) {
                WeightedAvgAccum a = iter.next();
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }
        public void resetAccumulator(WeightedAvgAccum acc) {
            acc.count = 0;
            acc.sum = 0L;
        }
    }

    /*todo: 4.表值聚合函数
      自定义表值聚合函数（UDTAGG）可以把一个表（一行或者多行，每行有一列或者多列）聚合成另一张表，结果中可以有多行多列。
     */
    public static class Top2Accum {
        public Integer first;
        public Integer second;
    }

    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accum> {
        @Override
        public Top2Accum createAccumulator() {
            Top2Accum acc = new Top2Accum();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }
        public void accumulate(Top2Accum acc, Integer v) {
            if (v > acc.first) {
                acc.second = acc.first;
                acc.first = v;
            } else if (v > acc.second) {
                acc.second = v;
            }
        }

        public void emitValue(Top2Accum acc, Collector<Tuple2<Integer, Integer>> out) {
            // emit the value and rank
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }
}


