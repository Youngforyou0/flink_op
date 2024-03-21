package com.atguigu.state;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env
                .socketTextStream("localhost", 7777)
                .map(new StringCountFunction())
                .print();
        env.execute();
    }
    public static class StringCountFunction extends RichMapFunction<String, Long> implements CheckpointedFunction {
        private transient ListState<Long> countState;
        private long count;

        @Override
        public Long map(String value) throws Exception {
            // 对输入字符串进行计数
            count++;
            return count;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 在快照时保存计数状态
            countState.clear();
            countState.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState...");
            countState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("countState", Long.class));
            if (context.isRestored()) {
                for (Long c : countState.get()) {
                    count += c;
                }
            }
        }
    }
}
