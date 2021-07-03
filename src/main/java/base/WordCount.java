package base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = input.split(",");
                for (String world : split) {
                    collector.collect(Tuple2.of(world, 1));
                }
            }
        }).keyBy(0).sum(1);


        sum.print();
        env.execute("hello");
    }
}
