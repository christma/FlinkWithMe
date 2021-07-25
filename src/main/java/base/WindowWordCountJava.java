package base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WindowWordCountJava {
    public static void main(String[] args) throws Exception {

        ParameterTool too = ParameterTool.fromArgs(args);
        int port = too.getInt("port");
        String host = too.get("hostname");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream(host, port);


        SingleOutputStreamOperator<WordAndCount> sum = source.flatMap(new FlatMapFunction<String, WordAndCount>() {

            @Override
            public void flatMap(String line, Collector<WordAndCount> coll) throws Exception {
                String[] fields = line.split("\t");


                for (String word : fields) {
                    coll.collect(new WordAndCount(word, 1));
                }
            }
        }).keyBy("world").sum("count");


        sum.print().setParallelism(1);


        env.execute("WindowWordCountJava");


    }

    public static class WordAndCount {
        public String world;
        public int count;


        public WordAndCount() {

        }

        public WordAndCount(String world, int count) {
            this.world = world;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordAndCount{" +
                    "world='" + world + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
