package base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountWithMethodAndParm {
    public static class WordAndCount {

        private String Word;

        private Integer Count;

        public WordAndCount(String word, Integer count) {
            Word = word;
            Count = count;
        }

        public WordAndCount() {

        }

        public String getWord() {
            return Word;
        }

        public void setWord(String word) {
            Word = word;
        }

        public Integer getCount() {
            return Count;
        }

        public void setCount(Integer count) {
            Count = count;
        }

        @Override
        public String toString() {
            return "WordAndCount{" +
                    "Word='" + Word + '\'' +
                    ", Count=" + Count +
                    '}';
        }
    }

    public static class splitMethod implements FlatMapFunction<String, WordAndCount> {


        @Override
        public void flatMap(String line, Collector<WordAndCount> out) throws Exception {
            String[] split = line.split(",");
            for (String word : split) {
                out.collect(new WordAndCount(word, 1));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool tool = ParameterTool.fromArgs(args);
        String hostname = tool.get("hostname");
        int port = tool.getInt("port");
        DataStreamSource<String> socketSource = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<WordAndCount> sum =
                socketSource.flatMap(new splitMethod())
                        .keyBy("Word")
                        .sum("Count");


        sum.print();
        env.execute("hello");
    }
}
