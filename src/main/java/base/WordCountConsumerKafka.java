package base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class WordCountConsumerKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        String topic = "nxflink";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1");
        properties.setProperty("group.id", "flink_consumer");


        FlinkKafkaConsumer011<String> source = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties);
        DataStreamSource<String> datas = env.addSource(source).setParallelism(3);


        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = datas.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String lines, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] line = lines.split(",");
                for (String word : line) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(0).sum(1);

        sum.print();


        env.execute("Flink Consumer Kafka");
    }
}
