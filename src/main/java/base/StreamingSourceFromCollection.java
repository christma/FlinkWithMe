package base;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class StreamingSourceFromCollection {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        ArrayList<String> data = new ArrayList<>();

        data.add("spark");
        data.add("hadoop");
        data.add("flink");


        DataStreamSource<String> source = env.fromCollection(data);

        SingleOutputStreamOperator<String> map = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String line) throws Exception {
                return "nx_" + line;
            }
        });


        map.print();


        env.execute("StreamingSourceFromCollection");

    }
}
