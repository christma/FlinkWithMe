package base;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

public class ConnectionSource {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source1 = env.addSource(new MyNoParalleSource()).setParallelism(1);
        DataStreamSource<Long> source2 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<String> sourceMap = source2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                System.out.println("添加前缀 str_ " + value);
                return "str_" + value;
            }
        });


        SingleOutputStreamOperator<Object> operator = source1.connect(sourceMap).map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long aLong) throws Exception {
                return aLong;
            }

            @Override
            public Object map2(String s) throws Exception {
                return s;
            }
        });


        operator.print();


        env.execute("ConnectionSource");
    }
}
