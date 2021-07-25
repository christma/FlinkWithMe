package base;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class UnionSource {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Long> source1 = env.addSource(new MyNoParalleSource());


        DataStreamSource<Long> source2 = env.addSource(new MyNoParalleSource());


        DataStream<Long> union = source1.union(source2);


        SingleOutputStreamOperator<Long> map = union.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("print source :" + value);
                return value;
            }
        });



        map.timeWindowAll(Time.seconds(2)).sum(0).print();


        env.execute("unionSource");

    }

}
