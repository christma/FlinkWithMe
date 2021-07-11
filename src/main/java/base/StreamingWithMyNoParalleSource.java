package base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamingWithMyNoParalleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);


        DataStreamSource<Long> longDataStreamSource = env.addSource(new MyNoParalleSource());


        SingleOutputStreamOperator<Long> map = longDataStreamSource.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {

                System.out.println("接受数据 : " + aLong);

                return aLong;
            }
        });


        SingleOutputStreamOperator<Long> flatMap = map.flatMap(new FlatMapFunction<Long, Long>() {
            @Override
            public void flatMap(Long aLong, Collector<Long> collector) throws Exception {
                if (aLong % 2 == 0) {
                    collector.collect(aLong);
                }
            }
        });


        flatMap.print();


        env.execute("StreamingWithMyNoParalleSource");
    }
}
