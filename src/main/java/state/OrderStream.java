package state;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static state.OrderInfo1.string2OrderInfo;
import static state.OrderInfo2.string2OrderInfo2;

public class OrderStream {

  public static void main(String[] args) throws Exception {
    //

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<String> info1 = env.addSource(new FileSource(Constants.ORDER_INFO1_PATH));

    DataStreamSource<String> info2 = env.addSource(new FileSource(Constants.ORDER_INFO2_PATH));

    KeyedStream<OrderInfo1, Long> orderInfo1Stream =
        info1.map(line -> string2OrderInfo(line)).keyBy(orderInfo1 -> orderInfo1.getOrderId());

    KeyedStream<OrderInfo2, Long> orderInfo2Stream =
        info2.map(line -> string2OrderInfo2(line)).keyBy(orderInfo2 -> orderInfo2.getOrderId());


      orderInfo1Stream.connect(orderInfo2Stream).flatMap(new EnrichmentFunction()).print();


    env.execute("OrderStream");
  }
}
