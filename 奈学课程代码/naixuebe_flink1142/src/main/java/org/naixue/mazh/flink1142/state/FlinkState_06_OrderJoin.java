package org.naixue.mazh.flink1142.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 需求： 将两个流中的 订单号一样的数据拼接到一起进行输出，相当于完成流 join
 */
public class FlinkState_06_OrderJoin {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);

        // TODO_MA 马中华 注释： 模拟两个数据流，每个数据流，都是每隔 0.5s 发送一条数据
        // TODO_MA 马中华 注释： String = 123,拖把,30.0
        DataStreamSource<String> order1DS = environment.addSource(new FileSource(Constants.ORDER_INFO1_PATH));
        // TODO_MA 马中华 注释： String = 123,2021-11-11 10:11:12,江苏
        DataStreamSource<String> order2DS = environment.addSource(new FileSource(Constants.ORDER_INFO2_PATH));

        // TODO_MA 马中华 注释： order1DS 按照 OrderId 分组
        // TODO_MA 马中华 注释： order2DS 按照 OrderId 分组
        KeyedStream<OrderInfo1, Long> keyedOrderInfo1DS = order1DS.map(line -> OrderInfo1.string2OrderInfo1(line))
                .keyBy(orderInfo1 -> orderInfo1.getOrderId());
        KeyedStream<OrderInfo2, Long> keyedOrderInfo2DS = order2DS.map(line -> OrderInfo2.string2OrderInfo2(line))
                .keyBy(orderInfo2 -> orderInfo2.getOrderId());

        // TODO_MA 马中华 注释： keyedOrderInfo1DS 和 keyedOrderInfo2DS 做 connect 动作
        // TODO_MA 马中华 注释： 目的是： 通过 state 实现这两个 数据流的 join 效果
        // TODO_MA 马中华 注释： select a.* , b.* from a join b on a.orderid = b.orderid;
        keyedOrderInfo1DS.connect(keyedOrderInfo2DS).flatMap(new OrderJoinFunction()).print();
        // TODO_MA 马中华 注释： 为什么需要用到状态呢？
        // TODO_MA 马中华 注释： 假设数据流1 接收到了 orderid = 1 的数据记录， 但是 数据流2 的 orderid = 1 还没有收到
        // TODO_MA 马中华 注释： 先接收到的不能完成链接的，则作为 state 存储起来
        // TODO_MA 马中华 注释： 当接收到的数据，能完成链接的时候，先从 state 获取记录，然后完成链接
        // TODO_MA 马中华 注释： DS1[Info1]   DS2[Info2]

        // TODO_MA 马中华 注释：
        environment.execute("FlinkState_OrderJoin");
    }
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 自定义处理逻辑
 */
class OrderJoinFunction extends RichCoFlatMapFunction<OrderInfo1, OrderInfo2, Tuple2<OrderInfo1, OrderInfo2>> {

    // TODO_MA 马中华 注释： 存储两个流中， 未进行 join 的数据
    private ValueState<OrderInfo1> orderInfo1State;
    private ValueState<OrderInfo2> orderInfo2State;

    @Override
    public void open(Configuration parameters) {
        orderInfo1State = getRuntimeContext().getState(new ValueStateDescriptor<OrderInfo1>("info1", OrderInfo1.class));
        orderInfo2State = getRuntimeContext().getState(new ValueStateDescriptor<OrderInfo2>("info2", OrderInfo2.class));
    }

    // TODO_MA 马中华 注释： 处理是左边流，左表的数据
    @Override
    public void flatMap1(OrderInfo1 orderInfo1, Collector<Tuple2<OrderInfo1, OrderInfo2>> collector) throws Exception {
        OrderInfo2 value2 = orderInfo2State.value();
        if (value2 != null) {
            orderInfo2State.clear();
            collector.collect(Tuple2.of(orderInfo1, value2));
        } else {
            orderInfo1State.update(orderInfo1);
        }
    }

    // TODO_MA 马中华 注释： 处理的是右边流，右表的数据
    @Override
    public void flatMap2(OrderInfo2 orderInfo2, Collector<Tuple2<OrderInfo1, OrderInfo2>> collector) throws Exception {
        OrderInfo1 value1 = orderInfo1State.value();
        if (value1 != null) {
            orderInfo1State.clear();
            collector.collect(Tuple2.of(value1, orderInfo2));
        } else {
            orderInfo2State.update(orderInfo2);
        }
    }
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 自定义一个读取文件数据的 Source
 */
class FileSource implements SourceFunction<String> {

    private String filePath;
    private InputStream inputStream;
    private BufferedReader bufferedReader;
    private Random random = new Random();

    FileSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        bufferedReader = new BufferedReader(new FileReader(new File(filePath)));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            // 模拟发送数据
            TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
            // 发送数据
            sourceContext.collect(line);
        }
        if (bufferedReader != null) {
            bufferedReader.close();
        }
        if (inputStream != null) {
            inputStream.close();
        }
    }

    @Override
    public void cancel() {
        try {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            if (inputStream != null) {
                inputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 两个文件，模拟从 Kafka 中接收到的两个 topic 数据流
 */
class Constants {
    public static final String ORDER_INFO1_PATH = "C:\\bigdata-src\\naixuebe_flink1142\\src\\main\\flink_state_input01\\order_info1.txt";
    public static final String ORDER_INFO2_PATH = "C:\\bigdata-src\\naixuebe_flink1142\\src\\main\\flink_state_input01\\order_info2.txt";
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 文件1中读取到的数据对象1
 */
class OrderInfo1 {
    //订单ID
    private Long orderId;
    //商品名称
    private String productName;
    //价格
    private Double price;

    public OrderInfo1() {
    }

    public OrderInfo1(Long orderId, String productName, Double price) {
        this.orderId = orderId;
        this.productName = productName;
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderInfo1{" + "orderId=" + orderId + ", productName='" + productName + '\'' + ", price=" + price + '}';
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    // TODO_MA 马中华 注释： 将从文件中，读取到的一行数据转化成 OrderInfo1
    public static OrderInfo1 string2OrderInfo1(String line) {
        OrderInfo1 orderInfo1 = new OrderInfo1();
        if (line != null && line.length() > 0) {
            String[] fields = line.split(",");
            orderInfo1.setOrderId(Long.parseLong(fields[0]));
            orderInfo1.setProductName(fields[1]);
            orderInfo1.setPrice(Double.parseDouble(fields[2]));
        }
        return orderInfo1;
    }
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 文件2中读取到的数据对象2
 */
class OrderInfo2 {
    //订单ID
    private Long orderId;
    //下单时间
    private String orderDate;
    //下单地址
    private String address;

    public OrderInfo2() {
    }

    public OrderInfo2(Long orderId, String orderDate, String address) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.address = address;
    }

    @Override
    public String toString() {
        return "OrderInfo2{" + "orderId=" + orderId + ", orderDate='" + orderDate + '\'' + ", address='" + address + '\'' + '}';
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    // TODO_MA 马中华 注释： 工具方法，将文件中读取到的一行数据转化成 OrderInfo2
    public static OrderInfo2 string2OrderInfo2(String line) {
        OrderInfo2 orderInfo2 = new OrderInfo2();
        if (line != null && line.length() > 0) {
            String[] fields = line.split(",");
            orderInfo2.setOrderId(Long.parseLong(fields[0]));
            orderInfo2.setOrderDate(fields[1]);
            orderInfo2.setAddress(fields[2]);
        }
        return orderInfo2;
    }
}
