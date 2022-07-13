package day02.broadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 这个地方做一个 join 的需求实现
 */
public class FlinkBroadCast_Demo01 {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // TODO_MA 马中华 注释： a join b ==> datastream1 join datastream2
        // TODO_MA 马中华 注释： datastream1.map(value => {1、先拿到全量的 datastream2 的数据， 2、拿value到datastream2中去匹配})
        //1：准备需要广播的数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        // TODO_MA 马中华 注释：     name  age
        broadData.add(new Tuple2<>("zs", 18));
        broadData.add(new Tuple2<>("ls", 20));
        broadData.add(new Tuple2<>("ww", 17));
        DataSet<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);

        // TODO_MA 马中华 注释： 处理需要广播的数据,把数据集转换成map类型，map中的key就是用户姓名，value就是用户年龄
        DataSet<HashMap<String, Integer>> toBroadcast = tupleData.map(
                new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
                    @Override
                    public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        HashMap<String, Integer> res = new HashMap<>();
                        res.put(value.f0, value.f1);
                        return res;
                    }
                });

        // TODO_MA 马中华 注释： 源数据
        // TODO_MA 马中华 注释：                           name
        DataSource<String> data = env.fromElements("zs", "ls", "ww");

        // TODO_MA 马中华 注释： dataset1 = data
        // TODO_MA 马中华 注释： dataset2 = toBroadcast

        // TODO_MA 马中华 注释： 用一个 SQL 来表示：
        // TODO_MA 马中华 注释： select name,age from data a join toBroadcast b on a.name = b.name;
        // TODO_MA 马中华 注释： 在 MapReduce 中我们把他叫做 mapjoin
        // TODO_MA 马中华 注释： mapreduce 的 mapjoin 就会通过 广播变量 机制来实现的
        // TODO_MA 马中华 注释： 比较大的价值，提高效率，避免数据倾斜！
        // TODO_MA 马中华 注释： 假设你做 join 发生了数据倾斜，如果是大小表做join 可以考虑优化成 mapjoin
        // TODO_MA 马中华 注释： 优化成 mapjoin 的执行模式，就得依靠 广播变量这个机制 来实现！

        // TODO_MA 马中华 注释： 在这里需要使用到RichMapFunction获取广播变量
        // TODO_MA 马中华 注释： 其实就是实现了一个 data join toBroadcast 的效果
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            List<HashMap<String, Integer>> broadCastMap = new ArrayList<HashMap<String, Integer>>();
            // TODO_MA 马中华 注释： 存储了广播过来的所有数据
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();

            // TODO_MA 马中华 注释： 这个方法只会执行一次,可以在这里实现一些初始化的功能, 所以，就可以在open方法中获取广播变量数据
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //3:获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String value) throws Exception {
                // TODO_MA 马中华 注释： value 是  data 这个 dataset 中的一个元素，其实就是 zs,ls,ww 这些值
                // TODO_MA 马中华 注释： 根据这个 name（zs,ls,ww） 去广播数据集中去匹配
                Integer age = allMap.get(value);
                // TODO_MA 马中华 注释： 输出结果
                return value + "," + age;
            }
        }).withBroadcastSet(toBroadcast, "broadCastMapName");  // TODO_MA 马中华 注释： 执行广播数据的操作

        // TODO_MA 马中华 注释：
        result.print();

//        env.execute("FlinkBroadCast_Demo01");
    }
}
