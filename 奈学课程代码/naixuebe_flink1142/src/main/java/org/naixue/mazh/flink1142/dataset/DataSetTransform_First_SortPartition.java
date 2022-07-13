package org.naixue.mazh.flink1142.dataset;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释：
 *  面试题： 假设有 1000T 的数据，现在想求解其中最大的 50 个值
 *  具体的方案：
 *  1、切分：  1000T 的数据 = 1000 * 1024个 Task, 每个 Task 就是 1G 的数据
 *  2、分区排序： 每个 Task 内的所有数据，做一个降序排序，取得前面的 50 个  ==== 维护一个只存储了 50 个元素的小根堆
 *     1024000 个 Task, 每个 Task 取了最大的 50 条数据，然后得到 51200000 条数据
 *  3、合并起来求最大的 50 条
 *  -
 *  每个班级，总分最高的 3 个同学！ = TopN
 *  1、先分区排序
 *  2、分区求最值
 */
public class DataSetTransform_First_SortPartition {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(2, "zs"));
        data.add(new Tuple2<>(4, "ls"));
        data.add(new Tuple2<>(3, "ww"));
        data.add(new Tuple2<>(1, "xw"));
        data.add(new Tuple2<>(1, "aw"));
        data.add(new Tuple2<>(1, "mw"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        //获取前3条数据，按照数据插入的顺序
        text.first(3).print();
        System.out.println("==============================");

        //根据数据中的第一列进行分组，获取每组的前2个元素
        text.groupBy(0).first(2).print();
        System.out.println("==============================");

        //根据数据中的第一列分组，再根据第二列进行组内排序[升序]，获取每组的前2个元素
        text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
        System.out.println("==============================");
        // TODO_MA 马中华 注释： select * from table order by score limit 2;
        // TODO_MA 马中华 注释： Hive  clickhoues:   窗口分析函数来做

        //不分组，全局排序获取集合中的前3个元素，针对第一个元素升序，第二个元素倒序
        text.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.DESCENDING).first(3).print();
    }
}
