//package com.jiang.flink.study.source;
//
//import org.apache.flink.table.api.TableSchema;
//import org.apache.flink.table.functions.AsyncTableFunction;
//import org.apache.flink.table.functions.FunctionContext;
//import org.apache.flink.table.functions.TableFunction;
//import org.apache.flink.table.sources.LookupableTableSource;
//import org.apache.flink.types.Row;
//
///**
// * @ClassName LookupableTableSourceTest
// * @Description TODO
// * @Author jiang.li
// * @Date 2019-12-12 15:46
// * @Version 1.0
// */
//public class LookupableTableSourceTest implements LookupableTableSource<String> {
//
//    //返回一个同步访问外部数据系统的函数，
//    // 什么意思呢，就是你通过 Key 去查询外部数据库，
//    // 需要等到返回数据后才继续处理数据，这会对系统处理的吞吐率有影响。
//    @Override
//    public TableFunction<String> getLookupFunction(String[] strings) {
//        return null;
//    }
//
//    //返回一个异步的函数，异步访问外部数据系统，获取数据，这能极大的提升系统吞吐率。
//    @Override
//    public AsyncTableFunction<String> getAsyncLookupFunction(String[] strings) {
//        return null;
//    }
//
//    //主要表示该表是否支持异步访问外部数据源获取数据，
//    // 当返回 true 时，那么在注册到 TableEnvironment 后，使用时会返回异步函数进行调用，
//    // 当返回 false 时，则使同步访问函数
//    @Override
//    public boolean isAsyncEnabled() {
//        return false;
//    }
//
//    @Override
//    public TableSchema getTableSchema() {
//        return null;
//    }
//
//    // 同步访问
//    public static class MyLookupFunction extends TableFunction<Row> {
//        //异步外部数据源的client要在类中定义为 transient,然后在 open 方法中进行初始化，
//        // 这样每个任务实例都会有一个外部数据源的 client
//        //  防止同一个 client 多个任务实例调用，出现线程不安全情况
//        Jedis jedis;
//
//        //open 方法在进行初始化算子实例的进行调用，
//        @Override
//        public void open(FunctionContext context) throws Exception {
//            jedis = new Jedis("");
//        }
//
//        //eval 则是 TableFunction 最重要的方法，它用于关联外部数据。
//        // 当程序有一个输入元素时，就会调用eval一次，用户可以将产生的数据使用 collect() 进行发送下游。
//        //paramas 的值为用户输入元素的值，
//        // 比如在 Join 的时候，使用 A.id = B.id and A.name = b.name,
//        // B 是维表，A 是用户数据表，paramas 则代表 A.id,A.name 的值
//        public void eval(Object... paramas) {
//            String key = "userInfo:userId:" + paramas[0].toString() + ":userName";
//            String value = jedis.get(key);
//            collect(Row.of(key, value));
//
//        }
//    }
//
//
//    public class MyAsyncLookupFunction extends AsyncTableFunction<Row> {
//
//        private transient RedisAsyncCommands<String, String> async;
//
//        @Override
//
//        public void open(FunctionContext context) throws Exception {
//            RedisClient redisClient = RedisClient.create("redis://172.16.44.28:6379/0");
//            StatefulRedisConnection<String, String> connection = redisClient.connect();
//            async = connection.async();
//        }
//
//        public void eval(CompletableFuture<Collection<Row>> future, Object... params) {
//            redisFuture.thenAccept(new Consumer<String>() {
//                @Override
//                public void accept(String value) {
//                    future.complete(Collections.singletonList(Row.of(key, value)));
//                }
//
//            });
//        }
//    }
//}
