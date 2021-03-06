维表异步访问函数总体和同步函数实现类似，这里说一下注意点：

1. 外部数据源异步客户端(如RedisAsyncCommands)初始化。如果是线程安全的(多个客户端一起使用)，你可以不加 transient 关键字,初始化一次。否则，你需要加上 transient,不对其进行初始化，而在 open 方法中，为每个 Task 实例初始化一个。

2. eval 方法中多了一个 CompletableFuture,当异步访问完成时，需要调用其方法进行处理.

为了减少每条数据都去访问外部数据系统，提高数据的吞吐量，一般我们会在同步函数和异步函数中加入缓存，如果以前某个关键字访问过外部数据系统，我们将其值放入到缓存中，在缓存没有失效之前，如果该关键字再次进行处理时，直接先访问缓存，有就直接返回，没有再去访问外部数据系统，然后在进行缓存，进一步提升我们实时程序处理的吞吐量。

一般缓存类型有以下几种类型：
数据全部缓存，定时更新。
LRU Cache，设置一个超时时间。
用户自定义缓存。