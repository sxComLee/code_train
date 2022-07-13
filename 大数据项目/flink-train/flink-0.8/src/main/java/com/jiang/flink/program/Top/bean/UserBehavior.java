package com.jiang.flink.program.Top.bean;

/**
 * @ClassName UserBehavior
 * @Description TODO 用户行为数据结构
 * @Author jiang.li
 * @Date 2019-11-25 14:57
 * @Version 1.0
 */
public class UserBehavior {
    public long userId;         // 用户ID
    public long itemId;         // 商品ID
    public int categoryId;      // 商品类目ID
    public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
    public long timestamp;      // 行为发生的时间戳，单位秒

}
