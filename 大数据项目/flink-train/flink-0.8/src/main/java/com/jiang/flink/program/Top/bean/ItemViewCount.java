package com.jiang.flink.program.Top.bean;

/**
 * @ClassName ItemViewCount
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-25 14:55
 * @Version 1.0
 */
public class ItemViewCount {
    public long itemId;     // 商品ID
    public long windowEnd;  // 窗口结束时间戳
    public long viewCount;  // 商品的点击量

    public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.itemId = itemId;
        result.windowEnd = windowEnd;
        result.viewCount = viewCount;
        return result;
    }

}
