package com.jiang.flink.program.recommandSystemDemo.map;

import com.jiang.flink.program.recommandSystemDemo.client.HbaseClient;
import com.jiang.flink.program.recommandSystemDemo.domain.LogEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * @ClassName UserHistoryWithInterestMapFunction
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-10 13:43
 * @Version 1.0
 */
public class UserHistoryWithInterestMapFunction extends RichMapFunction<LogEntity,String> {

    ValueState<Action> state;
    @Override
    public void open(Configuration parameters) throws Exception {
        //设置 state的过期时间100s
        StateTtlConfig ttl = StateTtlConfig
                .newBuilder(Time.seconds(100L))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<Action> desc = new ValueStateDescriptor<>("Action time", Action.class);
        desc.enableTimeToLive(ttl);
        state = getRuntimeContext().getState(desc);
    }

    @Override
    public String map(LogEntity value) throws Exception {
        Action actionLastTime = state.value();
        Action action = new Action(value.getAction(), value.getTime().toString());
        int times =1;
        if(null == actionLastTime){
            actionLastTime = action;
        }else{
            times = getTimesByRule(actionLastTime, action);
        }
        saveToHbase(value, times);

        // 如果用户的操作为3(购物),则清除这个key的state
        if(action.getType().equals("3")){
            state.clear();
        }
        return null;
    }
    
    /**
     * @Author jiang.li
     * @Description //TODO 判断是不是一次兴趣事件
     * @Date 14:09 2020-01-10
     * @Param [actionLastTime, actionThisTime]
     * @return int
     **/
    private int getTimesByRule(Action actionLastTime,Action actionThisTime){
        // 动作主要有3种类型
        // 1 -> 浏览  2 -> 分享  3 -> 购物
        int a1 = Integer.parseInt(actionLastTime.getType());
        int a2 = Integer.parseInt(actionThisTime.getType());
        int t1 = Integer.parseInt(actionLastTime.getTime());
        int t2 = Integer.parseInt(actionThisTime.getTime());
        int pluse = 1;
        // 如果动作连续发生且时间很短(小于100秒内完成动作), 则标注为用户对此产品兴趣度很高
        if (a2 > a1 && (t2 - t1) < 100_000L){
            pluse *= a2 - a1;
        }
        return pluse;
    }

    /**
     * @Author jiang.li
     * @Description //TODO 将数据保存到hbase中
     * @Date 14:02 2020-01-10
     * @Param [log, times]
     * @return void
     **/
    private void saveToHbase(LogEntity log,int times) throws Exception{
        if(null != log){
            for (int i = 0; i <times ; i++) {
                HbaseClient.increamColumn("u_interest", ""+log.getUserId(), "p", log.getAction());
            }
        }
    }

}


/**
 * 动作类 记录动作类型和动作发生时间(Event Time)
 */
@Data
@AllArgsConstructor
class Action implements Serializable {
    private String type;
    private String time;

}
