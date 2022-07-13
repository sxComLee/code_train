package cn.naixue.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

public class UserSource  extends AbstractSource implements Configurable , PollableSource {

    private Long delay;
    private String field;


    /**
     * 核心方法都在这个process里面，所有的获取数据都是在这个方法里面后去的
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        try{

        HashMap<String, String> headerMap = new HashMap<>();

        //自己给自己发送一些数据出去
        //在flume当中，所有的数据都是封装成为一个个的Event对象
        SimpleEvent event = new SimpleEvent();
        for(int i = 0;i <5;i++){
            //设置数据发送的头部信息
            event.setHeaders(headerMap);
            //设置真正发送给channel的数据
            event.setBody((field + i).getBytes());
            getChannelProcessor().processEvent(event);
            //发送完成数据，休眠一下
            Thread.sleep(delay);
        }
        }catch(Exception e ){
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;
    }
    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    /**
     * 配置属性的方法，从外部定义的陪孩子属性，都可以从这里拿到的
     *  a1.sources.r1.hello = world
     * @param context
     */
    @Override
    public void configure(Context context) {
        delay = context.getLong("delay");
        field = context.getString("filed","hello");

    }
}
