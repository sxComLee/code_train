package cn.naixue.flume.intercept;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserInterceptor implements Interceptor {

    private Pattern compile;

    /**
     * 初始化调用的方法
     */
    @Override
    public void initialize() {
        compile = Pattern.compile("^[0-9]*$]");

    }

    /**
     * 最终要的拦截方法就在这里，带了一个参数，event就是包装了我们发送过来的数据
     * 单条数据拦截使用这个方法
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String line = new String(body);
        Matcher matcher = compile.matcher(line);
        if(matcher.matches()){
            //就给数据的头加上了一个标识，标识是一个数字
            event.getHeaders().put("type","number");
        }else{
            event.getHeaders().put("type","str");
        }
        //使用正则来匹配是否全部都是数字
        return event;
    }

    /**
     * 批量数据的拦截采用的方法
     * @param list
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public  static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new UserInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


}
