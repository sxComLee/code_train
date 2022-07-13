package proxy.statics;

/**
 * @ClassName Developer
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-15 16:21
 * @Version 1.0
 */
public class Developer implements IDeveloper {

    private String name;

    public Developer(String name){
        this.name = name;
    }

    @Override
    public void writeCode() {
        System.out.println("Developer " + name + " writes code");
    }
}
