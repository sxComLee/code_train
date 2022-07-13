package proxy.statics;

/**
 * @ClassName DeveloperProxy
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-01-15 16:43
 * @Version 1.0
 */
public class DeveloperProxy implements IDeveloper {

    private IDeveloper developer;
    public DeveloperProxy(IDeveloper developer){
        this.developer = developer;
    }

    @Override
    public void writeCode() {
        System.out.println("Write documentation...");
        this.developer.writeCode();
    }
}
