package faceObject.keyword.bean;

import java.io.Serializable;

/**
 * @ClassName UserInfo
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-18 15:35
 * @Version 1.0
 */

public class UserInfo implements Serializable {

    private static final long serialVersionUID = -3097000567941631785L;

    private static String name;
    private static transient String psw;

    public UserInfo(String name,String psw){
        this.name = name;
        this.psw = psw;
    }

    public String toString(){
        return "name="+name+", psw="+psw;
    }

}
