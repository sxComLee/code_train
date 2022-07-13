package faceObject.keyword;

import faceObject.keyword.bean.UserInfo;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @ClassName TestTransient
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-18 15:34
 * @Version 1.0
 */
public class TestTransient {

    public static void main(String[] args) throws Exception {
        UserInfo info = new UserInfo("张三", "123456");
        System.out.println(info);

        ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream("UserInfo.txt"));

        stream.writeObject(info);
        stream.close();

        ObjectInputStream in = new ObjectInputStream(new FileInputStream("UserInfo.txt"));
        UserInfo userInfo = (UserInfo) in.readObject();
        System.out.println(userInfo);
    }
}



