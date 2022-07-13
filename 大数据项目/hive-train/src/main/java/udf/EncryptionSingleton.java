package udf;

import com.fengjr.locksmith.storage.key.Encryption;

/**
 * Created by jialiang.li@fengjr.com on 2016/8/23.
 */
public class EncryptionSingleton {

    private static volatile Encryption singleton = null;

    public static Encryption getInstance() {
        if (singleton == null) {
            synchronized (Encryption.class) {
                if (singleton == null) {
                    singleton = new Encryption();
                }
            }
        }
        return singleton;
    }


    private EncryptionSingleton() {
    }

    public static void main(String[] args) {
        Encryption es = EncryptionSingleton.getInstance();
        String appId = es.getSlefAPPID();
        String encryptResult = es.encrypt(appId, null, "asss");
        System.out.println(encryptResult);
    }
}
