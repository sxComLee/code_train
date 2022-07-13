package udf;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.spec.InvalidKeySpecException;

/**
 * @author sobranie
 */
public class DESTextCipher implements TextCipher {

    /**
     * 加密
     */
    private Cipher encryptCipher;

    /**
     * 解密
     */
    private Cipher decryptCipher;

    /**
     * KeyFactory
     */
    private SecretKeyFactory keyFactory;

    public DESTextCipher() {
        try {
            encryptCipher = Cipher.getInstance("DES");
            decryptCipher = Cipher.getInstance("DES");
            keyFactory = SecretKeyFactory.getInstance("DES");
        } catch (GeneralSecurityException ex) {
            ex.printStackTrace();
        }

        init("CreditCloudRock!");
//        init("helloword!"); //javax.crypto.BadPaddingException: Given final block not properly padded
    }

    @Override
    public void init(String salt) {
        try {
            SecretKey sk = keyFactory.generateSecret(new DESKeySpec(salt.getBytes()));
            encryptCipher.init(Cipher.ENCRYPT_MODE, sk);
            decryptCipher.init(Cipher.DECRYPT_MODE, sk);
        } catch (InvalidKeyException | InvalidKeySpecException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public String encrypt(String value) throws GeneralSecurityException {
        return new String(Base64.encodeBase64(encryptCipher.doFinal(value.getBytes())));
    }

    @Override
    public String decrypt(String value) throws GeneralSecurityException {
        return new String(decryptCipher.doFinal(Base64.decodeBase64(value.getBytes())));
    }

    public static void main(String[] args) throws GeneralSecurityException {
        TextCipher cipher = new DESTextCipher();

        System.out.println(cipher.decrypt("/2crdgG5MdV7YG65Rttoaw=="));
//        System.out.println(cipher.encrypt("51310119710505052X"));
        System.out.println(cipher.encrypt("13901322467"));

        //用户的密码无法解密,因为数据库保存的好像是md5摘要串,
//        System.out.println("------>"+cipher.decrypt("c6abbb347f39178d5ab8586fd6e66a3f3fed6ebf"));
    }
}