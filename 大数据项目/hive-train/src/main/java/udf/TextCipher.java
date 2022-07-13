package udf;

import java.security.GeneralSecurityException;

/**
 * 加密解密Text类型的数据
 *
 * @author sobranie
 */
public interface TextCipher {

    /**
     * 初始化
     *
     * @param salt
     */
    public void init(String salt);

    /**
     * 加密信息
     *
     * @param value
     * @return
     * @throws GeneralSecurityException
     */
    public String encrypt(String value) throws GeneralSecurityException;

    /**
     * 解密信息
     *
     * @param value
     * @return
     * @throws GeneralSecurityException
     */
    public String decrypt(String value) throws GeneralSecurityException;
}