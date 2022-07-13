/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package udf;

import com.fengjr.locksmith.api.bean.LocksmithResultBean;
import com.fengjr.locksmith.storage.key.Encryption;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * EdwSecret.
 *
 */
public class EdwAssetsDecrypt extends UDF {

    String appId = "e2b567001503";

    public EdwAssetsDecrypt() {
    }


    public String evaluate(String value) {
        if (value == null || "".equals(value)) {
            return null;
        }
        Encryption es = EncryptionSingleton.getInstance();
        LocksmithResultBean decryptResult = es.decrypt(appId, null, value);
        if (decryptResult.isSuccess()) {
            return decryptResult.getResult().toString();
        } else {
            return decryptResult.getCode() + ":" + decryptResult.getMsg();
        }
    }

    public String evaluate(String value, String salt) {
        if (value == null || "".equals(value)) {
            return null;
        }
        Encryption es = EncryptionSingleton.getInstance();
        LocksmithResultBean decryptResult = es.decrypt(appId, salt, value);//解密
        if (decryptResult.isSuccess()) {
            return decryptResult.getResult().toString();
        } else {
            return decryptResult.getCode() + ":" + decryptResult.getMsg();
        }
    }


}
