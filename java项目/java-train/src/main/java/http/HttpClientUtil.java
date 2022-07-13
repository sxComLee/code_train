package http;


import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: meichao
 * Date: 2015/6/18
 * Time: 15:43
 * Desc:
 * To change this template use File | Settings | File Templates.
 */
public class HttpClientUtil {

    /**
     * @param url
     * @param keyValueMap
     * @param charSet
     * @return
     */

    public static String executeTradePost(String url, Map<String, String> keyValueMap, String charSet) {
        HttpClient client = new HttpClient();
        PostMethod postMethod = new PostMethod(url);
        try {
            //设置请求参数
            if (keyValueMap != null) {
                Iterator it = keyValueMap.entrySet().iterator();
                NameValuePair[] parameters = new NameValuePair[keyValueMap.size()];
                int c = 0;
                while (it.hasNext()) {
                    Map.Entry<String, String> entry = (Map.Entry<String, String>) it.next();
                    NameValuePair nvp = new NameValuePair();
                    nvp.setName(entry.getKey());
                    nvp.setValue(entry.getValue());
                    parameters[c] = nvp;
                    c++;
                }
                postMethod.addParameters(parameters);
            }
            //log.debug("query uri ===============" + postMethod.getURI());
            postMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
            postMethod.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, charSet);
            postMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 10000);
            int statusCode = client.executeMethod(postMethod);
            if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_BAD_REQUEST) {
                //log.info("request '" + url + "' failed,the status is not 200,status:" + statusCode);
                return "";
            }
            String responseBody = postMethod.getResponseBodyAsString();
            return responseBody;
        } catch (Exception e) {
            //log.error("executeHttpRequest发生异常！请检查网络和参数,url:" + url, e);
        } finally {
            postMethod.releaseConnection();
        }
        return null;

    }
    /**
     * @param url
     * @param keyValueMap
     * @param charSet
     * @return
     */
    public static String executeHttpRequest(String url, Map<String, String> keyValueMap, String charSet) {
        HttpClient client = new HttpClient();
        PostMethod postMethod = new PostMethod(url);
        try {
            //设置请求参数
            if (keyValueMap != null) {
                Iterator it = keyValueMap.entrySet().iterator();
                NameValuePair[] parameters = new NameValuePair[keyValueMap.size()];
                int c = 0;
                while (it.hasNext()) {
                    Map.Entry<String, String> entry = (Map.Entry<String, String>) it.next();
                    NameValuePair nvp = new NameValuePair();
                    nvp.setName(entry.getKey());
                    nvp.setValue(entry.getValue());
                    parameters[c] = nvp;
                    c++;
                }
                postMethod.addParameters(parameters);
            }
            //log.debug("query uri ===============" + postMethod.getURI());
            postMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
            postMethod.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, charSet);
            postMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 10000);
            int statusCode = client.executeMethod(postMethod);
            if (statusCode != HttpStatus.SC_OK) {
                //log.info("request '" + url + "' failed,the status is not 200,status:" + statusCode);
                return "";
            }
            String responseBody = postMethod.getResponseBodyAsString();
            return responseBody;
        } catch (Exception e) {
            //log.error("executeHttpRequest发生异常！请检查网络和参数,url:" + url, e);
        } finally {
            postMethod.releaseConnection();
        }
        return null;

    }


    private static PostMethod getPost(String url, String data) throws UnsupportedEncodingException {
        PostMethod method = new PostMethod(url);
        method.setRequestEntity(new StringRequestEntity(data, "application/json", "utf-8"));
//		String token=TokenCheck.getToken();
//		if(token!=null){
//			method.addRequestHeader(TokenCheck.TOKENHEAD, token);
//		}
        return method;
    }


    public static String callRemote(String url, String jsonData) {

        HttpClient client = new HttpClient();

        //log.info("请求参数:" + jsonData);
        PostMethod method = null;
        String result = null;
        try {

            method = getPost(url, jsonData);
            
            HttpConnectionManagerParams managerParams = client.getHttpConnectionManager().getParams(); 
            // 设置连接超时时间(单位毫秒) 
            managerParams.setConnectionTimeout(30000); 
            // 设置读数据超时时间(单位毫秒) 
            managerParams.setSoTimeout(300000);
            client.executeMethod(method);

            InputStream responseBodyAsStream = method.getResponseBodyAsStream();
            result = IOUtils.toString(responseBodyAsStream, "utf-8");
            //log.info("远程返回字符串::" + result);
            IOUtils.closeQuietly(responseBodyAsStream);
        } catch (Exception e) {
            //log.error("远程请求异常url::" + url, e);
            throw new RuntimeException("远程请求异常url::" + url);
        }
        return result;
    }

    public static String callRemoteByGet(String url) {
        if (StringUtils.isEmpty(url)) {
            return null;
        }
        HttpClient httpclient = new HttpClient();

        GetMethod method = new GetMethod(url);

        String result = null;
        int statusCode = 200;

        try {
            statusCode = httpclient.executeMethod(method);

            if (HttpStatus.SC_OK == statusCode) {
                byte[] rsbyte = method.getResponseBody();
                if (rsbyte != null) {
                    result = new String(rsbyte, "UTF-8");
                    //log.info("调用成功，url: " + url + "，远程返回字符串: " + result);
                }
            } else {
                //log.info("调用异常，url: " + url + "，返回码：" + statusCode);
            }

        } catch (Exception e) {
            //log.error("Http Client invoke error, method:getObjByURL, statusCode:" + statusCode, e);
        }

        return result;
    }

    
	public static String doGet(String link, String charset) {
		try {
			URL url = new URL(link);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("User-Agent", "Mozilla/5.0");
			BufferedInputStream in = new BufferedInputStream(conn.getInputStream());
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			byte[] buf = new byte[1024];
			for (int i = 0; (i = in.read(buf)) > 0;) {
				out.write(buf, 0, i);
			}
			out.flush();
			String s = new String(out.toByteArray(), charset);
			return s;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
}
