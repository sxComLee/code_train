package udf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IdCard extends UDF {
    private String provinces;
    private String birthday;
    private String gender;
    private String id;
    private String age;
    private String bean;
    private static ConcurrentHashMap<String, String> map = null;

    public IdCard() {
        this.provinces = provinces;
        this.birthday = birthday;
        this.gender = gender;
        this.id = id;
        this.age = age;
        this.bean = bean;
    }

    static {
        loadIDCardLocation();
    }

    private static void loadIDCardLocation() {
        BufferedReader br = null;
        try {
            String uri = "hdfs://feng-cluster/udf/ip_id/id/";
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(uri));
            map = new ConcurrentHashMap<String, String>();
            for (FileStatus file : status) {
                FSDataInputStream inputStream = fs.open(file.getPath());
                br = new BufferedReader(new InputStreamReader(inputStream));

                String line = null;
                while (null != (line = br.readLine())) {
                    String[] split = line.split(",");
                    map.put(split[0], split[1]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 中国公民身份证号码最小长度。
     */
    public static final int CHINA_ID_MIN_LENGTH = 15;

    /**
     * 中国公民身份证号码最大长度。
     */
    public static final int CHINA_ID_MAX_LENGTH = 18;

    public static Map<String, String> cityCodes = new HashMap<String, String>();

    static {
        cityCodes.put("11", "北京");
        cityCodes.put("12", "天津");
        cityCodes.put("13", "河北");
        cityCodes.put("14", "山西");
        cityCodes.put("15", "内蒙古");
        cityCodes.put("21", "辽宁");
        cityCodes.put("22", "吉林");
        cityCodes.put("23", "黑龙江");
        cityCodes.put("31", "上海");
        cityCodes.put("32", "江苏");
        cityCodes.put("33", "浙江");
        cityCodes.put("34", "安徽");
        cityCodes.put("35", "福建");
        cityCodes.put("36", "江西");
        cityCodes.put("37", "山东");
        cityCodes.put("41", "河南");
        cityCodes.put("42", "湖北");
        cityCodes.put("43", "湖南");
        cityCodes.put("44", "广东");
        cityCodes.put("45", "广西");
        cityCodes.put("46", "海南");
        cityCodes.put("50", "重庆");
        cityCodes.put("51", "四川");
        cityCodes.put("52", "贵州");
        cityCodes.put("53", "云南");
        cityCodes.put("54", "西藏");
        cityCodes.put("61", "陕西");
        cityCodes.put("62", "甘肃");
        cityCodes.put("63", "青海");
        cityCodes.put("64", "宁夏");
        cityCodes.put("65", "新疆");
        cityCodes.put("71", "台湾");
        cityCodes.put("81", "香港");
        cityCodes.put("82", "澳门");
        cityCodes.put("91", "国外");
    }

    // 计算省份
    public static String getProvinces(String id) {
        String str = null;
        if (id.length() != 18 && id.length() != 15) {
            return id + ":证件错误";
        }

        String provinceId = id.substring(0, 2);
        str = cityCodes.get(provinceId);
        if (str == null) {
            return id + ":证件错误";
        }
        return str;
    }

//	// 根据身份证号获取出生年月
//	public static String getBirthday(String id) {
//		byte[] i = null;
//		String age = null;
//		SimpleDateFormat formatter = null;
//		SimpleDateFormat formatter_new = null;
//		String str = null;
//		Date parse;
//		try {
//			i = id.trim().getBytes();
//			if (i.length != 18 && i.length != 15 && i == null) {
//				return id + ":证件错误";
//			}
//			if (i.length == 18) {
//				age = id.substring(6, 14);
//			}
//			if (i.length == 15) {
//
//				age = id.substring(6, 12);
//			}
//			formatter = new SimpleDateFormat("yyyy年MM月dd");
//			formatter_new = new SimpleDateFormat("yyyyMMdd");
//			parse = formatter_new.parse(age);
//			str = formatter.format(parse);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//		return str;
//	}

    // 根据身份证号获取出生年月
    public static String getBirthday(String id, String format) {
        String age = null;
        try {
            if (id.length() != 18 && id.length() != 15) {
                return id + ":证件错误";
            }
            if (id.length() == 18) {
                age = id.substring(6, 14);
            }
            if (id.length() == 15) {
                age = "19" + id.substring(6, 12);
            }
            if (format == null) {
                return age;
            }
            SimpleDateFormat formatter = null;
            SimpleDateFormat formatter_new = null;
            Date parse;
            formatter = new SimpleDateFormat(format);
            formatter_new = new SimpleDateFormat("yyyyMMdd");
            parse = formatter_new.parse(age);
            return formatter.format(parse);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 计算年龄
    public static String getAge(String id) {
        byte[] i = null;
        String age1 = null;
        String age2 = null;
//		Date dd = null;
        int age = 0;
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy");
        String cYear1 = sdf1.format(new Date()).substring(0, 4);
        i = id.trim().getBytes();
        if (i.length != 18 && i.length != 15 && i == null) {
            return id + ":证件错误";
        }
        try {
            if (i.length == 18) {
                age1 = id.substring(6, 10);
                age = Integer.parseInt(cYear1) - Integer.parseInt(age1);
            }
            if (i.length == 15) {
                age2 = id.substring(6, 8);
                age = Integer.parseInt(cYear1) - (1900 + Integer.parseInt(age2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return String.valueOf(age);
    }


    // 计算年龄 按照日期计算
    public static String getAgeClear(String id) {
        String age1 = null;
        String age2 = null;
        int age = 0;
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
        String nowDate = sdf1.format(new Date());
        String cYear1 = nowDate.substring(0, 4);
        String month_date1 = nowDate.substring(4, 8);
        String month_date2 = null;
        //String cYear2 = sdf2.format(new Date()).substring(0, 2);
//			System.out.println(id.length());
        if (id.length() != 18 && id.length() != 15) {
            return id + ":证件错误";
        }
        try {
            if (id.length() == 18) {
                age1 = id.substring(6, 10);
                age = Integer.parseInt(cYear1) - Integer.parseInt(age1);
                month_date2 = id.substring(10, 14);
            }
            if (id.length() == 15) {
                age2 = id.substring(6, 8);
                age = Integer.parseInt(cYear1) - (1900 + Integer.parseInt(age2));
                month_date2 = id.substring(8, 12);
            }
            if (month_date2.compareTo(month_date1) > 0) {
                age--;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return String.valueOf(age);
    }

    // 根据身份证号获取性别
    public static String getGender(String id) {
        if (id.length() != 18 && id.length() != 15) {
            return id + ":证件错误";
        }
        try {
            if (id.length() == 18) {
                return ((id.charAt(16) - '0') % 2 == 1) ? "男" : "女";
            } else if (id.length() == 15) {
                return ((id.charAt(14) - '0') % 2 == 1) ? "男" : "女";
            }
        } catch (Exception ex) {
            System.out
                    .println("Error happend when get gender from idNumber.[idNumber={"
                            + id + "}]," + ex);
        }
        return "身份证格式错误";
    }

    public String evaluate(String ids, String... beans) {
        String str = null;
        if (ids == null || ids.equals("null")) {
            return null;
        }
        String id = ids.trim();
        if (beans == null || beans.length == 0) {
            return "输入参数错误";
        }
        String bean = beans[0];
        if (bean.equals("provinces")) {
            str = IdCard.getProvinces(id);
        } else if (bean.equals("birthday")) {
            if (beans.length == 1) {
                str = IdCard.getBirthday(id, null);
            } else {
                str = IdCard.getBirthday(id, beans[1]);
            }
        } else if (bean.equals("gender")) {
            str = IdCard.getGender(id);
        } else if (bean.equals("age")) {
            str = IdCard.getAge(id);
        } else if (bean.equals("age_d")) {
            //按照天计算出生日期
            str = IdCard.getAgeClear(id);
        } else if (bean.equals("address")) {
            String id_pre = id.substring(0, 6);
            str = getAddress(id_pre);
        }

        return str;
    }

    public static String getAddress(String idPre) {

        if (map == null) {
            loadIDCardLocation();
        }
        return map.get(idPre);
    }

    public static void main(String[] args) {
        IdCard idCard = new IdCard();
        String str = idCard.evaluate("620421810702064", "birthday");
        System.out.println(str);
        System.out.println(idCard.evaluate("51352419880701725y", "gender"));
        System.out.println("age:" + idCard.evaluate("51352388070172y", "age"));
        System.out.println("age_d:" + idCard.evaluate("51352388070172y", "age_d"));
        System.out.println("age2:" + idCard.evaluate(" 142301198602193412", "age_d"));
    }

    public String getBean() {
        return bean;
    }

    public void setBean(String bean) {
        this.bean = bean;
    }

    public void setProvinces(String provinces) {
        this.provinces = provinces;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}