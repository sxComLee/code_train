import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * @ClassName FastJsonTest
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-10-17 17:36
 * @Version 1.0
 */
public class FastJsonTest {
    public static void main(String[] args) {
        String json = "[{'学生':'张三', '班级':'一班', '年级':'大一', '科目':'高数', '成绩':90}, {'学生':'李四', '班级':'二班', '年级':'大一', '科目':'高数', '成绩':80}]";

        JSONArray jsonArray = JSONArray.parseArray(json);
        JSONObject jsonObject = jsonArray.getJSONObject(0);
        System.out.println(jsonObject.getString("学生"));
    }

}
