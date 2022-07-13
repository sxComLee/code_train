package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Created by ran.li on 2019-11-13
 * 解析json多行数据
 *
 * 传入 [{"name":"jack"},{"name":"mark"},{"name":"bill"}]  "2019-11-12" "INSERT"
 * 变成
 * {"name":"jack"} "2019-11-12" "INSERT"
 * {"name":"mark"} "2019-11-12" "INSERT"
 * {"name":"bill"} "2019-11-12" "INSERT"
 */
@Description(name = "json_array",
        value = "_FUNC_(array_string) - Convert a string of a JSON-encoded array to a Hive array of strings.")
public class UDFJsonArray extends GenericUDTF {


    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("NameParserGenericUDTF() takes a string as a parameter");
        }

        // output inspectors -- an object with two fields!
        List<String> fieldNames = new ArrayList<String>(args.length);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(args.length);

        for(int i=0;i<args.length;i++){
            fieldNames.add("column_name_"+i);
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 处理json多行数据 并且把其他的字段也做多行处理
     * @param record  传入的参数 譬如 json_array(jsonStr,date,....) 支持多个
     * @return
     * @throws HiveException
     */
    public ArrayList<String[]> processInputRecord(Object[] record) throws HiveException {
        ArrayList<String[]> result = new ArrayList<String[]>();
        if(record[0]==null)
            return result;

        //定义第一个值为json字段 根据第一个值
        final String jsonString  =  record[0].toString();


        try{
            JSONArray extractObject = new JSONArray(jsonString);
            for (int ii = 0; ii < extractObject.length(); ++ii) {
                String[] colValueArr = new String[record.length];
                colValueArr[0] = extractObject.get(ii).toString();//第一个值为json数组的元素

                //其他字段为传入的值
                if(record.length>1){
                    for(int j=1;j<record.length;j++){
                        colValueArr[j] = record[j].toString();
                    }
                }
                result.add(colValueArr);
            }
        }catch (JSONException e){
            throw new HiveException("json error");
        }
        return result;
    }

    @Override
    public void process(Object[] record) throws HiveException {

        ArrayList<String[]> results = processInputRecord(record);

        Iterator<String[]> it = results.iterator();

        while (it.hasNext()){
            String[] r = it.next();
            forward(r);
        }
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }
}
