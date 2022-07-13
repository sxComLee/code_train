package udaf;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

/**
 * @ClassName UDAFAllToJson
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-09 15:55
 * @Version 1.0
 */
@Slf4j
@Description(name = "deal_json",
        value = "_FUNC_(jsonkeyField,jsonvalueField) - Convert a field to json.")
public class UDAFAllToJson extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        //对参数的类型进行判断，这里只接受两个参数，第一个参数代表返回json的key值，第二个参数代表返回json的value值
        if(info.length != 2){
            throw new UDFArgumentTypeException(0,
                    "primitive type length 2 arguments are accepted but "
                            + info.length + " is passed.");
        }

        return new GenericJson();
    }

    public static class GenericJson extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector inputKey;
        private PrimitiveObjectInspector inputValue;

        private Text result;

        //定义返回值类型为String
        //每个阶段都会执行init，不同阶段对应的parameters是不一样的，
        //  在map阶段parameters代表的是sql语句中每个udaf对应参数的ObjectInspector,
        //  而在combiner或者reducer中parameters代表部分聚合结果对应的ObjectInspector。所以要区分对待。
        //  从iterate和merge的参数类型（一个数组类型，一个是object）就能看出来。
        //  因此在iterate和merge中分别使用inputOI1/2/3和outputOI 提取对应数据

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            result = new Text();
            super.init(m,parameters);
            //PARTIAL1为map阶段，PARTIAL2 为combiner阶段  final 为reduce阶段 complete为只有map没有reduce阶段
            if(m == Mode.PARTIAL1 || m == Mode.COMPLETE ){
                inputKey = (PrimitiveObjectInspector) parameters[0];
                inputValue = (PrimitiveObjectInspector) parameters[1];
            }


            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        }

        static class NewJSON extends JSONObject implements AggregationBuffer {
        }

        //创建新的聚合计算的需要的内存，用来存储mapper,combiner,reducer运算过程中的相加总和。
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            NewJSON json = new NewJSON();
            reset(json);
            return json;
        }

        //mapreduce支持mapper和reducer的重用，所以为了兼容，也需要做内存的重用。
        // 重置聚集结果
        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            NewJSON json = (NewJSON) aggregationBuffer;
        }

        //map阶段调用，只要把保存当前的对象json，再加上输入的参数转换成的JSON，就可以了。
        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            NewJSON newJSON = new NewJSON();
            String key = PrimitiveObjectInspectorUtils.getString(objects[0], inputKey);
            String value = PrimitiveObjectInspectorUtils.getString(objects[1], inputValue);
            newJSON.put(key,value);
            merge(aggregationBuffer,newJSON.toJSONString());
        }

        //mapper结束要返回的结果，还有combiner结束返回的结果
        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            return terminate(aggregationBuffer);
        }

        //combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果。
        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {

            if(o!= null){
                String s = o.toString();
                JSONObject jsonObject = JSONObject.parseObject(s);
                for (String key:jsonObject.keySet()
                     ) {
                    ((NewJSON)aggregationBuffer).put(key,jsonObject.getString(key));
                }
            }
        }

        //reducer返回结果，或者是只有mapper，没有reducer时，在mapper端返回结果。
        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            NewJSON json = (NewJSON) aggregationBuffer;
            if(json.isEmpty()){
                return null;
            }
            result.set(json.toString());
            return result;
        }
    }


}
