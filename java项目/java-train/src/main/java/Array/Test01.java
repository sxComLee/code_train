package Array;

/**
 * @ClassName Test01
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-25 14:13
 * @Version 1.0
 */
public class Test01 {

    public static void main(String[] args) {
//        int[] arr = {1,2,3,4,5,6,7};
//        int[] vrr = new int[4];
//        System.arraycopy(arr, 3, vrr, 0, 4);
//        for (int a :vrr
//             ) {
//            System.out.println(a);
//        }
        String[] arr = {"1","2","3","4","5","6","7"};
//        deleteIndex(arr,2);
//        extendRange(arr);
        insertRange(arr, 2, "12");
    }

    public static void insertRange(String[] arr,int index,String a){
        String[] strings = extendRange(arr);
        System.arraycopy(strings, index, strings, index+1, arr.length-index);
        strings[index] =a;
        for (int i=0;i<strings.length;i++) {
            System.out.println(i +"......"+strings[i]);
        }
    }

    /**
     * @Author jiang.li
     * @Description //TODO 扩展数组中的元素
     * @Date 14:59 2019-12-25
     * @Param []
     * @return void
     **/
    public static String[] extendRange(String[] arr){
//        String[] s1 = {"1","2","3","4"};
        String[] s2 = new String[arr.length+10];
        System.arraycopy(arr, 0, s2, 0, arr.length);
        for (int i=0;i<s2.length;i++) {
            System.out.println(i +"......"+s2[i]);
        }
        return s2;

    }

    /**
     * @Author jiang.li
     * @Description //TODO 删除数组中的元素
     * @Date 14:54 2019-12-25
     * @Param [arrays, index]
     * @return void
     **/
    public static void deleteIndex(String[] arrays,int index){
        System.arraycopy(arrays, index+1, arrays, index, arrays.length-index-1);
        arrays[arrays.length-1] = null;
        for (int i=0;i<arrays.length;i++) {
            System.out.println(i +"......"+arrays[i]);
        }

    }

}
