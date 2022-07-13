package search;

/**
 * @ClassName BinarySearch
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-25 16:28
 * @Version 1.0
 */
public class BinarySearch {
    public static void main(String[] args) {
        int[] arr = {1,2,3,4,5,6,7,8,9,10};
        int low = 0;
        int index = (low+arr.length)/2;
        findVlaue(arr, 0, arr.length-1, 91);
    }
    //假设数组中的元素全部从小到达排列好了
    public static int findVlaue(int[] arr,int low,int max,int value){
        //判断是否在范围内，如果不在，说明不存在
        if(arr[low] > value || arr[max] < value){
            System.out.println("数据不存在");
            return 0;
        }


        int middle = (arr[low]+arr[max])/2;

        if(arr[low] == value || arr[max] == value ){
            System.out.println("find it");
        }

        if(arr[middle]>value){
            return findVlaue(arr,low,middle,value);
        }else if(arr[middle]<value){
            return findVlaue(arr,middle,max,value);
        }else{
            System.out.println("find it , index is "+middle);
        }

            return middle;
    }




}
