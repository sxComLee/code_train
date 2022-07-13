package sort;

import java.util.Arrays;

/**
 * @ClassName BubbleTest
 * @Description TODO  
 * @Author jiang.li
 * @Date 2019-12-25 15:16
 * @Version 1.0
 */
public class BubbleTest {
    public static void main(String[] args) {
        int[] s1 = {1,22,13,24,12,23,34,45,56,67,78,65,454,34,23,21,11};
        sort(s1);
        fixSort(s1);
    }

    /**
     * 1.整个数列分成两部分：前面是无序数列，后面是有序数列。
     * 2.初始状态下，整个数列都是无序的，有序数列是空。
     * 3.每一趟循环可以让无序数列中最大数排到最后，(也就是说有序数列的元素个数增加1)，也就是不用再去顾及有序序列。
     * 4.每一趟循环都从数列的第一个元素开始进行比较，依次比较相邻的两个元素，比较到无序数列的末尾即可(而不是数列的末尾);如果前一个大于后一个，交换。
     * 5.判断每一趟是否发生了数组元素的交换，如果没有发生，则说明此时数组已经有序，无需再进行后续趟数的比较了。此时可以中止比较。
     **/
    public static int[] fixSort(int[] s1){
        int times = 0;
        for (int i = 0; i < s1.length ; i++) {
            boolean flag = true;
            for (int j = 0; j < s1.length-i-1; j++) {
                //从小到大排列
                if(s1[j] > s1[j+1]){
                    int tmp = s1[j+1];
                    s1[j+1] = s1[j];
                    s1[j] = tmp;
                }
                times ++;
                flag = false;
            }
            if(flag){
                break;
            }
        }
        System.out.println(Arrays.toString(s1));
        System.out.printf("一共计算了 %d 次",times);

        return  s1;
    }


    /**
     *  *       1. 比较相邻的元素。如果第一个比第二个大，就交换他们两个。
     *  *       2. 对每一对相邻元素作同样的工作，从开始第一对到结尾的最后一对。在这一点，最后的元素应该会是最大的数。
     *  *       3. 针对所有的元素重复以上的步骤，除了最后一个。
     *  *       4. 持续每次对越来越少的元素重复上面的步骤，直到没有任何一对数字需要比较。
     **/
    public static int[] sort(int[] s1){
        int times = 0;
        for (int i = 0; i < s1.length; i++) {
            for (int j = 0; j < s1.length-i-1 ; j++) {
//                //从小到大最大的数
//                if(s1[j] > s1[j+1]){
//                    int tmp = s1[j+1];
//                    s1[j+1] = s1[j];
//                    s1[j] = tmp;
//                }

                //从大到小排序
                if(s1[j] <s1[j+1]){
                    int tmp = s1[j+1];
                    s1[j+1] = s1[j];
                    s1[j] = tmp;
                }
                times++;
            }
        }

        System.out.println(Arrays.toString(s1));
        System.out.printf("一共计算了 %d 次",times);
        System.out.println();
        return s1;
    }

}
