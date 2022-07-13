package dataStruct;

/**
 * @ClassName MaxPrimeFactor
 * @Description TODO 13195的所有质因数为5、7、13和29。600851475143最大的质因数是多少？
 * @Author jiang.li
 * @Date 2020-02-11 09:29
 * @Version 1.0
 */
public class MaxPrimeFactor {
    public static void main(String[] args) {

    }

    /*基本思路：通过递归不断寻找*/
    public static long findMaxPrimeFactor(long number) {
        //如果传入的数据是1，那么这个数的最大质因数就是1
        if (number == 1) {
            return number;
        }
        //传入的数据大于1，从2开始向上找，根据最小因数能找到最大因数，然后将因数进行分解，找最大因数的因数。。。知道这个最大的因数是质因数，不能再分解
        for (int i = 2; i < number; i++) {
            if(number  % i ==0){
                return findMaxPrimeFactor(number/i);
            }
        }
        //从 2 循环到number 一直没有因数，那么最大质因数就是number
        return number;
    }
}
