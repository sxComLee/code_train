package dataStruct.tree;

import dataStruct.tree.bean.TreeNode;

import java.util.LinkedList;

/**
 * @ClassName levelRead
 * @Description TODO 将每一层进行输出
 * @Author jiang.li
 * @Date 2020-02-17 15:42
 * @Version 1.0
 */
public class levelRead {

    public static void main(String[] args) {
        //两个递增int数组
        int[] a = {1,2,4,7,9};
        int[] b = {3,5,6,10};
        //创建一个新的数组用来存放
        int[] c = new int[a.length+b.length];

        //两个标志用来辅助获取数据
        int tmp_a = 0;
        int tmp_b = 0;
        for(int i =0;i< c.length;i++){
            if(i == c.length-1){
               c[i] = (a[tmp_a]>=b[tmp_b]?a[tmp_a]:b[tmp_b]);
                System.out.print(c[i]);
               continue;
            }
            if(a[tmp_a]>b[tmp_b]){
                c[i] = b[tmp_b];
                tmp_b++;
            }else if(a[tmp_a]<b[tmp_b]){
                c[i] = a[tmp_a];
                tmp_a++;
            }else if(a[tmp_a]== b[tmp_b]){
                c[i] = b[tmp_b];
                c[i+1] = b[tmp_b];
                tmp_b++;
                tmp_a++;
            }
            //判断当前数组下标是否越界
            tmp_a = (tmp_a >= a.length?a.length-1:tmp_a);
            tmp_b = (tmp_b >= a.length?a.length-1:tmp_b);
            System.out.print(c[i]);
        }

        }


    public static void levelRead(TreeNode root){
        if(null == root) return;
        LinkedList<TreeNode> tree = new LinkedList<>();
        tree.add(root);
        while(tree.size()>0){
            //将集合进行遍历
            for (int i=0;i <tree.size(); i++) {
                //取出头部元素
                TreeNode tmp = tree.poll();
                System.out.println(tmp.getVal()+"   ");

                //将元素的左节点放入
                if(tmp.getLeftNode() != null) tree.add(tmp.getLeftNode() );
                if(tmp.getRightNode() != null) tree.add(tmp.getRightNode() );

            }
        }
    }
}
