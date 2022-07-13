package dataStruct.tree;

import dataStruct.tree.bean.TreeNode;

import java.util.LinkedList;

/**
 * @ClassName levelPrint
 * @Description TODO 给定二叉树的跟节点，将二叉树层级遍历
 * @Author jiang.li
 * @Date 2020-02-18 15:32
 * @Version 1.0
 */
public class levelPrint {
    public static void main(String[] args) {

    }

    /**
     * @Author jiang.li
     * @Description //TODO
     * 思路：将二叉树的子节点放入一个先进先出的队列中，然后后续分别加入子节点的子节点，不断循环队列元素,逐层打印
     * @Date 15:34 2020-02-18
     * @Param [node]
     * @return void
     **/
    public static void printLevel(TreeNode node){
        if(null == node) return;
        LinkedList<TreeNode> list = new LinkedList<>();
        list.add(node);
        while(!list.isEmpty()){
            //或者为了优化，可以写一个for循环，将当前队列中的元素全部弹出之后再循环
            //将第一个元素弹出
            TreeNode node1 = list.poll();
            System.out.println(node1.getVal());
            TreeNode leftNode = node1.getLeftNode();
            TreeNode rightNode = node1.getRightNode();

            //将它的子节点加入
            if(leftNode != null)list.add(leftNode);
            if(rightNode != null)list.add(rightNode);
        }
    }
}
