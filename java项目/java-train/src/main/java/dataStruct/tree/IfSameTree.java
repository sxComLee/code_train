package dataStruct.tree;


import dataStruct.tree.bean.TreeNode;

import java.util.Stack;

/**
 * @ClassName IfSameTree
 * @Description TODO 判断两棵树是否为相同的
 * @Author jiang.li
 * @Date 2020-02-11 09:39
 * @Version 1.0
 */
public class IfSameTree {
    public static void main(String[] args) {
        TreeNode p = new TreeNode("lifei",null,null);
        TreeNode q = new TreeNode("lifei",p,null);
        boolean sameTree = isSameTree(p, q);
        System.out.println(sameTree);
        sameTree = isSameTree(p, q);
        System.out.println(sameTree);
    }

    public static boolean isSameTree(TreeNode p, TreeNode q){
        if(p == null || q == null)
            return p == q;

        return (p.getVal().equals(q.getVal())) && isSameTree(p.getLeftNode(),q.getLeftNode()) && isSameTree(p.getRightNode(),q.getRightNode());
    }

    public static boolean isSameTreeStack(TreeNode p, TreeNode q){
        //创建两个栈用户放置节点对象，栈的特点，先进后出
        Stack<TreeNode> node_p = new Stack<>();
        Stack<TreeNode> node_q = new Stack<>();
        //如果这两个节点不为空，那么将值放入栈对象中
        if(p != null) node_p.push(p);
        if(q != null) node_q.push(q);
        //将栈中的数据弹出判断值是否相等，同时判断两个栈为否为空，如果不为空就继续
        while(!node_p.isEmpty() && !node_q.isEmpty()){
            //peek()函数返回栈顶的元素，但不弹出该栈顶元素。pop()函数返回并弹出栈顶元素
            TreeNode tp = node_p.pop();
            TreeNode tq = node_p.pop();
            //如果两个值不相等，返回false
            if(!tp.getVal().equals(tq.getVal())) return false;
            //接着将左右节点的值进行判断
            TreeNode pleftNode = p.getLeftNode();
            TreeNode qleftNode = q.getLeftNode();
            if(pleftNode != null)node_p.push(pleftNode);
            if(qleftNode != null)node_q.push(qleftNode);
            if(node_p.size() != node_q.size()) return false;

            TreeNode prightNode = p.getRightNode();
            TreeNode qrightNode = q.getRightNode();
            if(prightNode != null)node_p.push(prightNode);
            if(qrightNode != null)node_q.push(qrightNode);
            if(node_p.size() != node_q.size()) return false;

        }

        return node_p.size() == node_q.size();
    }

}
