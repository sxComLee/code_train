package dataStruct.tree;

import dataStruct.tree.bean.TreeNode;

import java.util.LinkedList;

/**
 * @ClassName SymmetryTree
 * @Description TODO 判断一棵树是不是对称的树
 * @Author jiang.li
 * @Date 2020-02-11 10:35
 * @Version 1.0
 */
public class SymmetryTree {
    public static void main(String[] args) {
        TreeNode p = new TreeNode("lifei",null,null);
        TreeNode q = new TreeNode("lifei",p,null);
        boolean sameTree = ifSymmetryTree(p, q);
        System.out.println(sameTree);
        sameTree = ifSymmetryTree(p, q);
        System.out.println(sameTree);
    }

    public static boolean ifSymmetryTree(TreeNode p, TreeNode q){
        //如果两个节点都为null，那么对称
        if(p == null && q == null) return true;
        //如果只有一个为null，那么不对称
        if(p == null || q == null) return false;
        //在两节点的值相等，且左子树的左节点和右子树的右节点值相同，且左子树的右节点和右子树的左节点值相同
        return (p.getVal().equals(q.getVal()))
                && ifSymmetryTree(p.getLeftNode(),q.getRightNode())
                && ifSymmetryTree(p.getRightNode(),q.getLeftNode());
    }


    //判断一棵树是不是对称树
    public static boolean ifSymmetryTree1(TreeNode t){
        LinkedList<TreeNode> p = new LinkedList<>();
        //将左右加入队列
        p.add(t.getLeftNode());
        p.add(t.getRightNode());

        //循环判断是不是对称的
        while(p.size()>0){
            TreeNode left = p.poll() ,right = p.poll();;
            if(left == null && right == null) return true;
            if(left == null || right == null) return false;
            if(left.getVal() != right.getVal()) return false;
            p.add(left.getLeftNode());
            p.add(right.getRightNode());
            p.add(right.getLeftNode());
            p.add(left.getRightNode());
        }

        return true;
    }
}
