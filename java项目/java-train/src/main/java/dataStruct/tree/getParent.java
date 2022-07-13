package dataStruct.tree;

import dataStruct.tree.bean.TreeNode;

/**
 * @ClassName getParent
 * @Description TODO
 * @Author jiang.li
 * @Date 2020-02-12 14:20
 * @Version 1.0
 */
public class getParent {
    public static void main(String[] args) {

    }

    public static TreeNode getParent(TreeNode root, TreeNode left, TreeNode right){
        //如果根节点或者左右节点为空，那么返回空
        if(root == null || left == null || right == null) return null;
        //如果根节点的值为左节点或者右节点的值，那么根节点就是那个值
        if(left == root || right == root) return root;
        //如果没有找到，那么向下寻找
        TreeNode left1 = getParent(root.getLeftNode(), left, right);
        TreeNode right1 = getParent(root.getRightNode(), left, right);
        //如果左右子树都能找到，那么当前节点就是
        if(left1 != null && right1 != null) return root;
        //如果右子树有结果，那么返回右子树
        if(right1 != null) return right1;
        //右子树没有，那么返回左子树的查找结果
        else return left1;
    }

}
