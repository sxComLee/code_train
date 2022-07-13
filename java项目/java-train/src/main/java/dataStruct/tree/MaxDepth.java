package dataStruct.tree;

import dataStruct.tree.bean.TreeNode;

/**
 * @ClassName MaxDepth
 * @Description TODO 递归就是在程序中考虑节点所在层级和下一层级的情况，然后进行循环调用
 * @Author jiang.li
 * @Date 2020-02-18 08:22
 * @Version 1.0
 */
public class MaxDepth {
    public static void main(String[] args) {

    }

    /**
     * @return int
     * @Author jiang.li
     * @Description //TODO 判断最大深度
     * @Date 08:25 2020-02-18
     * @Param [node]
     **/
    public static int maxDepth(TreeNode node) {
        if (null == node) {
            return 0;
        }
        //判断左节点深度
        int left = maxDepth(node.getLeftNode());
        int right = maxDepth(node.getRightNode());
        return Math.max(left, right) + 1;
    }

    /**
     * @return int
     * @Author jiang.li
     * @Description //TODO 判断最小深度
     * @Date 08:26 2020-02-18
     * @Param [node]
     **/
    public static int minDepth(TreeNode node) {
        if (null == node) {
            return 0;
        }
        //判断左节点深度
        int left = minDepth(node.getLeftNode());
        //判断右节点深度
        int right = minDepth(node.getRightNode());
        return Math.min(left, right) + 1;
    }

    /**
     * @Author jiang.li
     * @Description //TODO 判断节点个数
     * @Date 08:30 2020-02-18
     * @Param [node]
     * @return int
     **/
    public static int numOfTreeNode(TreeNode node) {
        if (null == node) return 0;
        int left = numOfTreeNode(node.getLeftNode());
        int right = numOfTreeNode(node.getRightNode());
        //这里+1加的是跟节点个数
        return left + right + 1;
    }

    /**
     * @Author jiang.li
     * @Description //TODO 判断叶子节点个数
     * @Date 08:33 2020-02-18
     * @Param [node]
     * @return int
     **/
    public static int leafNumOfTreeNode(TreeNode node) {
        if (null == node) return 0;
        TreeNode leftNode = node.getLeftNode();
        TreeNode rightNode = node.getRightNode();
        if(leftNode == null && rightNode == null){
            return 1;
        }
      return leafNumOfTreeNode(leftNode)+leafNumOfTreeNode(rightNode);
    }

    /**
     * @Author jiang.li
     * @Description //TODO 求从跟节点到k层节点总个数
     * @Date 08:33 2020-02-18
     * @Param [node]
     * @return int
     **/
    public static int numsOfkLevelTreeNode(TreeNode node, int k) {
        if (null == node || k<1) return 0;
        if(k == 1) return 1;
        int left = numsOfkLevelTreeNode(node.getLeftNode(), k - 1);
        int right = numsOfkLevelTreeNode(node.getRightNode(), k - 1);
        return left + right ;
    }

    /**
     * @Author jiang.li
     * @Description //TODO 判断是不是平衡二叉树
     * @Date 08:33 2020-02-18
     * @Param [node]
     * @return int
     **/
    public static boolean isBalanced(TreeNode node){
        return maxDeath2(node)!=-1;
    }

    public static int maxDeath2(TreeNode node){
        if(node == null){
            return 0;
        }
        int left = maxDeath2(node.getLeftNode());
        int right = maxDeath2(node.getRightNode());
        if(left==-1||right==-1||Math.abs(left-right)>1){
            return -1;
        }
        return Math.max(left, right) + 1;
    }

}
