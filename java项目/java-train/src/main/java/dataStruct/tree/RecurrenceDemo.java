package dataStruct.tree;

import dataStruct.tree.bean.TreeNode;

/**
 * @ClassName MaxDepth 递归案例
 * @Description TODO 递归就是在程序中考虑节点所在层级和下一层级的情况，然后进行循环调用
 *  先考虑极限情况，实际就是考虑终态怎么半，然后考虑终态上一层怎么办，层层递进
 * @Author jiang.li
 * @Date 2020-02-18 08:22
 * @Version 1.0
 */
public class RecurrenceDemo {
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
        //如果传入的节点是空
        if (null == node) {
            return 0;
        }
        //传入节点不为空，那么需要考虑左子树情况，终态是左节点为null
        int left = maxDepth(node.getLeftNode());
        //考虑右子树情况，终态是左节点为null
        int right = maxDepth(node.getRightNode());
        //将左右子树得到的结果与本身相加
        //考虑最极端情况，如果左右节点都为空，但是本身不为空，那么需要加上1，表示自己的深度
        return Math.max(left , right )+ 1;
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
        //递归的终态都是通不过上面的限制条件，不会走下面的代码
        int left = minDepth(node.getLeftNode());
        int right = minDepth(node.getRightNode());
        return Math.min(left, right) + 1;
    }

    /**
     * @return int
     * @Author jiang.li
     * @Description //TODO 判断节点个数
     * @Date 08:30 2020-02-18
     * @Param [node]
     **/
    public static int numOfTreeNode(TreeNode node) {
        if(null == node) return 0;
        int left = numOfTreeNode(node.getLeftNode());
        int right = numOfTreeNode(node.getRightNode());
        return left + right +1;
    }

    /**
     * @return int
     * @Author jiang.li
     * @Description //TODO 判断叶子节点个数
     * @Date 08:33 2020-02-18
     * @Param [node]
     **/
    public static int leafNumOfTreeNode(TreeNode node) {
        if(null == node) return 0;
        TreeNode leftNode = node.getLeftNode();
        TreeNode rightNode = node.getRightNode();
        if(null == leftNode && null == leftNode ) return 1;
        return leafNumOfTreeNode(leftNode) + leafNumOfTreeNode(rightNode);
    }

    /**
     * @return int
     * @Author jiang.li
     * @Description //TODO 求从跟节点到k层节点总个数
     * @Date 08:33 2020-02-18
     * @Param [node]
     **/
    public static int numsOfkLevelTreeNode(TreeNode node, int k) {
        if (null == node || k < 1) return 0;
        if(k == 1) return 1;
        int left = numsOfkLevelTreeNode(node.getLeftNode(), k - 1);
        int right = numsOfkLevelTreeNode(node.getRightNode(), k - 1);
        return left + right;
    }

    /**
     * @Author jiang.li
     * @Description //TODO 判断一棵树是不是平衡二叉树,返回-1就不是
     *  判断是否是平衡二叉树，需要节点是否相同，如果节点不相同，那么就不是
     * @Date 14:40 2020-02-18
     * @Param [node]
     * @return boolean
     **/
    public static int isBalanceTree(TreeNode node){
        if(node == null) return 1;
        int right = isBalanceTree(node.getRightNode());
        int left = isBalanceTree(node.getLeftNode());
        if(right == -1 || left==-1 || Math.abs(right - left) > 1){
            return -1;
        }
        //如果左右子树相同，那么返回
        return 1;
    }

    /**
     * @Author jiang.li
     * @Description //TODO 判断两棵树是否相同
     * @Date 14:56 2020-02-18
     * @Param [node1, node2]
     * @return boolean
     **/
    public static boolean isSame(TreeNode node1, TreeNode node2){
        if(node1 == null && node2 == null) return true;
        if(node1 == null || node2 == null) return false;
        if(node1.getVal() != node2.getVal()){
            return false;
        }
        boolean left = isSame(node1.getLeftNode(), node2.getLeftNode());
        boolean right = isSame(node1.getRightNode(), node2.getRightNode());
        return left&&right;

    }

    public static TreeNode getCommonParent(TreeNode root, TreeNode n1, TreeNode n2){
        if(root == null || null == n1 || null == n2) return null;
        //如果root与任一节点相同，那么root就是
        if(n1 == root || n2 == root) return root;
        //判断root左节点是否有
        TreeNode left = getCommonParent(root.getLeftNode(), n1, n2);
        TreeNode right = getCommonParent(root.getRightNode(), n1, n2);
        //如果两个节点都存在，那么这个节点就是
        if(left != null && right != null) return root;
        return null != left?left:right;
    }

}
