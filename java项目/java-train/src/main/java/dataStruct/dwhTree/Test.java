package dataStruct.dwhTree;

public class Test {
    public static void main(String[] args) {
        TreeNode rootNode = new TreeNode(1);
        TreeNode node2 = new TreeNode(2);
        TreeNode node3 = new TreeNode(3);
        TreeNode node4 = new TreeNode(4);
        TreeNode node5 = new TreeNode(5);
        TreeNode node6 = new TreeNode(6);
        TreeNode node7 = new TreeNode(7);
        TreeNode node8 = new TreeNode(8);
        TreeNode node9 = new TreeNode(9);
        TreeNode node10 = new TreeNode(10);
        TreeNode node11 = new TreeNode(11);
        TreeNode node12 = new TreeNode(12);
        TreeNode node13 = new TreeNode(13);
        TreeNode node14 = new TreeNode(14);
        TreeNode node15 = new TreeNode(15);

        node4.setLeftNode(node8);
        node4.setRightNode(node9);

        node5.setLeftNode(node10);
        node5.setRightNode(node11);

        node6.setLeftNode(node12);
        node6.setRightNode(node12);

        node7.setLeftNode(node14);
        node7.setRightNode(node15);

        node2.setLeftNode(node4);
        node2.setRightNode(node5);

        node3.setLeftNode(node6);
        node3.setRightNode(node7);

        rootNode.setLeftNode(node2);
        rootNode.setRightNode(node3);

        System.out.println(rootNode);

        // 先序遍历

        // 中序遍历

        // 后序遍历

        // 深度优先遍历

        // 广度优先遍历
    }

    private static void test1(TreeNode node) {
        if (node == null) return;

        System.out.println(node.getValue());
        test1(node.getLeftNode());
        test1(node.getRightNode());
    }
}
