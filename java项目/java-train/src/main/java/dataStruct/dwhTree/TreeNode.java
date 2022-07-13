package dataStruct.dwhTree;

public class TreeNode {
    private static int value;
    private static TreeNode leftNode;
    private static TreeNode rightNode;

    public static int getValue() {
        return value;
    }

    public TreeNode() {
    }

    public TreeNode(int value) {
        this.value = value;
    }

    public static void setValue(int value) {
        TreeNode.value = value;
    }

    public static TreeNode getLeftNode() {
        return leftNode;
    }

    public static void setLeftNode(TreeNode leftNode) {
        TreeNode.leftNode = leftNode;
    }

    public static TreeNode getRightNode() {
        return rightNode;
    }

    public static void setRightNode(TreeNode rightNode) {
        TreeNode.rightNode = rightNode;
    }

}
