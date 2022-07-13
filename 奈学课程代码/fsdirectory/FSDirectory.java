package com.mazh.nx.hdfs3.fsdirectory;

import java.util.*;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 关于 HDFS 的内存目录树的 类似简易实现！
 *  重点理解两点：
 *  1、关于目录树的抽象实现和关系维护
 *  2、关于对应功能方法的具体实现
 */
public class FSDirectory {

    // TODO_MA 马中华 注释： 根节点
    INodeDirectory rootDir = new INodeDirectory("");

    // TODO_MA 马中华 注释： 存储所有节点
    // TODO_MA 马中华 注释： key = 节点路径， value = 节点对象
    INodeMap inodeMap = new INodeMap(rootDir);

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 创建文件夹
     */
    public void mkdir(String abosuluteDirPath) throws Exception {
        mkdir(abosuluteDirPath, true);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 创建文件夹
     */
    public void mkdir(String dirPath, boolean recursive) throws Exception {
        if (!recursive) {
            String parentPath = dirPath.substring(0, dirPath.indexOf("/"));
            if (parentPath.equals("")) {
                parentPath = "/";
            }
            INode iNode = inodeMap.get(parentPath);
            if (iNode == null) {
                throw new Exception("父节点：" + parentPath + " 不存在");
            }
            if (!iNode.isDir) {
                throw new Exception("当前要创建的文件夹 " + parentPath + " 已经存在同名文件");
            }
        } else {
            // TODO_MA 马中华 注释： /a/b/c/d
            String[] split = dirPath.split("/");
            String parentPath = "/";
            for (int i = 1; i < split.length; i++) {
                INodeDirectory parentNode = (INodeDirectory) inodeMap.get(parentPath);
                INode currentDir = new INodeDirectory(split[i]);
                String currentPath = (parentNode.getAbsolutPath() + "/" + split[i]).replace("//", "/");
                if (inodeMap.get(currentPath) != null) {
                    ///
                } else {
                    parentNode.addNode(currentDir);
                    inodeMap.add(currentDir.getAbsolutPath(), currentDir);
                }
                parentPath = (parentPath + "/" + split[i]).replace("//", "/");
            }
        }
    }

    public void print(INodeDirectory directory) {
        // TODO_MA 马中华 注释： 打印输出当前节点
        System.out.println(directory.getAbsolutPath());

        // TODO_MA 马中华 注释： 打印输出子节点
        for (INode node : directory.getChildren()) {
            if (node.isDir) {
                INodeDirectory dir = (INodeDirectory) node;
                print(dir);
            } else {
                System.out.println(node.getAbsolutPath());
            }
        }
    }

    // TODO_MA 马中华 注释： 上传文件
    // TODO_MA 马中华 注释： 这个方法的具体实现，作为作业留个大家自己实践一下！
    // TODO_MA 马中华 注释： 这个方法的具体实现，作为作业留个大家自己实践一下！
    // TODO_MA 马中华 注释： 这个方法的具体实现，作为作业留个大家自己实践一下！
    public String createFile(String parentDir, String fileName) {
        return parentDir + "/" + fileName;
    }

    // TODO_MA 马中华 注释： 测试入口
    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 第一种方式
        // TODO_MA 马中华 注释： 通过指定 绝对路径来创建 文件夹的方式来搞定
        test1();

        System.out.println("-----华丽分界线-----");

        // TODO_MA 马中华 注释： 第二种方式
        // TODO_MA 马中华 注释： 逐级维护， 先创建 /a, 再创建 /a/b
        test2();
    }

    public static void test1() throws Exception {
        // TODO_MA 马中华 注释： 创建一颗内存树
        FSDirectory fsDirectory = new FSDirectory();

        fsDirectory.mkdir("/subDir1");
        fsDirectory.mkdir("/subDir2");
        fsDirectory.mkdir("/subDir1/dir1");
        fsDirectory.mkdir("/subDir1/dir2");
        fsDirectory.mkdir("/subDir2/dir3");
        fsDirectory.mkdir("/subDir2/dir3/dir4/dir5");
        fsDirectory.mkdir("/subDir2/dir6");
        fsDirectory.mkdir("/subDir2/dir7");

        fsDirectory.print(fsDirectory.rootDir);
    }

    public static void test2() {
        // TODO_MA 马中华 注释： 创建一颗内存空树（只有根节点）
        FSDirectory fsDirectory = new FSDirectory();

        // TODO_MA 马中华 注释： 根节点下第一个子节点： 文件夹
        INodeDirectory subDir1 = new INodeDirectory("subDir1");
        fsDirectory.rootDir.addNode(subDir1);
        fsDirectory.inodeMap.add(subDir1.getAbsolutPath(), subDir1);

        // TODO_MA 马中华 注释： 根节点下第二个子节点： 文件夹
        INodeDirectory subDir2 = new INodeDirectory("subDir2");
        fsDirectory.rootDir.addNode(subDir2);
        fsDirectory.inodeMap.add(subDir2.getAbsolutPath(), subDir2);

        // TODO_MA 马中华 注释： subDir2 下一个子文件夹
        INodeDirectory subDir3 = new INodeDirectory("subDir3");
        subDir2.addNode(subDir3);
        fsDirectory.inodeMap.add(subDir3.getAbsolutPath(), subDir3);

        // TODO_MA 马中华 注释： 第一个文件夹下的一个文件
        Block block1 = new Block(UUID.randomUUID().toString().substring(0, 6));
        Block block2 = new Block(UUID.randomUUID().toString().substring(0, 6));
        Block block3 = new Block(UUID.randomUUID().toString().substring(0, 6));
        ArrayList<Block> blocks = new ArrayList<>();
        Collections.addAll(blocks, block3, block2, block1);
        INodeFile file1 = new INodeFile("file1", blocks);
        subDir1.addNode(file1);
        fsDirectory.inodeMap.add(file1.getAbsolutPath(), subDir1);

        // TODO_MA 马中华 注释： 输出 目录树
        fsDirectory.print(fsDirectory.rootDir);
    }
}

class INodeMap {

    // TODO_MA 马中华 注释： 成员变量 map 存储所有节点
    Map<String, INode> inodeMap = new HashMap<String, INode>();

    public INodeMap(INodeDirectory rootDir) {
        inodeMap.put("/", rootDir);
    }

    // TODO_MA 马中华 注释： 提供一个加入方法
    public void add(String path, INode iNode) {
        inodeMap.put(path, iNode);
    }

    public INode get(String path) {
        return inodeMap.get(path);
    }
}

// TODO_MA 马中华 注释： 节点抽象
class INode {
    // TODO_MA 马中华 注释： /a/b/c, name = c
    String name;
    boolean isDir;
    // TODO_MA 马中华 注释： /a/b/c
    private String absolutPath;  // TODO_MA 马中华 注释： 是我额外加的，HDFS 没有！

    // TODO_MA 马中华 注释： HDFS 没有维护这个关系，是因为很好解决
    // TODO_MA 马中华 注释： currentPath = /a/b/c
    // TODO_MA 马中华 注释： parentPath = currentPath.subString(0, currentPath.lastindexOf("/"))
    // TODO_MA 马中华 注释： INode paretnNode = iNodeMap.get(parentPath);
    private String parent;

    public INode(String name) {
        this.name = name;
    }

    public INode(String name, boolean isDir) {
        this.name = name;
        this.isDir = isDir;
    }

    public void setIsDir(boolean isDir) {
        this.isDir = isDir;
    }

    public String getAbsolutPath() {
        return absolutPath;
    }

    public void setAbsolutPath(String absolutPath) {
        this.absolutPath = absolutPath;
    }
}

// TODO_MA 马中华 注释： 文件
class INodeFile extends INode {

    // TODO_MA 马中华 注释： 文件是由数据块组成的
    List<Block> blocks;

    public INodeFile(String name) {
        super(name);
    }

    public INodeFile(String name, List<Block> blocks) {
        super(name);
        this.isDir = false;
        this.blocks = blocks;
    }
}

// TODO_MA 马中华 注释： 文件夹
class INodeDirectory extends INode {

    // TODO_MA 马中华 注释： 子节点列表
    private List<INode> children;

    public INodeDirectory(String name) {
        super(name);
        children = new ArrayList<INode>();
        this.isDir = true;
        if (name.equals("")) {
            setAbsolutPath("/");
        }
    }

    public void addNode(INode child) {
        String path = (this.getAbsolutPath() + "/" + child.name).replace("//", "/");
        child.setAbsolutPath(path);
        this.children.add(child);
    }

    public List<INode> getChildren() {
        return this.children;
    }
}

// TODO_MA 马中华 注释： 数据块
class Block {
    String blockID;

    public Block(String blockID) {
        this.blockID = blockID;
    }
}