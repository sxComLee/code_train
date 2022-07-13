package com.lij.design_pattern.composite;

import java.util.Map;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 组合设计模式的实现
 *  1、FSDirectory 整合 内存元数据管理功能 和 磁盘元数据管理管理功能
 *  2、MemoryTree 管理内存元数据
 *  3、FSImage 管理磁盘元数据
 */
public class FSDirectory {

    // TODO_MA 马中华 注释： 维护内存元数据
    private MemoryTree memoryTree;

    // TODO_MA 马中华 注释： 维护磁盘元数据
    private FSImage fsImage;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 后续这一部分代码都是 FSImage 提供
     */
    void logEdit(Edit edit) {
        fsImage.logEdit(edit);
    }

    void flush() {
        fsImage.flush();
    }

    void startLogSegment() {
        fsImage.startLogSegment();
    }

    public void saveFSImage() {
        fsImage.saveFSImage();
    }

    public void loadFSImage() {
        fsImage.loadFSImage();
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 后续这部分代码都是 MemoryTree 提供
     */
    void createNode(INode iNode){
        memoryTree.createNode(iNode);
    }

    void deleteNode(){
        memoryTree.deleteNode();
    }

    void listNode(){
        memoryTree.listNode();
    }
}

class FSImage {

    private FSEdit fsEdit;

    FSImage(FSEdit fsEdit) {
        this.fsEdit = fsEdit;
    }

    public void saveFSImage() {
    }

    public void loadFSImage() {
    }

    void logEdit(Edit edit) {
        fsEdit.logEdit(edit);
    }

    void flush() {
        fsEdit.flush();
    }

    void startLogSegment() {
        fsEdit.startLogSegment();
    }
}

class FSEdit {
    void logEdit(Edit edit) {
    }

    void flush() {
    }

    void startLogSegment() {
    }
}

class Edit {
}

interface INode {
}

class INodeFile implements INode {
}

class INodeDirectory implements INode {
}

class MemoryTree{

    private INodeDirectory root;
    private Map<String, INode> inodeMap;

    MemoryTree(INodeDirectory root, Map<String, INode> inodeMap) {
        this.root = root;
        this.inodeMap = inodeMap;
    }

    void createNode(INode iNode){

    }

    void deleteNode(){

    }

    void listNode(){

    }
}