package com.mazh.nx.hdfs3.design_pattern.decorate;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 装饰器设计模式
 */
public class DecoratePatternDemo2 {

    public static void main(String[] args) throws Exception {

        // TODO_MA 马中华 注释： 原始类
        DataSource dataSource = new DataSource();
        dataSource.writeData("");

        // TODO_MA 马中华 注释： 装饰类
        new CheckableDataSource(dataSource).writeData("");
    }
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 装饰类
 */
class CheckableDataSource{

    private DataSource dataSource;

    CheckableDataSource() {
    }

    CheckableDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    String readData() throws Exception {
        String s = dataSource.readData();
        if (s == null){
            throw new Exception("没有读到 Data，抛出异常");
        }
        return s;
    }

    void writeData(String data) throws Exception {
        if (data == null || data == ""){
            throw new Exception("没有指定 Data，抛出异常");
        }
        dataSource.writeData(data);
    }
}

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 原始类
 */
class DataSource{

    private String data;

    String readData(){
        return getData();
    }

    void writeData(String data){
        setData(data);
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}