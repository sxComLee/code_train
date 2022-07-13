package com.mazh.nx.hdfs3.design_pattern.composite;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * 组合设计模式
 *
 * HDFS 目录树
 *      权限系统
 *      部门系统
 *
 *      奈学主部门：
 *          奈学子部门1：
 *                叶子部门1
 *                叶子部门2
 *          子部门2
 *                叶子部门3
 *
 */
public class WhithoutPatternDemo {
    public static void main(String[] args) {

        Department coreDep = new Department("主部门");

        Department subDep1 = new Department("子部门1");
        Department subDep2 = new Department("子部门2");

        Department leafDep1 = new Department("叶子部门1");
        Department leafDep2 = new Department("叶子部门2");
        Department leafDep3 = new Department("叶子部门3");

        subDep1.child.add(leafDep1);
        subDep1.child.add(leafDep2);

        subDep2.child.add(leafDep3);

        coreDep.child.add(subDep1);
        coreDep.child.add(subDep2);
        //需求 删除主部门

        if(coreDep.child.size() > 0){
            for(Department sub:coreDep.child){
                if(sub.child.size() > 0){
                    for (Department leaf:sub.child){
                        leaf.remove();
                    }
                }
                sub.remove();
            }
        }
        coreDep.remove();
    }

    public static class Department{
        private String name;
        private List<Department> child =new ArrayList<Department>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<Department> getChild() {
            return child;
        }

        public void setChild(List<Department> child) {
            this.child = child;
        }

        public Department(String name) {
            this.name = name;
        }
        public void remove(){
            System.out.println("删除"+name);
        }
    }
}
