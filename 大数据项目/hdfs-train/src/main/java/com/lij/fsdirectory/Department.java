package com.lij.fsdirectory;

import java.util.ArrayList;
import java.util.List;

/**
 * /主部门
 *      /子部门1
 *          /叶子部门1
 *          /叶子部门2
 *      /子部门2
 *          /叶子部门3
 */
public class Department {

    public static void main(String[] args) {

        Department coreDep = new Department("主部门");
        Department dep1 = new Department("子部门1");
        Department dep2 = new Department("子部门2");
        Department leafDep1 = new Department("叶子部门1");
        Department leafDep2 = new Department("叶子部门2");
        Department leafDep3 = new Department("叶子部门3");

        coreDep.child.add(dep1);
        coreDep.child.add(dep2);

        dep1.child.add(leafDep1);
        dep1.child.add(leafDep2);

        dep2.child.add(leafDep3);
    }

    private String name;//部门名
    private List<Department> child = new ArrayList<Department>();

    public Department(String name) {
        this.name = name;
    }

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
}
