package com.lij.design_pattern.composite;

import java.util.ArrayList;
import java.util.List;

/**
 * 组合设计模式
 */
public class CompositePatternDemo {
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

        //需求一： 删除主部门
        coreDep.remove();
        //需求二： 重命名
        coreDep.rename("奈学");
    }

    public static class Department {
        private String name;
        private List<Department> child = new ArrayList<Department>();

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

        public void remove() {
            if (this.child.size() > 0) {
                for (Department department : this.child) {
                    department.remove();
                }
            }
            System.out.println("删除部门" + name);
        }

        public void rename(String preStr) {
            if (this.child.size() > 0) {
                for (Department department : this.child) {
                    department.rename(preStr);
                }
            }
            System.out.println(preStr + "：" + name);
        }
    }
}
