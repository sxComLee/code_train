package com.lij.design_pattern.decorate;

/*************************************************
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 装饰器设计模式
 */
public class DecoratePatternDemo {

    public static void main(String[] args) {
        SuperPerson superPerson = new SuperPerson(new Person());
        superPerson.superEat();
    }

    public static class Person {
        public void eat() {
            System.out.println("吃饭");
        }
    }

    public static class SuperPerson {

        private Person person;

        public SuperPerson(Person person) {
            this.person = person;
        }

        public void superEat() {
            System.out.println("来根烟");
            person.eat();
            System.out.println("吃甜点");
        }
    }
}
