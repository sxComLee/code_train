package com.lij.design_pattern.builder;

/**
 * 构建者设计模式
 */
public class WithoutPatternDemo {
    public static void main(String[] args) {
        Student student = new Student();
        if(true){
            System.out.println("非常复杂逻辑处理");
            student.setField1("1");
        }
        if(true){
            System.out.println("非常复杂逻辑处理");
            student.setField2("2");
        }
        if(true){
            System.out.println("复杂");
            student.setField3("3");
        }

        System.out.println(student);

    }


    public static class Student{
        private String field1;
        private String field2;
        private String field3;

        public String getField1() {
            return field1;
        }

        public void setField1(String field1) {
            this.field1 = field1;
        }

        public String getField2() {
            return field2;
        }

        public void setField2(String field2) {
            this.field2 = field2;
        }

        public String getField3() {
            return field3;
        }

        public void setField3(String field3) {
            this.field3 = field3;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "field1='" + field1 + '\'' +
                    ", field2='" + field2 + '\'' +
                    ", field3='" + field3 + '\'' +
                    '}';
        }
    }
}
