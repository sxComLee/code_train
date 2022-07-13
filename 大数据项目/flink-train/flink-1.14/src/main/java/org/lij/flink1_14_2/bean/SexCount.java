package org.lij.flink1_14_2.bean;

/**
 * Description:
 *
 * @author lij
 * @date 2022-01-29 16:21
 */
public class SexCount {
    private String sex;
    private int count;

    public SexCount() {
    }

    public SexCount(String sex, int count) {
        this.count = count;
        this.sex = sex;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    @Override
    public String toString() {
        return "SexCount{" + "sex='" + sex + '\'' + ", count=" + count + '}';
    }
}
