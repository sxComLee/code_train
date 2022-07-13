package com.jiang.flink.program.bean;

/**
 * @ClassName RegionInfo
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-11-15 10:37
 * @Version 1.0
 */
public class RegionInfo {
    String regionInfo = "";
    String value = "";
    String innerCode = "";

    public String getRegionInfo() {
        return regionInfo;
    }

    public void setRegionInfo(String regionInfo) {
        this.regionInfo = regionInfo;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getInnerCode() {
        return innerCode;
    }

    public void setInnerCode(String innerCode) {
        this.innerCode = innerCode;
    }

    public RegionInfo(String regionInfo, String value, String innerCode) {
        this.regionInfo = regionInfo;
        this.value = value;
        this.innerCode = innerCode;
    }
}
