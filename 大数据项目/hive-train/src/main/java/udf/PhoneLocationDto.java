package udf;

import java.io.Serializable;

public class PhoneLocationDto implements Serializable {

    private static final long serialVersionUID = 1L;

    private int Id;
    private String MobileNumber;
    private String MobileArea;
    private String MobileType;
    private String AreaCode;
    private String PostCode;

    public String getAreaCode() {
        return AreaCode;
    }

    public void setAreaCode(String areaCode) {
        this.AreaCode = areaCode;
    }

    public int getID() {
        return Id;
    }

    public void setID(int ID) {
        this.Id = ID;
    }

    public String getMobileArea() {
        return MobileArea;
    }

    public void setMobileArea(String mobileArea) {
        this.MobileArea = mobileArea;
    }

    public String getMobileNumber() {
        return MobileNumber;
    }

    public void setMobileNumber(String mobileNumber) {
        this.MobileNumber = mobileNumber;
    }

    public String getMobileType() {
        return MobileType;
    }

    public void setMobileType(String mobileType) {
        this.MobileType = mobileType;
    }

    public String getPostCode() {
        return PostCode;
    }

    public void setPostCode(String postCode) {
        this.PostCode = postCode;
    }
}