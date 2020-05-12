/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import java.util.Date;

/**
 *
 * @author Huynq13
 */
public class SubInfo {

    private String isdnSub;
    private String productCode;
    private String status;
    private String actStatus;
    private Date activeDate;
    private String imsi;
    private String serial;
    private Long subId;
    private Long custId;

    public Long getCustId() {
        return custId;
    }

    public void setCustId(Long custId) {
        this.custId = custId;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    public String getSerial() {
        return serial;
    }

    public void setSerial(String serial) {
        this.serial = serial;
    }

    public Long getSubId() {
        return subId;
    }

    public void setSubId(Long subId) {
        this.subId = subId;
    }

    public String getActStatus() {
        return actStatus;
    }

    public void setActStatus(String actStatus) {
        this.actStatus = actStatus;
    }

    public Date getActiveDate() {
        return activeDate;
    }

    public void setActiveDate(Date activeDate) {
        this.activeDate = activeDate;
    }

    public String getIsdnSub() {
        return isdnSub;
    }

    public void setIsdnSub(String isdnSub) {
        this.isdnSub = isdnSub;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
