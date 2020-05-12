/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.obj;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 * @author kdvt_tungtt8
 * @version x.x
 * @since Dec 18, 2012
 */
public class SubscriberPost {

    protected Long subId; // SUB_ID
    protected Long contractId; // CONTRACT_ID
    protected String productCode; // PRODUCT_CODE
    protected String actStatus; // ACT_STATUS
    protected Date staDatetime; // STA_DATETIME
    protected Date endDatetime; // END_DATETIME
    protected Integer status;  // STATUS
//    protected Date createDate;
//    protected String subType;
//    protected Long quota;
//    protected String promotionCode;
//    protected String regType;
//    protected Long reasonDepositId;
//    protected Long deposit;
//    protected Long quotaOver;
//    protected Long isNewSub;
//    Map resourceMap;
//    protected Map attributeMap;
//    protected Map vasMap;
//    protected Map vasCheckBoxMap;
//    protected Long staffId;
//    protected String staffName;
//    protected Date changeDatetime;
    protected String serial; // SERIAL
    protected String imsi; // IMSI
//    protected String offerName;
    protected Long custId;  // CUST_ID
//    protected String custName;
//    protected List<SubRelProduct> lstVasActiveSuccess;
//    protected List<SubRelProduct> lstVasActiverFail;
//    protected List<SubRelProduct> lstVasNotActive;
//    protected Long actionLogPrId;
    protected Integer numResetZone; // NUM_RESET_ZONE
    protected String language ;

    public SubscriberPost() {
    }

    public String getActStatus() {
        return actStatus;
    }

    public void setActStatus(String actStatus) {
        this.actStatus = actStatus;
    }

    public Long getContractId() {
        return contractId;
    }

    public void setContractId(Long contractId) {
        this.contractId = contractId;
    }

    public Long getCustId() {
        return custId;
    }

    public void setCustId(Long custId) {
        this.custId = custId;
    }

    public Date getEndDatetime() {
        return endDatetime;
    }

    public void setEndDatetime(Date endDatetime) {
        this.endDatetime = endDatetime;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    public Integer getNumResetZone() {
        return numResetZone;
    }

    public void setNumResetZone(Integer numResetZone) {
        this.numResetZone = numResetZone;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getSerial() {
        return serial;
    }

    public void setSerial(String serial) {
        this.serial = serial;
    }

    public Date getStaDatetime() {
        return staDatetime;
    }

    public void setStaDatetime(Date staDatetime) {
        this.staDatetime = staDatetime;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Long getSubId() {
        return subId;
    }

    public void setSubId(Long subId) {
        this.subId = subId;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }
    
    
}
