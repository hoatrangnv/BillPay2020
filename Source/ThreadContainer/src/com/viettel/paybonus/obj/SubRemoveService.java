/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author kdvt_tungtt8
 */
public class SubRemoveService {

    private long id;
    private long subId;
    private String msisdn;
    private String relProductCode;
    private String serviceType;

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getRelProductCode() {
        return relProductCode;
    }

    public void setRelProductCode(String relProductCode) {
        this.relProductCode = relProductCode;
    }

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public long getSubId() {
        return subId;
    }

    public void setSubId(long subId) {
        this.subId = subId;
    }
}
