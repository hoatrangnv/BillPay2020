/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author itbl_linh
 */
public class RevertTransaction {

    private String requestId;
    private String requestDate;
    private String orgRequestId;
    private String partnerCode;
    private String mobile;
    private String amount;
    private String signature;
    private String UserName;
    private String FunctionName;
    private String FunctionParams;

    public String getUserName() {
        return UserName;
    }

    public void setUserName(String UserName) {
        this.UserName = UserName;
    }

    public String getFunctionName() {
        return FunctionName;
    }

    public void setFunctionName(String FunctionName) {
        this.FunctionName = FunctionName;
    }

    public String getFunctionParams() {
        return FunctionParams;
    }

    public void setFunctionParams(String FunctionParams) {
        this.FunctionParams = FunctionParams;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public String getOrgRequestId() {
        return orgRequestId;
    }

    public void setOrgRequestId(String orgRequestId) {
        this.orgRequestId = orgRequestId;
    }

    public RevertTransaction() {
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getRequestDate() {
        return requestDate;
    }

    public void setRequestDate(String requestDate) {
        this.requestDate = requestDate;
    }

    public String getPartnerCode() {
        return partnerCode;
    }

    public void setPartnerCode(String partnerCode) {
        this.partnerCode = partnerCode;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }
}
