package com.viettel.paybonus.obj;

public class EwalletLog {

    private long ationAuditId;
    private String staffCode;
    private int channelTypeId;
    private String mobile;
    private String transId;
    private String actionCode;
    private long amount;
    private String functionName;
    private String url;
    private String userName;
    private String request;
    private String respone;
    private long duration;
    private String errorCode;
    private String description;
    private String isdn;
    private String requestId;
    private Double amountEmola;
    private long bonusType;

    public long getBonusType() {
        return bonusType;
    }

    public void setBonusType(long bonusType) {
        this.bonusType = bonusType;
    }

    public Double getAmountEmola() {
        return amountEmola;
    }

    public void setAmountEmola(Double amountEmola) {
        this.amountEmola = amountEmola;
    }

    public String getIsdn() {
        return isdn;
    }

    public void setIsdn(String isdn) {
        this.isdn = isdn;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public EwalletLog() {
    }

    public String getActionCode() {
        return actionCode;
    }

    public void setActionCode(String actionCode) {
        this.actionCode = actionCode;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public long getAtionAuditId() {
        return ationAuditId;
    }

    public void setAtionAuditId(long ationAuditId) {
        this.ationAuditId = ationAuditId;
    }

    public int getChannelTypeId() {
        return channelTypeId;
    }

    public void setChannelTypeId(int channelTypeId) {
        this.channelTypeId = channelTypeId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getRespone() {
        return respone;
    }

    public void setRespone(String respone) {
        this.respone = respone;
    }

    public String getStaffCode() {
        return staffCode;
    }

    public void setStaffCode(String staffCode) {
        this.staffCode = staffCode;
    }

    public String getTransId() {
        return transId;
    }

    public void setTransId(String transId) {
        this.transId = transId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }
}