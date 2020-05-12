/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author itbl_linh
 */
public class StockIsdnBean implements java.io.Serializable {

    private Long stockTypeId;
    private String stockTypeCode;
    private String stockTypeName;
    //thong tin ve so
    private String isdn;
    private String isdnType;
    private Long isdnStatus;
    private Long ownerType;
    private Long ownerId;
    //thong tin ve luat so dep
    private Long ruleId;
    private String ruleMaskMapping;
    private String ruleName;
    //thong tin ve mat hang
    private Long stockModelId; //thong tin luu thua -> phuc vu viec luu sau nay
    private String stockModelCode;
    private String stockModelName;
    private String accountingModelCode;
    private String accountingModelName;
    //thong tin ve gia
    private Long priceId; //thong tin luu thua -> phuc vu viec luu vao bang saleTransDetail sau nay
    private Double price; //thong tin luu thua -> phuc vu viec luu vao bang saleTransDetail sau nay
    private Double priceVat; //thong tin luu thua -> phuc vu viec luu vao bang saleTransDetail sau nay
    private Long pricePledgeAmount; //muc tien cam ket hang thang
    private Long pricePledgeTime; //thoi gian cam ket
    private Long pricePriorPay; //so thang ung truoc
    //thong tin ve lastupdate user
    private String lastUpdateUser;
    private String lastUpdateIpAddress;
    //
    private Long messageCode; //luu ma loi trong truong hop xay ra loi khi thao tac voi doi tuong nay

//    private StockModelBean stockModelBean;
    public StockIsdnBean() {
    }

    public String getAccountingModelCode() {
        return accountingModelCode;
    }

    public void setAccountingModelCode(String accountingModelCode) {
        this.accountingModelCode = accountingModelCode;
    }

    public String getAccountingModelName() {
        return accountingModelName;
    }

    public void setAccountingModelName(String accountingModelName) {
        this.accountingModelName = accountingModelName;
    }

    public Long getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(Long ownerId) {
        this.ownerId = ownerId;
    }

    public Long getOwnerType() {
        return ownerType;
    }

    public void setOwnerType(Long ownerType) {
        this.ownerType = ownerType;
    }

    public StockIsdnBean(Long stockTypeId, String isdn) {
        this.stockTypeId = stockTypeId;
        this.isdn = isdn;
    }

    public String getIsdn() {
        return isdn;
    }

    public void setIsdn(String isdn) {
        this.isdn = isdn;
    }

    public Long getStockTypeId() {
        return stockTypeId;
    }

    public void setStockTypeId(Long stockTypeId) {
        this.stockTypeId = stockTypeId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Long getPriceId() {
        return priceId;
    }

    public void setPriceId(Long priceId) {
        this.priceId = priceId;
    }

    public Double getPriceVat() {
        return priceVat;
    }

    public void setPriceVat(Double priceVat) {
        this.priceVat = priceVat;
    }

    public Long getStockModelId() {
        return stockModelId;
    }

    public void setStockModelId(Long stockModelId) {
        this.stockModelId = stockModelId;
    }

    public Long getMessageCode() {
        return messageCode;
    }

    public void setMessageCode(Long messageCode) {
        this.messageCode = messageCode;
    }

    public String getIsdnType() {
        return isdnType;
    }

    public void setIsdnType(String isdnType) {
        this.isdnType = isdnType;
    }

    public Long getRuleId() {
        return ruleId;
    }

    public void setRuleId(Long ruleId) {
        this.ruleId = ruleId;
    }

    public String getRuleMaskMapping() {
        return ruleMaskMapping;
    }

    public void setRuleMaskMapping(String ruleMaskMapping) {
        this.ruleMaskMapping = ruleMaskMapping;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public Long getIsdnStatus() {
        return isdnStatus;
    }

    public void setIsdnStatus(Long isdnStatus) {
        this.isdnStatus = isdnStatus;
    }

    public String getStockModelCode() {
        return stockModelCode;
    }

    public void setStockModelCode(String stockModelCode) {
        this.stockModelCode = stockModelCode;
    }

    public String getStockModelName() {
        return stockModelName;
    }

    public void setStockModelName(String stockModelName) {
        this.stockModelName = stockModelName;
    }

    public Long getPricePledgeAmount() {
        return pricePledgeAmount;
    }

    public void setPricePledgeAmount(Long pricePledgeAmount) {
        this.pricePledgeAmount = pricePledgeAmount;
    }

    public Long getPricePledgeTime() {
        return pricePledgeTime;
    }

    public void setPricePledgeTime(Long pricePledgeTime) {
        this.pricePledgeTime = pricePledgeTime;
    }

    public Long getPricePriorPay() {
        return pricePriorPay;
    }

    public void setPricePriorPay(Long pricePriorPay) {
        this.pricePriorPay = pricePriorPay;
    }

    public String getLastUpdateIpAddress() {
        return lastUpdateIpAddress;
    }

    public void setLastUpdateIpAddress(String lastUpdateIpAddress) {
        this.lastUpdateIpAddress = lastUpdateIpAddress;
    }

    public String getLastUpdateUser() {
        return lastUpdateUser;
    }

    public void setLastUpdateUser(String lastUpdateUser) {
        this.lastUpdateUser = lastUpdateUser;
    }

    public String getStockTypeCode() {
        return stockTypeCode;
    }

    public void setStockTypeCode(String stockTypeCode) {
        this.stockTypeCode = stockTypeCode;
    }

    public String getStockTypeName() {
        return stockTypeName;
    }

    public void setStockTypeName(String stockTypeName) {
        this.stockTypeName = stockTypeName;
    }
}
