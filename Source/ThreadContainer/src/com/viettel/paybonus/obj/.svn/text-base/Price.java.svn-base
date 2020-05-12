/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import java.util.Date;

/**
 *
 * @author itbl_linh
 */
public class Price implements java.io.Serializable {

    private Long priceId;
    private Long stockModelId;
    private String type;
    private Double price;
    private String priceName;
    private Long status;
    private String description;
    private Date staDate;
    private Date endDate;
    private Double vat;
    private Date createDate;
    private String userName;
    private String areaCode;
    private Long channelTypeId;
    private String pricePolicy;
    private Long pledgeAmount; //muc tien cam ket hang thang
    private Long pledgeTime; //thoi gian cam ket
    private Long priorPay; //so thang ung truoc
    private String typeName; //ten loai gia
    private String pricePolicyName; //ten chinh sach gia
    private String priceAndDessciption; //truong chua gia + mo ta gia (phuc vu viec hien thi)
    private String currency;

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Price(Long priceId, Long stockModelId, Double price,
            Long status, Date staDate, Date endDate,
            Double vat, Date createDate, String userName,
            String type, String pricePolicy,
            String typeName, String pricePolicyName) {
        this.priceId = priceId;
        this.stockModelId = stockModelId;
        this.price = price;
        this.status = status;
        this.staDate = staDate;
        this.endDate = endDate;
        this.vat = vat;
        this.createDate = createDate;
        this.userName = userName;
        this.type = type;
        this.pricePolicy = pricePolicy;
        this.typeName = typeName;
        this.pricePolicyName = pricePolicyName;
    }

    public String getPriceName() {
        return priceName;
    }

    public void setPriceName(String priceName) {
        this.priceName = priceName;
    }

    public Long getPledgeAmount() {
        return pledgeAmount;
    }

    public void setPledgeAmount(Long pledgeAmount) {
        this.pledgeAmount = pledgeAmount;
    }

    public Long getPledgeTime() {
        return pledgeTime;
    }

    public void setPledgeTime(Long pledgeTime) {
        this.pledgeTime = pledgeTime;
    }

    public Long getPriorPay() {
        return priorPay;
    }

    public void setPriorPay(Long priorPay) {
        this.priorPay = priorPay;
    }

    public String getPriceAndDessciption() {
        return priceAndDessciption;
    }

    public void setPriceAndDessciption(String priceAndDessciption) {
        this.priceAndDessciption = priceAndDessciption;
    }

    public Price(Long priceId, String priceAndDessciption) {
        this.priceId = priceId;
        this.priceAndDessciption = priceAndDessciption;
    }

    // Constructors
    /**
     * default constructor
     */
    public Price() {
    }

    /**
     * minimal constructor
     */
    public Price(Long priceId, String type, Double price, Date staDate, String username, Long status) {
        this.priceId = priceId;
        this.type = type;
        this.price = price;
        this.staDate = staDate;
        this.status = status;
    }

    /**
     * full constructor
     */
    public Price(Long priceId, Long goodsDefId, String areaCode, Long partnerTypeId, String type, Double price, Date staDate, Date endDate, String username, Long status, Long vat, String description) {
        this.priceId = priceId;
        this.type = type;
        this.price = price;
        this.staDate = staDate;
        this.endDate = endDate;
        this.status = status;
        this.description = description;
    }

    // Property accessors
    public String getPricePolicy() {
        return pricePolicy;
    }

    public void setPricePolicy(String pricePolicy) {
        this.pricePolicy = pricePolicy;
    }

    public String getAreaCode() {
        return areaCode;
    }

    public void setAreaCode(String areaCode) {
        this.areaCode = areaCode;
    }

    public Long getChannelTypeId() {
        return channelTypeId;
    }

    public void setChannelTypeId(Long channelTypeId) {
        this.channelTypeId = channelTypeId;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Double getVat() {
        return vat;
    }

    public void setVat(Double vat) {
        this.vat = vat;
    }

    public Long getPriceId() {
        return this.priceId;
    }

    public void setPriceId(Long priceId) {
        this.priceId = priceId;
    }

    public Long getStockModelId() {
        return this.stockModelId;
    }

    public void setStockModelId(Long goodsDefId) {
        this.stockModelId = goodsDefId;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Double getPrice() {
        return this.price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Date getStaDate() {
        return this.staDate;
    }

    public void setStaDate(Date staDate) {
        this.staDate = staDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public Long getStatus() {
        return this.status;
    }

    public void setStatus(Long status) {
        this.status = status;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getPricePolicyName() {
        return pricePolicyName;
    }

    public void setPricePolicyName(String pricePolicyName) {
        this.pricePolicyName = pricePolicyName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }
}
