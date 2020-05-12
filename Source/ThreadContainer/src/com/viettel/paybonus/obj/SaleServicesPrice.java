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
public class SaleServicesPrice implements java.io.Serializable {

    private Long saleServicesPriceId;
    private Long saleServicesId;
    private Double price;//Gia DVBH
    private Long status;
    private String description;
    private Date staDate;
    private Date endDate;
    private Double vat;//Ti le VAT
    private Date createDate;
    private String username;
    private String pricePolicy;
    private String currency;

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }
    // Constructors
    private String pricePolicyName;

    /**
     * default constructor
     */
    public SaleServicesPrice() {
    }

    /**
     * minimal constructor
     */
    public SaleServicesPrice(Long id, Double price, Long status) {
        this.saleServicesPriceId = id;
        this.price = price;
        this.status = status;
    }

    /**
     * full constructor
     */
    public SaleServicesPrice(Long id, Double price, Long status,
            String description, Date staDate, Date endDate, Long saleServicesId) {
        this.saleServicesPriceId = id;
        this.price = price;
        this.status = status;
        this.description = description;
        this.staDate = staDate;
        this.endDate = endDate;
        this.saleServicesId = saleServicesId;
    }

    // Property accessors
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Long getSaleServicesPriceId() {
        return saleServicesPriceId;
    }

    public void setSaleServicesPriceId(Long saleServicesPriceId) {
        this.saleServicesPriceId = saleServicesPriceId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Double getVat() {
        return vat;
    }

    public void setVat(Double vat) {
        this.vat = vat;
    }

    public Long getSaleServicesId() {
        return saleServicesId;
    }

    public void setSaleServicesId(Long saleServicesId) {
        this.saleServicesId = saleServicesId;
    }

    public Date getStaDate() {
        return staDate;
    }

    public void setStaDate(Date staDate) {
        this.staDate = staDate;
    }

    public Long getStatus() {
        return status;
    }

    public void setStatus(Long status) {
        this.status = status;
    }

    public String getPricePolicy() {
        return pricePolicy;
    }

    public void setPricePolicy(String pricePolicy) {
        this.pricePolicy = pricePolicy;
    }

    public String getPricePolicyName() {
        return pricePolicyName;
    }

    public void setPricePolicyName(String pricePolicyName) {
        this.pricePolicyName = pricePolicyName;
    }
}
