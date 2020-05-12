/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.wsfw.object;

/**
 *
 * @author dev_manhnv
 */
public class KitBatchDetailModel {
    private String Isdn;
    private Integer KitBatchId;
    private Integer PrepaidMonth;
    private String ProductCode;
    private Double MoneyDiscount;
    private Double Price;
    private Double MoneyProduct;
    private String CurrenctProductCode;
    private boolean IsExpired;

    public KitBatchDetailModel() {
    }

    public String getIsdn() {
        return Isdn;
    }

    public void setIsdn(String Isdn) {
        this.Isdn = Isdn;
    }

    public Integer getKitBatchId() {
        return KitBatchId;
    }

    public void setKitBatchId(Integer KitBatchId) {
        this.KitBatchId = KitBatchId;
    }

    public Integer getPrepaidMonth() {
        return PrepaidMonth;
    }

    public void setPrepaidMonth(Integer PrepaidMonth) {
        this.PrepaidMonth = PrepaidMonth;
    }

    public String getProductCode() {
        return ProductCode;
    }

    public void setProductCode(String ProductCode) {
        this.ProductCode = ProductCode;
    }

    public Double getMoneyProduct() {
        return MoneyProduct;
    }

    public void setMoneyProduct(Double MoneyProduct) {
        this.MoneyProduct = MoneyProduct;
    }

    public String getCurrenctProductCode() {
        return CurrenctProductCode;
    }

    public void setCurrenctProductCode(String CurrenctProductCode) {
        this.CurrenctProductCode = CurrenctProductCode;
    }

    public Double getMoneyDiscount() {
        return MoneyDiscount;
    }

    public void setMoneyDiscount(Double MoneyDiscount) {
        this.MoneyDiscount = MoneyDiscount;
    }

    public Double getPrice() {
        return Price;
    }

    public void setPrice(Double Price) {
        this.Price = Price;
    }

    public boolean isIsExpired() {
        return IsExpired;
    }

    public void setIsExpired(boolean IsExpired) {
        this.IsExpired = IsExpired;
    }
    
}
