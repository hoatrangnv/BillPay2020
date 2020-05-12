/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.wsfw.object;

/**
 *
 * @author itbl_linh
 */
public class ProductConnectKit {

    private String productCode;
    private long offerId;
    private String moneyFee;
    private String pricePlan;
    private String ppBuyMore;

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public long getOfferId() {
        return offerId;
    }

    public void setOfferId(long offerId) {
        this.offerId = offerId;
    }

    public String getMoneyFee() {
        return moneyFee;
    }

    public void setMoneyFee(String moneyFee) {
        this.moneyFee = moneyFee;
    }

    public String getPricePlan() {
        return pricePlan;
    }

    public void setPricePlan(String pricePlan) {
        this.pricePlan = pricePlan;
    }

    public String getPpBuyMore() {
        return ppBuyMore;
    }

    public void setPpBuyMore(String ppBuyMore) {
        this.ppBuyMore = ppBuyMore;
    }

    @Override
    public String toString() {
        return "ProductConnectKit{" + "productCode=" + productCode + ", offerId=" + offerId + ", moneyFee=" + moneyFee + ", pricePlan=" + pricePlan + ", ppBuyMore=" + ppBuyMore + '}';
    }
}
