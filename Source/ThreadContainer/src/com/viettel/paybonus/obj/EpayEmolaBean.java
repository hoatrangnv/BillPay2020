/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author dev_linh
 */
public class EpayEmolaBean {

    private String requestId;
    private String transactionDate;
    private String srcStoreCode;
    private String amount;
    private String description;
    private String signature;

    public EpayEmolaBean() {
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public String getTransactionDate() {
        return transactionDate;
    }

    public void setTransactionDate(String transactionDate) {
        this.transactionDate = transactionDate;
    }

    public String getSrcStoreCode() {
        return srcStoreCode;
    }

    public void setSrcStoreCode(String srcStoreCode) {
        this.srcStoreCode = srcStoreCode;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
