/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author itbl_linh
 */
public class PaymentVoucher {

    private String requestId;
    private String requestDate;
    private String partnerCode;
    private String mobile;
    private String amount;
    private String voucherCode;
    private int transactionType;
    private String content;
    private String signature;
    private String UsernameAuth;
    private String PasswordAuth;
    private String orgRequestId;

    public String getOrgRequestId() {
        return orgRequestId;
    }

    public void setOrgRequestId(String orgRequestId) {
        this.orgRequestId = orgRequestId;
    }

    public PaymentVoucher() {
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

    public String getVoucherCode() {
        return voucherCode;
    }

    public void setVoucherCode(String voucherCode) {
        this.voucherCode = voucherCode;
    }

    public int getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(int transactionType) {
        this.transactionType = transactionType;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public String getUsernameAuth() {
        return UsernameAuth;
    }

    public void setUsernameAuth(String usernameAuth) {
        this.UsernameAuth = usernameAuth;
    }

    public String getPasswordAuth() {
        return PasswordAuth;
    }

    public void setPasswordAuth(String passwordAuth) {
        this.PasswordAuth = passwordAuth;
    }
}
