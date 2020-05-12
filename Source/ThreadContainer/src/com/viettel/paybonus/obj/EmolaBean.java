/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author dev_linh
 */
public class EmolaBean {

    private String requestId;
    private String requestDate;
    private String enterpriseKey;
    private String enterpriseAccount;
    private String amount;
    private int transactionType;
    private String content;
    private String signature;
    private String usernameAuth;
    private String passwordAuth;
    private String orgRequestId;
    private String partnerCode;

    public String getOrgRequestId() {
        return orgRequestId;
    }

    public void setOrgRequestId(String orgRequestId) {
        this.orgRequestId = orgRequestId;
    }

    public EmolaBean() {
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

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
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

    public String getEnterpriseKey() {
        return enterpriseKey;
    }

    public void setEnterpriseKey(String enterpriseKey) {
        this.enterpriseKey = enterpriseKey;
    }

    public String getEnterpriseAccount() {
        return enterpriseAccount;
    }

    public void setEnterpriseAccount(String enterpriseAccount) {
        this.enterpriseAccount = enterpriseAccount;
    }

    public String getUsernameAuth() {
        return usernameAuth;
    }

    public void setUsernameAuth(String usernameAuth) {
        this.usernameAuth = usernameAuth;
    }

    public String getPasswordAuth() {
        return passwordAuth;
    }

    public void setPasswordAuth(String passwordAuth) {
        this.passwordAuth = passwordAuth;
    }

    public String getPartnerCode() {
        return partnerCode;
    }

    public void setPartnerCode(String partnerCode) {
        this.partnerCode = partnerCode;
    }
}
