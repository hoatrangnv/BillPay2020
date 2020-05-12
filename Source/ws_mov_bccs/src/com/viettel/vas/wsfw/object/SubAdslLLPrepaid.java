/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.wsfw.object;

import java.util.Date;

/**
 *
 * @author Huynq13
 */
public class SubAdslLLPrepaid {

    private Long subAdslLlPrepaidId;
    private Long subId;
    private Long contractId;
    private String account;
    private String newProductCode;
    private Date createTime;
    private Date expireTime;
    private String createUser;
    private String createShop;
    private Long saleTransId;
    private Long prepaidType;
    private Long warningCount;
    private Date blockTime;
    private String description;
    private String idBatch;
    private String message;
    private String actStatus;
    private Long id;
    private String telFax;

    public Long getPrepaidType() {
        return prepaidType;
    }

    public void setPrepaidType(Long prepaidType) {
        this.prepaidType = prepaidType;
    }

    public Long getSubAdslLlPrepaidId() {
        return subAdslLlPrepaidId;
    }

    public void setSubAdslLlPrepaidId(Long subAdslLlPrepaidId) {
        this.subAdslLlPrepaidId = subAdslLlPrepaidId;
    }

    public Long getSubId() {
        return subId;
    }

    public void setSubId(Long subId) {
        this.subId = subId;
    }

    public Long getContractId() {
        return contractId;
    }

    public void setContractId(Long contractId) {
        this.contractId = contractId;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getNewProductCode() {
        return newProductCode;
    }

    public void setNewProductCode(String newProductCode) {
        this.newProductCode = newProductCode;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(Date expireTime) {
        this.expireTime = expireTime;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public String getCreateShop() {
        return createShop;
    }

    public void setCreateShop(String createShop) {
        this.createShop = createShop;
    }

    public Long getSaleTransId() {
        return saleTransId;
    }

    public void setSaleTransId(Long saleTransId) {
        this.saleTransId = saleTransId;
    }

    public Long getWarningCount() {
        return warningCount;
    }

    public void setWarningCount(Long warningCount) {
        this.warningCount = warningCount;
    }

    public Date getBlockTime() {
        return blockTime;
    }

    public void setBlockTime(Date blockTime) {
        this.blockTime = blockTime;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getIdBatch() {
        return idBatch;
    }

    public void setIdBatch(String idBatch) {
        this.idBatch = idBatch;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public SubAdslLLPrepaid() {
    }

    public String getActStatus() {
        return actStatus;
    }

    public void setActStatus(String actStatus) {
        this.actStatus = actStatus;
    }

    public String getTelFax() {
        return telFax;
    }

    public void setTelFax(String telFax) {
        this.telFax = telFax;
    }
}
