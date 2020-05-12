/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import com.viettel.cluster.agent.integration.Record;

/**
 *
 * @author dev_manhnv
 */
public class KitBatchGroup  implements Record {
    private long id;
    private String batchId;
    private Integer kitBatchId;
    private String transId;
    private String isdn;
    private String productCode;
    private String enterpriseWallet;
    private String createUser;
    private Integer prepaidMonth;
    private Double price;
    private Double discount;
    private String description;
    private String resultCode;
    private Integer actionType;
    private String clusterName;
    private String nodeName;
    private String threadName;
    private String requestId;
    private Integer status;

    public KitBatchGroup() {
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public Integer getKitBatchId() {
        return kitBatchId;
    }

    public void setKitBatchId(Integer kitBatchId) {
        this.kitBatchId = kitBatchId;
    }

    public String getTransId() {
        return transId;
    }

    public void setTransId(String transId) {
        this.transId = transId;
    }

    public String getIsdn() {
        return isdn;
    }

    public void setIsdn(String isdn) {
        this.isdn = isdn;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getEnterpriseWallet() {
        return enterpriseWallet;
    }

    public void setEnterpriseWallet(String enterpriseWallet) {
        this.enterpriseWallet = enterpriseWallet;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public Integer getPrepaidMonth() {
        return prepaidMonth;
    }

    public void setPrepaidMonth(Integer prepaidMonth) {
        this.prepaidMonth = prepaidMonth;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Double getDiscount() {
        return discount;
    }

    public void setDiscount(Double discount) {
        this.discount = discount;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getResultCode() {
        return resultCode;
    }

    public void setResultCode(String resultCode) {
        this.resultCode = resultCode;
    }

    public Integer getActionType() {
        return actionType;
    }

    public void setActionType(Integer actionType) {
        this.actionType = actionType;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    
    @Override
    public String getID() {
        return "" + this.id;
    }

    @Override
    public String getClusterName() {
        return this.clusterName;
    }

    @Override
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public String getNodeName() {
        return this.nodeName;
    }

    @Override
    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public String getTheadName() {
        return this.threadName;
    }

    @Override
    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }
    
}
