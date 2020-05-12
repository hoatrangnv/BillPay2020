/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import com.viettel.cluster.agent.integration.Record;
import java.util.Date;

/**
 *
 * @author dev_linh
 */
public class KitElitePrepaid implements Record {

    private String idBatch;
    private String clusterName;
    private String nodeName;
    private String threadName;
    private String resultCode;
    private String description;
    private Long id;
    private String isdn;
    private int prepaidMonth;
    private int remainMonth;
    private Date createTime;
    private Date remainTime;
    private Long status;
    private Long kitBatchId;
    private int prepaidType;
    private Long duration;
    private String createUser;
    private String productCode;
    private String requestId;
    private String resultTopup;
    private Long actionAuditId;
    private String moneyProduct;
    private Long moId;

    public Long getMoId() {
        return moId;
    }

    public void setMoId(Long moId) {
        this.moId = moId;
    }

    public String getMoneyProduct() {
        return moneyProduct;
    }

    public void setMoneyProduct(String moneyProduct) {
        this.moneyProduct = moneyProduct;
    }

    public Long getActionAuditId() {
        return actionAuditId;
    }

    public void setActionAuditId(Long actionAuditId) {
        this.actionAuditId = actionAuditId;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getResultTopup() {
        return resultTopup;
    }

    public void setResultTopup(String resultTopup) {
        this.resultTopup = resultTopup;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public int getPrepaidType() {
        return prepaidType;
    }

    public void setPrepaidType(int prepaidType) {
        this.prepaidType = prepaidType;
    }

    public String getIdBatch() {
        return idBatch;
    }

    public void setIdBatch(String idBatch) {
        this.idBatch = idBatch;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getIsdn() {
        return isdn;
    }

    public void setIsdn(String isdn) {
        this.isdn = isdn;
    }

    public int getPrepaidMonth() {
        return prepaidMonth;
    }

    public void setPrepaidMonth(int prepaidMonth) {
        this.prepaidMonth = prepaidMonth;
    }

    public int getRemainMonth() {
        return remainMonth;
    }

    public void setRemainMonth(int remainMonth) {
        this.remainMonth = remainMonth;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getRemainTime() {
        return remainTime;
    }

    public void setRemainTime(Date remainTime) {
        this.remainTime = remainTime;
    }

    public Long getKitBatchId() {
        return kitBatchId;
    }

    public void setKitBatchId(Long kitBatchId) {
        this.kitBatchId = kitBatchId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Long getStatus() {
        return status;
    }

    public void setStatus(Long status) {
        this.status = status;
    }

    @Override
    public String getID() {
        return "" + this.id;
    }

    @Override
    public String getBatchId() {
        return this.idBatch;
    }

    @Override
    public void setBatchId(String batchId) {
        this.idBatch = batchId;
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

    @Override
    public String getResultCode() {
        return this.resultCode;
    }

    @Override
    public void setResultCode(String resultCode) {
        this.resultCode = resultCode;
    }
}
