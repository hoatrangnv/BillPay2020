/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import com.viettel.cluster.agent.integration.Record;
import java.sql.Timestamp;
import java.util.Date;

/**
 *
 * @author itbl_jony
 */
public class BranchPromotionSub implements Record {

    private Long actionAuditId;
    private String isdn;
    private String createUser;
    private Date createTime;
    private String productCode;
    private Date exprieTime;
    private String idBatch;
    private String threadName;
    private String nodeName;
    private String clusterName;
    private String resultCode;
    private String description;
    private Long moneyFee;
    private Date lastExprieTime;
    private Date newExprieTime;
    private String actionType;
    private String lastBtsCode;
    private Timestamp processTime;
    private Long countProcess;
    private Date lastProcess;

    public Long getMoneyFee() {
        return moneyFee;
    }

    public void setMoneyFee(Long moneyFee) {
        this.moneyFee = moneyFee;
    }

    public Date getLastExprieTime() {
        return lastExprieTime;
    }

    public void setLastExprieTime(Date lastExprieTime) {
        this.lastExprieTime = lastExprieTime;
    }

    public Date getNewExprieTime() {
        return newExprieTime;
    }

    public void setNewExprieTime(Date newExprieTime) {
        this.newExprieTime = newExprieTime;
    }

    public Long getActionAuditId() {
        return actionAuditId;
    }

    public void setActionAuditId(Long actionAuditId) {
        this.actionAuditId = actionAuditId;
    }

    public String getIsdn() {
        return isdn;
    }

    public void setIsdn(String isdn) {
        this.isdn = isdn;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public Date getExprieTime() {
        return exprieTime;
    }

    public void setExprieTime(Date exprieTime) {
        this.exprieTime = exprieTime;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String getID() {
        return "" + this.actionAuditId;
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

    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = actionType;
    }

    public String getLastBtsCode() {
        return lastBtsCode;
    }

    public void setLastBtsCode(String lastBtsCode) {
        this.lastBtsCode = lastBtsCode;
    }

    public Timestamp getProcessTime() {
        return processTime;
    }

    public void setProcessTime(Timestamp processTime) {
        this.processTime = processTime;
    }

    public Long getCountProcess() {
        return countProcess;
    }

    public void setCountProcess(Long countProcess) {
        this.countProcess = countProcess;
    }

    public Date getLastProcess() {
        return lastProcess;
    }

    public void setLastProcess(Date lastProcess) {
        this.lastProcess = lastProcess;
    }
}
