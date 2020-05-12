/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import com.viettel.cluster.agent.integration.Record;
import java.util.Date;

/**
 *
 * @author Huynq13
 */
public class BounesMonthly implements Record {

    private Long actionAuditId;
    private String isdn;
    private Date createTime;
    private String createStaff;
    private String checkInfo;
    private String eMolaIsdn;
    private Long countProcess;
    private String eWalletErrCode;
    private String threadName;
    private String nodeName;
    private String clusterName;
    private String idBatch;
    private Long id;
    private String resultCode;
    private String description;
    private Long duration;
    private String imei;
    private int bonusType;

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

    public int getBonusType() {
        return bonusType;
    }

    public void setBonusType(int bonusType) {
        this.bonusType = bonusType;
    }

    
    public Long getActionAuditId() {
        return actionAuditId;
    }

    public void setActionAuditId(Long actionAuditId) {
        this.actionAuditId = actionAuditId;
    }

    public String geteMolaIsdn() {
        return eMolaIsdn;
    }

    public void seteMolaIsdn(String eMolaIsdn) {
        this.eMolaIsdn = eMolaIsdn;
    }

    public String getIsdn() {
        return isdn;
    }

    public void setIsdn(String isdn) {
        this.isdn = isdn;
    }

    public String getCheckInfo() {
        return checkInfo;
    }

    public void setCheckInfo(String checkInfo) {
        this.checkInfo = checkInfo;
    }

    public BounesMonthly() {
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getCreateStaff() {
        return createStaff;
    }

    public void setCreateStaff(String createStaff) {
        this.createStaff = createStaff;
    }

    public Long getCountProcess() {
        return countProcess;
    }

    public void setCountProcess(Long countProcess) {
        this.countProcess = countProcess;
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String geteWalletErrCode() {
        return eWalletErrCode;
    }

    public void seteWalletErrCode(String eWalletErrCode) {
        this.eWalletErrCode = eWalletErrCode;
    }
}
