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
public class KitBatchInfo implements Record {

    private Long kitBatchId;
    private String idBatch;
    private String clusterName;
    private String nodeName;
    private String threadName;
    private String resultCode;
    private String description;
    private String createUser;
    private String unitCode;
    private Long status;
    private String commissionUser;
    private String commissionAccount;
    private Long custId;
    private String numberType;
    private String payType;
    private String bankName;
    private String bankTransCode;
    private String bankTransAmount;
    private String emolaAccount;
    private String emolaVoucherCode;
    private String addMonth;
    private String fileName;
    private String filePath;
    private String groupStatus;
    private long totalSuccess;
    private long totalFail;
    private long totalMoney;
    private Date createTime;
    private Long duration;
    private String expireTimeGroup;
    private String channelType;
    private Long extendFromKitBatchId;
    private String groupName;
    private String referenceId;
    private String enterpriseWallet;
    private String bankAccount;
    private String bankNib;
    private String bankNameDirectDebit;

    public String getBankAccount() {
        return bankAccount;
    }

    public void setBankAccount(String bankAccount) {
        this.bankAccount = bankAccount;
    }

    public String getBankNib() {
        return bankNib;
    }

    public void setBankNib(String bankNib) {
        this.bankNib = bankNib;
    }

    public String getBankNameDirectDebit() {
        return bankNameDirectDebit;
    }

    public void setBankNameDirectDebit(String bankNameDirectDebit) {
        this.bankNameDirectDebit = bankNameDirectDebit;
    }

    public String getReferenceId() {
        return referenceId;
    }

    public void setReferenceId(String referenceId) {
        this.referenceId = referenceId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public Long getExtendFromKitBatchId() {
        return extendFromKitBatchId;
    }

    public void setExtendFromKitBatchId(Long extendFromKitBatchId) {
        this.extendFromKitBatchId = extendFromKitBatchId;
    }

    public String getExpireTimeGroup() {
        return expireTimeGroup;
    }

    public void setExpireTimeGroup(String expireTimeGroup) {
        this.expireTimeGroup = expireTimeGroup;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Long getKitBatchId() {
        return kitBatchId;
    }

    public void setKitBatchId(Long kitBatchId) {
        this.kitBatchId = kitBatchId;
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

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public String getUnitCode() {
        return unitCode;
    }

    public void setUnitCode(String unitCode) {
        this.unitCode = unitCode;
    }

    public Long getStatus() {
        return status;
    }

    public void setStatus(Long status) {
        this.status = status;
    }

    public String getCommissionUser() {
        return commissionUser;
    }

    public void setCommissionUser(String commissionUser) {
        this.commissionUser = commissionUser;
    }

    public String getCommissionAccount() {
        return commissionAccount;
    }

    public void setCommissionAccount(String commissionAccount) {
        this.commissionAccount = commissionAccount;
    }

    public Long getCustId() {
        return custId;
    }

    public void setCustId(Long custId) {
        this.custId = custId;
    }

    public String getNumberType() {
        return numberType;
    }

    public void setNumberType(String numberType) {
        this.numberType = numberType;
    }

    public String getPayType() {
        return payType;
    }

    public void setPayType(String payType) {
        this.payType = payType;
    }

    public String getBankName() {
        return bankName;
    }

    public void setBankName(String bankName) {
        this.bankName = bankName;
    }

    public String getBankTransCode() {
        return bankTransCode;
    }

    public void setBankTransCode(String bankTransCode) {
        this.bankTransCode = bankTransCode;
    }

    public String getBankTransAmount() {
        return bankTransAmount;
    }

    public void setBankTransAmount(String bankTransAmount) {
        this.bankTransAmount = bankTransAmount;
    }

    public String getEmolaAccount() {
        return emolaAccount;
    }

    public void setEmolaAccount(String emolaAccount) {
        this.emolaAccount = emolaAccount;
    }

    public String getEmolaVoucherCode() {
        return emolaVoucherCode;
    }

    public void setEmolaVoucherCode(String emolaVoucherCode) {
        this.emolaVoucherCode = emolaVoucherCode;
    }

    public String getAddMonth() {
        return addMonth;
    }

    public void setAddMonth(String addMonth) {
        this.addMonth = addMonth;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getGroupStatus() {
        return groupStatus;
    }

    public void setGroupStatus(String groupStatus) {
        this.groupStatus = groupStatus;
    }

    public long getTotalSuccess() {
        return totalSuccess;
    }

    public void setTotalSuccess(long totalSuccess) {
        this.totalSuccess = totalSuccess;
    }

    public long getTotalFail() {
        return totalFail;
    }

    public void setTotalFail(long totalFail) {
        this.totalFail = totalFail;
    }

    public long getTotalMoney() {
        return totalMoney;
    }

    public void setTotalMoney(long totalMoney) {
        this.totalMoney = totalMoney;
    }

    @Override
    public String getID() {
        return "" + this.kitBatchId;
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

    public String getChannelType() {
        return channelType;
    }

    public void setChannelType(String channelType) {
        this.channelType = channelType;
    }

    public String getEnterpriseWallet() {
        return enterpriseWallet;
    }

    public void setEnterpriseWallet(String enterpriseWallet) {
        this.enterpriseWallet = enterpriseWallet;
    }
}
