/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import com.viettel.cluster.agent.integration.Record;
import java.sql.Timestamp;

/**
 *
 * @author Huynq13
 */
public class VipSubDetail implements Record {

    public static String VIP_SUB_DETAIL_ID = "VIP_SUB_DETAIL_ID";
    public static String VIP_SUB_INFO_ID = "VIP_SUB_INFO_ID";
    public static String ISDN = "ISDN";
    public static String NEXT_PROCESS_TIME = "NEXT_PROCESS_TIME";
    public static String HIS_NEXT_PROCESS_TIME = "HIS_NEXT_PROCESS_TIME";
    public static String CYCLE_TYPE = "CYCLE_TYPE";
    public static String MONEY_ACC = "MONEY_ACC";
    public static String MONEY_VALUE = "MONEY_VALUE";
    public static String SMS_ACC = "SMS_ACC";
    public static String SMS_VALUE = "SMS_VALUE";
    public static String SMS_OUT_ACC = "SMS_OUT_ACC";
    public static String SMS_OUT_VALUE = "SMS_OUT_VALUE";
    public static String DATA_ACC = "DATA_ACC";
    public static String DATA_VALUE = "DATA_VALUE";
    public static String VOICE_ACC = "VOICE_ACC";
    public static String VOICE_VALUE = "VOICE_VALUE";
    public static String VOICE_OUT_ACC = "VOICE_OUT_ACC";
    public static String VOICE_OUT_VALUE = "VOICE_OUT_VALUE";
    public static String CREATE_TIME = "CREATE_TIME";
    public static String CREATE_USER = "CREATE_USER";
    public static String STATUS = "STATUS";
    public static String RESULT_CODE = "RESULT_CODE";
    public static String DESCRIPTION = "DESCRIPTION";
    private Long vipSubDetailId;
    private Long vipSubInfoId;
    private String isdn;
    private Timestamp nextProcessTime;
    private Timestamp hisNextProcessTime;
    private String cycleType;
    private String moneyAcc;
    private String moneyValue;
    private String smsAcc;
    private String smsValue;
    private String smsOutAcc;
    private String smsOutValue;
    private String dataAcc;
    private String dataValue;
    private String voiceAcc;
    private String voiceValue;
    private String voiceOutAcc;
    private String voiceOutValue;
    private String pricePlan;
    private Timestamp createTime;
    private String createUser;
    private String status;
    private String threadName;
    private String nodeName;
    private String clusterName;
    private String idBatch;
    private String message;
    private Long id;
    private String resultCode;
    private String description;
    private Long totalMoney;
    private Long eachMoney;
    private Long prepaidMonth;
    private Timestamp approvalTime;
    private String approvalUser;
    private Long remainPrepaidMonth;
    private Long makeSaleTrans; // 1 mean no need to make sale trans more
    private String policyId;
    private String voiceInOutAcc;
    private String voiceInOutValue;
    private String moneySpecialAcc;
    private String moneySpecialValue;
    private String policyName;
    private Long groupId;
    private String orgDesc;
    private int subStatus;

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

    public String getIsdn() {
        return isdn;
    }

    public void setIsdn(String isdn) {
        this.isdn = isdn;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public VipSubDetail() {
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

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Long getVipSubDetailId() {
        return vipSubDetailId;
    }

    public void setVipSubDetailId(Long vipSubDetailId) {
        this.vipSubDetailId = vipSubDetailId;
    }

    public Long getVipSubInfoId() {
        return vipSubInfoId;
    }

    public void setVipSubInfoId(Long vipSubInfoId) {
        this.vipSubInfoId = vipSubInfoId;
    }

    public Timestamp getNextProcessTime() {
        return nextProcessTime;
    }

    public void setNextProcessTime(Timestamp nextProcessTime) {
        this.nextProcessTime = nextProcessTime;
    }

    public String getCycleType() {
        return cycleType;
    }

    public void setCycleType(String cycleType) {
        this.cycleType = cycleType;
    }

    public String getMoneyAcc() {
        return moneyAcc;
    }

    public void setMoneyAcc(String moneyAcc) {
        this.moneyAcc = moneyAcc;
    }

    public String getSmsAcc() {
        return smsAcc;
    }

    public void setSmsAcc(String smsAcc) {
        this.smsAcc = smsAcc;
    }

    public String getSmsOutAcc() {
        return smsOutAcc;
    }

    public void setSmsOutAcc(String smsOutAcc) {
        this.smsOutAcc = smsOutAcc;
    }

    public String getDataAcc() {
        return dataAcc;
    }

    public void setDataAcc(String dataAcc) {
        this.dataAcc = dataAcc;
    }

    public String getVoiceAcc() {
        return voiceAcc;
    }

    public void setVoiceAcc(String voiceAcc) {
        this.voiceAcc = voiceAcc;
    }

    public String getVoiceOutAcc() {
        return voiceOutAcc;
    }

    public void setVoiceOutAcc(String voiceOutAcc) {
        this.voiceOutAcc = voiceOutAcc;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public String getMoneyValue() {
        return moneyValue;
    }

    public void setMoneyValue(String moneyValue) {
        this.moneyValue = moneyValue;
    }

    public String getSmsValue() {
        return smsValue;
    }

    public void setSmsValue(String smsValue) {
        this.smsValue = smsValue;
    }

    public String getSmsOutValue() {
        return smsOutValue;
    }

    public void setSmsOutValue(String smsOutValue) {
        this.smsOutValue = smsOutValue;
    }

    public String getDataValue() {
        return dataValue;
    }

    public void setDataValue(String dataValue) {
        this.dataValue = dataValue;
    }

    public String getVoiceValue() {
        return voiceValue;
    }

    public void setVoiceValue(String voiceValue) {
        this.voiceValue = voiceValue;
    }

    public String getVoiceOutValue() {
        return voiceOutValue;
    }

    public void setVoiceOutValue(String voiceOutValue) {
        this.voiceOutValue = voiceOutValue;
    }

    public Timestamp getHisNextProcessTime() {
        return hisNextProcessTime;
    }

    public void setHisNextProcessTime(Timestamp hisNextProcessTime) {
        this.hisNextProcessTime = hisNextProcessTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getTotalMoney() {
        return totalMoney;
    }

    public void setTotalMoney(Long totalMoney) {
        this.totalMoney = totalMoney;
    }

    public Long getEachMoney() {
        return eachMoney;
    }

    public void setEachMoney(Long eachMoney) {
        this.eachMoney = eachMoney;
    }

    public Long getPrepaidMonth() {
        return prepaidMonth;
    }

    public void setPrepaidMonth(Long prepaidMonth) {
        this.prepaidMonth = prepaidMonth;
    }

    public Timestamp getApprovalTime() {
        return approvalTime;
    }

    public void setApprovalTime(Timestamp approvalTime) {
        this.approvalTime = approvalTime;
    }

    public String getApprovalUser() {
        return approvalUser;
    }

    public void setApprovalUser(String approvalUser) {
        this.approvalUser = approvalUser;
    }

    public String getPricePlan() {
        return pricePlan;
    }

    public void setPricePlan(String pricePlan) {
        this.pricePlan = pricePlan;
    }

    public Long getRemainPrepaidMonth() {
        return remainPrepaidMonth;
    }

    public void setRemainPrepaidMonth(Long remainPrepaidMonth) {
        this.remainPrepaidMonth = remainPrepaidMonth;
    }

    public Long getMakeSaleTrans() {
        return makeSaleTrans;
    }

    public void setMakeSaleTrans(Long makeSaleTrans) {
        this.makeSaleTrans = makeSaleTrans;
    }

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public String getVoiceInOutAcc() {
        return voiceInOutAcc;
    }

    public void setVoiceInOutAcc(String voiceInOutAcc) {
        this.voiceInOutAcc = voiceInOutAcc;
    }

    public String getVoiceInOutValue() {
        return voiceInOutValue;
    }

    public void setVoiceInOutValue(String voiceInOutValue) {
        this.voiceInOutValue = voiceInOutValue;
    }

    public String getMoneySpecialAcc() {
        return moneySpecialAcc;
    }

    public void setMoneySpecialAcc(String moneySpecialAcc) {
        this.moneySpecialAcc = moneySpecialAcc;
    }

    public String getMoneySpecialValue() {
        return moneySpecialValue;
    }

    public void setMoneySpecialValue(String moneySpecialValue) {
        this.moneySpecialValue = moneySpecialValue;
    }

    public String getPolicyName() {
        return policyName;
    }

    public void setPolicyName(String policyName) {
        this.policyName = policyName;
    }

    public Long getGroupId() {
        return groupId;
    }

    public void setGroupId(Long groupId) {
        this.groupId = groupId;
    }

    public String getOrgDesc() {
        return orgDesc;
    }

    public void setOrgDesc(String orgDesc) {
        this.orgDesc = orgDesc;
    }

    public Integer getSubStatus() {
        return subStatus;
    }

    public void setSubStatus(Integer subStatus) {
        this.subStatus = subStatus;
    }
}
