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
public class Bonus implements Record {

    public static String REASON_ID = "REASON_ID";
    public static String ISSUE_DATETIME = "ISSUE_DATETIME";
    public static String USER_NAME = "USER_NAME";
    public static String SHOP_CODE = "SHOP_CODE";
    public static String SUB_ID = "SUB_ID";
    public static String PK_TYPE = "PK_TYPE";
    public static String PK_ID = "PK_ID";
    public static String ACTION_CODE = "ACTION_CODE";
    public static String ACTION_ID = "ACTION_ID";
    public static String ACTION_AUDIT_ID = "ACTION_AUDIT_ID";
    public static String AGENT_ID = "AGENT_ID";
    public static String ISDN = "ISDN";
    public static String ACTIVE_STATUS = "ACTIVE_STATUS";
    public static String ACTIVE_DATE = "ACTIVE_DATE";
    public static String RECEIVE_DATE = "RECEIVE_DATE";
    public static String RECEIVE_STATUS = "RECEIVE_STATUS";
    public static String PRODUCT_CODE = "PRODUCT_CODE";
    public static String STATUS = "STATUS";
    public static String STAFF_CODE = "STAFF_CODE";
    public static String CHANNEL_TYPE_ID = "CHANNEL_TYPE_ID";
    public static String ITEM_FEE_ID = "ITEM_FEE_ID";
    public static String ACTION_PROFILE_ID = "ACTION_PROFILE_ID";
    public static String AMOUNT = "AMOUNT";
    public static String RESULT_CODE = "RESULT_CODE";
    public static String DESCRIPTION = "DESCRIPTION";
    public static String ACCOUNT_ID = "ACCOUNT_ID";
    public static String AGENT_ISDN = "AGENT_ISDN";
    public static String SMS_ID = "SMS_ID";
    public static String DATE_ACTION = "DATE_ACTION";
    public static String DATE_PROCESS = "DATE_PROCESS";
    public static String DURATION = "DURATION";
    public static String EWALLET_ERROR_CODE = "EWALLET_ERROR_CODE";
    public static String EWALLET_DESCRIPTION = "EWALLET_DESCRIPTION";
    public static String EWALLET_DURATION = "EWALLET_DURATION";
    public static String EWALLET_TRANS_ID = "EWALLET_TRANS_ID";
    public static String ID_BATCH = "ID_BATCH";
    public static String TOTAL_CURRENT_VALUE = "TOTAL_CURRENT_VALUE";
    public static String TOTAL_CURRENT_TIMES = "TOTAL_CURRENT_TIMES";
    public static String CHECK_INFO = "CHECK_INFO";
    public static String ISDN_ACCOUNT = "ISDN_ACCOUNT";
    private String subId;
    private Long actionAuditId;
    private Long pkId;
    private String pkType;
    private String userName;
    private String shopCode;
    private Date issueDateTime;
    private String actionCode;
    private Long actionId;
    private Long reasonId;
    private Long agentId;
    private String isdn;
    private String activeStatus;
    private Date activeDate;
    private Date receiverDate;
    private String receiverStatus;
    private String productCode;
    private String status;
    private String staffCode;
    private int channelTypeId;
    private Long itemFeeId;
    private Long actionProfileId;
    private Long amount;
    private String description;
    private String resultCode;
    private Date endTime;
    private Long accountId;
    private String agentIsdn;
    private Long smsId;
    private Date dateAction;
    private long duration;
    private String eWalletErrCode;
    private String eWalletDescription;
    private long eWalletDuration;
    private long eWalletTransId;
    private String threadName;
    private String nodeName;
    private String clusterName;
    private String idBatch;
    private String message;
    private Long id;
    private int totalCurrentValue;
    private int totalCurrentAddTimes;
    private String checkInfo;
    private String isdnCustomer;
    private String staffCheck;
    private Date timeCheck;
    private String checkComment;
    private String bonusStatus;
    private long sumMoneyToPay;
    private String transId;
    private int countCorrectProfilePayEmoney;
    private String bts2G;
    //Linh add field 20180719
    private String simSerial;
    private String voucherCode;
    private Long custId;
    private String connectKitStatus;
    private long blockOcsHlr;
//    Linh 20180809 add field
    private String modifyType;
    private String requestEmola;
    private Long secondAmount;
    private long daySentSms;
//    LinhNBV 20181127 add field for register vas
    private String vasCode;
    private Long kitBatchId;
    private int prepaidMonth;
    private String handsetSerial;
    private int payMethod;
    private String bankName;
    private String bankTranCode;
    private String bankTranAmount;
    private String referenceId;
    private int saleHandsetType;
    private String mainProduct;
    private String mainPrice;
    private String handsetModel;
    private String studentCardCode;

    public String getStudentCardCode() {
        return studentCardCode;
    }

    public void setStudentCardCode(String studentCardCode) {
        this.studentCardCode = studentCardCode;
    }

    public String getHandsetModel() {
        return handsetModel;
    }

    public void setHandsetModel(String handsetModel) {
        this.handsetModel = handsetModel;
    }

    public String getMainProduct() {
        return mainProduct;
    }

    public void setMainProduct(String mainProduct) {
        this.mainProduct = mainProduct;
    }

    public String getMainPrice() {
        return mainPrice;
    }

    public void setMainPrice(String mainPrice) {
        this.mainPrice = mainPrice;
    }

    public int getSaleHandsetType() {
        return saleHandsetType;
    }

    public void setSaleHandsetType(int saleHandsetType) {
        this.saleHandsetType = saleHandsetType;
    }

    public int getPayMethod() {
        return payMethod;
    }

    public void setPayMethod(int payMethod) {
        this.payMethod = payMethod;
    }

    public String getBankName() {
        return bankName;
    }

    public void setBankName(String bankName) {
        this.bankName = bankName;
    }

    public String getBankTranCode() {
        return bankTranCode;
    }

    public void setBankTranCode(String bankTranCode) {
        this.bankTranCode = bankTranCode;
    }

    public String getBankTranAmount() {
        return bankTranAmount;
    }

    public void setBankTranAmount(String bankTranAmount) {
        this.bankTranAmount = bankTranAmount;
    }

    public String getReferenceId() {
        return referenceId;
    }

    public void setReferenceId(String referenceId) {
        this.referenceId = referenceId;
    }

    public int getPrepaidMonth() {
        return prepaidMonth;
    }

    public void setPrepaidMonth(int prepaidMonth) {
        this.prepaidMonth = prepaidMonth;
    }

    public String getHandsetSerial() {
        return handsetSerial;
    }

    public void setHandsetSerial(String handsetSerial) {
        this.handsetSerial = handsetSerial;
    }

    public Long getKitBatchId() {
        return kitBatchId;
    }

    public void setKitBatchId(Long kitBatchId) {
        this.kitBatchId = kitBatchId;
    }

    public String getVasCode() {
        return vasCode;
    }

    public void setVasCode(String vasCode) {
        this.vasCode = vasCode;
    }

    public long getDaySentSms() {
        return daySentSms;
    }

    public void setDaySentSms(long daySentSms) {
        this.daySentSms = daySentSms;
    }

    public String getModifyType() {
        return modifyType;
    }

    public void setModifyType(String modifyType) {
        this.modifyType = modifyType;
    }

    public String getRequestEmola() {
        return requestEmola;
    }

    public void setRequestEmola(String requestEmola) {
        this.requestEmola = requestEmola;
    }

    public long getBlockOcsHlr() {
        return blockOcsHlr;
    }

    public void setBlockOcsHlr(long blockOcsHlr) {
        this.blockOcsHlr = blockOcsHlr;
    }

    public Long getCustId() {
        return custId;
    }

    public void setCustId(Long custId) {
        this.custId = custId;
    }

    public String getVoucherCode() {
        return voucherCode;
    }

    public void setVoucherCode(String voucherCode) {
        this.voucherCode = voucherCode;
    }

    public String getSimSerial() {
        return simSerial;
    }

    public void setSimSerial(String simSerial) {
        this.simSerial = simSerial;
    }

    public Long getAccountId() {
        return accountId;
    }

    public void setAccountId(Long accountId) {
        this.accountId = accountId;
    }

    public Long getActionAuditId() {
        return actionAuditId;
    }

    public void setActionAuditId(Long actionAuditId) {
        this.actionAuditId = actionAuditId;
    }

    public String getActionCode() {
        return actionCode;
    }

    public void setActionCode(String actionCode) {
        this.actionCode = actionCode;
    }

    public Long getActionProfileId() {
        return actionProfileId;
    }

    public void setActionProfileId(Long actionProfileId) {
        this.actionProfileId = actionProfileId;
    }

    public Date getActiveDate() {
        return activeDate;
    }

    public void setActiveDate(Date activeDate) {
        this.activeDate = activeDate;
    }

    public String getActiveStatus() {
        return activeStatus;
    }

    public void setActiveStatus(String activeStatus) {
        this.activeStatus = activeStatus;
    }

    public Long getAgentId() {
        return agentId;
    }

    public void setAgentId(Long agentId) {
        this.agentId = agentId;
    }

    public String getAgentIsdn() {
        return agentIsdn;
    }

    public void setAgentIsdn(String agentIsdn) {
        this.agentIsdn = agentIsdn;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public int getChannelTypeId() {
        return channelTypeId;
    }

    public void setChannelTypeId(int channelTypeId) {
        this.channelTypeId = channelTypeId;
    }

    public Date getDateAction() {
        return dateAction;
    }

    public void setDateAction(Date dateAction) {
        this.dateAction = dateAction;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public String geteWalletDescription() {
        return eWalletDescription;
    }

    public void seteWalletDescription(String eWalletDescription) {
        this.eWalletDescription = eWalletDescription;
    }

    public long geteWalletDuration() {
        return eWalletDuration;
    }

    public void seteWalletDuration(long eWalletDuration) {
        this.eWalletDuration = eWalletDuration;
    }

    public String geteWalletErrCode() {
        return eWalletErrCode;
    }

    public void seteWalletErrCode(String eWalletErrCode) {
        this.eWalletErrCode = eWalletErrCode;
    }

    public long geteWalletTransId() {
        return eWalletTransId;
    }

    public void seteWalletTransId(long eWalletTransId) {
        this.eWalletTransId = eWalletTransId;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
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

    public String getIsdn() {
        return isdn;
    }

    public void setIsdn(String isdn) {
        this.isdn = isdn;
    }

    public Date getIssueDateTime() {
        return issueDateTime;
    }

    public void setIssueDateTime(Date issueDateTime) {
        this.issueDateTime = issueDateTime;
    }

    public Long getItemFeeId() {
        return itemFeeId;
    }

    public void setItemFeeId(Long itemFeeId) {
        this.itemFeeId = itemFeeId;
    }

    public Long getPkId() {
        return pkId;
    }

    public void setPkId(Long pkId) {
        this.pkId = pkId;
    }

    public String getPkType() {
        return pkType;
    }

    public void setPkType(String pkType) {
        this.pkType = pkType;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public Long getReasonId() {
        return reasonId;
    }

    public void setReasonId(Long reasonId) {
        this.reasonId = reasonId;
    }

    public Date getReceiverDate() {
        return receiverDate;
    }

    public void setReceiverDate(Date receiverDate) {
        this.receiverDate = receiverDate;
    }

    public String getReceiverStatus() {
        return receiverStatus;
    }

    public void setReceiverStatus(String receiverStatus) {
        this.receiverStatus = receiverStatus;
    }

    public String getShopCode() {
        return shopCode;
    }

    public void setShopCode(String shopCode) {
        this.shopCode = shopCode;
    }

    public Long getSmsId() {
        return smsId;
    }

    public void setSmsId(Long smsId) {
        this.smsId = smsId;
    }

    public String getStaffCode() {
        return staffCode;
    }

    public void setStaffCode(String staffCode) {
        this.staffCode = staffCode;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getSubId() {
        return subId;
    }

    public void setSubId(String subId) {
        this.subId = subId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
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

    public int getTotalCurrentAddTimes() {
        return totalCurrentAddTimes;
    }

    public void setTotalCurrentAddTimes(int totalCurrentAddTimes) {
        this.totalCurrentAddTimes = totalCurrentAddTimes;
    }

    public int getTotalCurrentValue() {
        return totalCurrentValue;
    }

    public void setTotalCurrentValue(int totalCurrentValue) {
        this.totalCurrentValue = totalCurrentValue;
    }

    public Long getActionId() {
        return actionId;
    }

    public void setActionId(Long actionId) {
        this.actionId = actionId;
    }

    public String getCheckInfo() {
        return checkInfo;
    }

    public void setCheckInfo(String checkInfo) {
        this.checkInfo = checkInfo;
    }

    public String getIsdnCustomer() {
        return isdnCustomer;
    }

    public void setIsdnCustomer(String isdnCustomer) {
        this.isdnCustomer = isdnCustomer;
    }

    public Bonus() {
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

    public String getStaffCheck() {
        return staffCheck;
    }

    public void setStaffCheck(String staffCheck) {
        this.staffCheck = staffCheck;
    }

    public Date getTimeCheck() {
        return timeCheck;
    }

    public void setTimeCheck(Date timeCheck) {
        this.timeCheck = timeCheck;
    }

    public long getSumMoneyToPay() {
        return sumMoneyToPay;
    }

    public void setSumMoneyToPay(long sumMoneyToPay) {
        this.sumMoneyToPay = sumMoneyToPay;
    }

    public String getTransId() {
        return transId;
    }

    public void setTransId(String transId) {
        this.transId = transId;
    }

    public int getCountCorrectProfilePayEmoney() {
        return countCorrectProfilePayEmoney;
    }

    public void setCountCorrectProfilePayEmoney(int countCorrectProfilePayEmoney) {
        this.countCorrectProfilePayEmoney = countCorrectProfilePayEmoney;
    }

    public String getCheckComment() {
        return checkComment;
    }

    public void setCheckComment(String checkComment) {
        this.checkComment = checkComment;
    }

    public String getBonusStatus() {
        return bonusStatus;
    }

    public void setBonusStatus(String bonusStatus) {
        this.bonusStatus = bonusStatus;
    }

    public String getBts2G() {
        return bts2G;
    }

    public void setBts2G(String bts2G) {
        this.bts2G = bts2G;
    }

    public String getConnectKitStatus() {
        return connectKitStatus;
    }

    public void setConnectKitStatus(String connectKitStatus) {
        this.connectKitStatus = connectKitStatus;
    }

    public Long getSecondAmount() {
        return secondAmount;
    }

    public void setSecondAmount(Long secondAmount) {
        this.secondAmount = secondAmount;
    }
}
