/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbProcessor;
import com.viettel.paybonus.obj.AccountAgent;
import com.viettel.paybonus.obj.Agent;
import com.viettel.paybonus.obj.Bonus;
import com.viettel.paybonus.obj.ItemFee;
import com.viettel.paybonus.obj.OutputChangeCommAccount;
import com.viettel.paybonus.obj.SubInfo;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class PayAnypayInActionProfile extends ProcessRecordAbstract {

    Exchange pro;
    DbProcessor db;
    //LinhNBV modified on September 04 2017: Add variables for message
    String msgFalseImageRegistrationPoint;
    String msgFalseInfoRegistrationPoint;
    String msgFalseBothRegistrationPoint;
    String msgFalseCustomer;
    String msgTrueRegistrationPoint;
    String msgTrueCustomer;

    public PayAnypayInActionProfile() {
        super();
        logger = Logger.getLogger(PayAnypayInActionProfile.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbProcessor();

        msgFalseImageRegistrationPoint = ResourceBundle.getBundle("configPayBonus").getString("msgFalseImageRegistrationPoint");
        msgFalseInfoRegistrationPoint = ResourceBundle.getBundle("configPayBonus").getString("msgFalseInfoRegistrationPoint");
        msgFalseBothRegistrationPoint = ResourceBundle.getBundle("configPayBonus").getString("msgFalseBothRegistrationPoint");
        msgFalseCustomer = ResourceBundle.getBundle("configPayBonus").getString("msgFalseCustomer");

        msgTrueRegistrationPoint = ResourceBundle.getBundle("configPayBonus").getString("msgTrueRegistrationPoint");
        msgTrueCustomer = ResourceBundle.getBundle("configPayBonus").getString("msgTrueCustomer");

    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        AccountAgent agent;
        Agent staffInfo;
        String staffCode;
        String isdn;
        boolean isAllowChannel;
        boolean isAllowProduct;
        SubInfo sub;
        ItemFee itemfee;
        int totalCurrValue;
        int totalCurrAddTime;
        boolean isMaxValueOrAddTimes;
        ConcurrentHashMap<String, Integer> mapCurrAddTimes = new ConcurrentHashMap<String, Integer>();
        ConcurrentHashMap<String, Integer> mapCurrAddValue = new ConcurrentHashMap<String, Integer>();
        OutputChangeCommAccount response;
        long timeSt;
        String actionCode;
        String msgInvalidProfileInfo;
        String msgInvalidProfileImage;
        String openFlagResult;
        String openFlagBAOCResult;
        String openFlagPLMResult;
        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            agent = null;
            staffInfo = null;
            staffCode = "";
            isdn = "";
            isAllowChannel = false;
            isAllowProduct = false;
            sub = null;
            itemfee = null;
            totalCurrValue = 0;
            totalCurrAddTime = 0;
            isMaxValueOrAddTimes = true;
            response = null;
            actionCode = "";
            msgInvalidProfileInfo = ResourceBundle.getBundle("configPayBonus").getString("msgInvalidProfileInfo");
            msgInvalidProfileImage = ResourceBundle.getBundle("configPayBonus").getString("msgInvalidProfileImage");
            Bonus bn = (Bonus) record;
            listResult.add(bn);
            openFlagResult = "";
            openFlagBAOCResult = "";
            openFlagPLMResult = "";
            if ("0".equals(bn.getResultCode())) {
//                Check account ewallet on SM to get isdn ewallet, staff_code
                staffCode = bn.getUserName();
                if ("".equals(staffCode)) {
                    logger.warn("UserName in action audit is null or empty actionAuditId " + bn.getActionAuditId());
                    bn.setResultCode("E01");
                    bn.setDescription("UserName in action audit is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                } else {
                    bn.setStaffCode(staffCode);
                }
                if (bn.getIsdnCustomer() == null || bn.getIsdnCustomer().trim().length() <= 0) {
                    logger.warn("ISDN_ACCOUNT is null or empty actionAuditId " + bn.getActionAuditId());
                    bn.setResultCode("E15");
                    bn.setDescription("ISDN_ACCOUNT is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//                Send sms for channel if checkinfo is equal 1, 2 it's mean invalid profile, info
                if ("2".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Invalid profile info, actionId " + bn.getActionAuditId()
                            + " isdn: " + isdn);
                    //LinhNBV start modified on September 04 2017: Add message false send to Registration Point.
                    String tel = db.getTelByStaffCode(staffCode);
                    String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                    String messageFalseRegistrationPoint = msgFalseInfoRegistrationPoint.replace("%XYZ%", serial);
                    db.sendSms("258" + tel, messageFalseRegistrationPoint, "86142");

                    String messageFalseCustomer = msgFalseCustomer.replace("%PHONE%", bn.getIsdnCustomer());
                    db.sendSms("258" + bn.getIsdnCustomer(), messageFalseCustomer, "86142");

                    logger.info("Message send to registration point: " + messageFalseRegistrationPoint);
                    logger.info("Message send to customer: " + messageFalseCustomer);
                    //LinhNBV end.
                    bn.setResultCode("E13");
                    bn.setDescription("Invalid profile info");
                    msgInvalidProfileInfo = msgInvalidProfileInfo.replace("XXX", bn.getIsdnCustomer());
                    bn.setMessage(msgInvalidProfileInfo);
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
                //LinhNBV modified on September 04 2017: Comment code for disable message for invalid profile image.
//                if ("1".equals(bn.getCheckInfo().trim())) {
//                    logger.warn("Invalid profile image, actionId " + bn.getActionAuditId()
//                            + " isdn: " + isdn);
//                    bn.setResultCode("E14");
//                    bn.setDescription("Invalid profile image");
//                    msgInvalidProfileImage = msgInvalidProfileImage.replace("XXX", bn.getIsdnCustomer());
//                    bn.setMessage(msgInvalidProfileImage);
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
                if ("1".equals(bn.getCheckInfo().trim())) {

                    String tel = db.getTelByStaffCode(staffCode);
                    String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                    String messageFalseRegistrationPoint = msgFalseImageRegistrationPoint.replace("%XYZ%", serial);
                    db.sendSms("258" + tel, messageFalseRegistrationPoint, "86142");

                    String messageFalseCustomer = msgFalseCustomer.replace("%PHONE%", bn.getIsdnCustomer());
                    db.sendSms("258" + bn.getIsdnCustomer(), messageFalseCustomer, "86142");

                    logger.info("Message send to registration point: " + messageFalseRegistrationPoint);
                    logger.info("Message send to customer: " + messageFalseCustomer);

                    logger.warn("Not correct profile, actionId " + bn.getActionAuditId()
                            + " isdn: " + isdn);
                    bn.setResultCode("E14");
                    bn.setDescription("Invalid profile");
//                    msgInvalidProfileImage = msgInvalidProfileImage.replace("XXX", bn.getIsdnCustomer());
                    bn.setMessage(msgFalseCustomer);
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }

                if (!"0".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Not correct profile, actionId " + bn.getActionAuditId()
                            + " isdn: " + isdn);
                    //LinhNBV start modified on September 04 2017: Add message false send to Registration Point.
                    String tel = db.getTelByStaffCode(staffCode);
                    String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                    String messageFalseRegistrationPoint = msgFalseBothRegistrationPoint.replace("%XYZ%", serial);
                    db.sendSms("258" + tel, messageFalseRegistrationPoint, "86142");

                    String messageFalseCustomer = msgFalseCustomer.replace("%PHONE%", bn.getIsdnCustomer());
                    db.sendSms("258" + bn.getIsdnCustomer(), messageFalseCustomer, "86142");

                    logger.info("Message send to registration point: " + messageFalseRegistrationPoint);
                    logger.info("Message send to customer: " + messageFalseCustomer);
                    //End.
                    bn.setResultCode("E14");
                    bn.setDescription("Invalid profile image");
                    msgInvalidProfileImage = msgInvalidProfileImage.replace("XXX", bn.getIsdnCustomer());
                    bn.setMessage(msgInvalidProfileImage);
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }

                //LinhNBV modified on September 04 2017: Truong hop dktt dung - Nhan tin den DKTT va Customer
                if ("0".equals(bn.getCheckInfo().trim())) {
                    String tel = db.getTelByStaffCode(staffCode);
                    String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                    String messageTrueRegistrationPoint = msgTrueRegistrationPoint.replace("%XYZ%", serial);
                    db.sendSms("258" + tel, messageTrueRegistrationPoint, "86142");

                    String messageTrueCustomer = msgTrueCustomer.replace("%PHONE%", bn.getIsdnCustomer());
                    db.sendSms("258" + bn.getIsdnCustomer(), messageTrueCustomer, "86142");

                    logger.info("Message send to registration point: " + messageTrueRegistrationPoint);
                    logger.info("Message send to customer: " + messageTrueCustomer);
                }
                //LinhNBV end.

//                Huynq13 20170816 start add to open flag when correct profile
                //                Open ODBOC flag on HLR for customer
                openFlagResult = pro.activeFlagSABLCK("258" + bn.getIsdnCustomer());
                if ("0".equals(openFlagResult)) {
                    logger.info("Open flag SABLCK successfully for sub when register info " + bn.getIsdn());
                } else {
                    logger.warn("Fail to open flag SABLCK for sub when register info " + bn.getIsdn());
                }
//                Open BAOC flag on HLR for customer -- HLR_HW_MOD_BAOC
                openFlagBAOCResult = pro.activeFlagBAOC("258" + bn.getIsdnCustomer());
                if ("0".equals(openFlagBAOCResult)) {
                    logger.info("Open flag BAOC successfully for sub when register info " + bn.getIsdn());
                } else {
                    logger.warn("Fail to open flag BAOC for sub when register info " + bn.getIsdn());
                }
//                Open USSD flag on HLR for customer -- HLR_HW_MOD_PLMNSS
                openFlagPLMResult = pro.activeFlagPLMNSS("258" + bn.getIsdnCustomer());
                if ("0".equals(openFlagPLMResult)) {
                    logger.info("Open flag PLMNSS successfully for sub when register info " + bn.getIsdn());
                } else {
                    logger.warn("Fail to open flag BAOC for sub when register info " + bn.getIsdn());
                }
//                Huynq13 20170816 end add to open flag when correct profile                
                logger.info("Start check pay by Emola " + bn.getActionAuditId());
                staffInfo = db.getAgentInfoByUser(staffCode);
                if (staffInfo == null) {
                    logger.warn("Can not get Staff Info, user: " + staffCode + " actionId " + bn.getActionAuditId()
                            + " isdn: " + isdn);
                    bn.setResultCode("E02");
                    bn.setDescription("Can not get staff Info");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                } else if (staffInfo.getIsdnWallet() != null && staffInfo.getIsdnWallet().length() > 0) {
//                    Huynq13 20171102 add to check channel have contract for pay via eMola
                    if (db.checkChannelHaveContract(staffCode)) {
                        if (db.checkAlreadyWaitApprove(bn.getActionAuditId())) {
                            logger.info("Have emola agent account but already have record same action_audit_id in bonus_approve "
                                    + bn.getActionAuditId());
                            bn.setResultCode("E17");
                            bn.setDescription("Have emoa agent account but already have record same action_audit_id in bonus_approve");
                            continue;
                        } else {
                            logger.info("Have emola agent account so must pay by Emola, now move this record to bonus_approve table "
                                    + bn.getActionAuditId());
                            db.insertBonusApprove(bn);
                            bn.setResultCode("E16");
                            bn.setDescription("Have emoa agent account so must pay by Emola, now already move to bonus_approve table");
                            continue;
                        }
                    }
                }
                logger.info("Start get AgentInfo " + bn.getActionAuditId());
                agent = db.getAccountAgentByUser(staffCode);
                if (agent == null || agent.getIsdn() == null || agent.getIsdn().length() <= 0) {
                    logger.warn("Can not get AccountAgent Info, user: " + staffCode + " actionId " + bn.getActionAuditId()
                            + " isdn: " + isdn);
                    bn.setResultCode("E02");
                    bn.setDescription("Can not get AccountAgent Info");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                } else {
                    isdn = agent.getIsdn();
                    bn.setIsdn(isdn);
                    bn.setAgentIsdn(isdn);
                    bn.setAccountId(agent.getAccountId());
                    bn.setAgentId(agent.getAgentId());
                    bn.setChannelTypeId(agent.getChannelTypeId());
                }
//                Get actioncode
                logger.info("Start get actioncode for actionId " + bn.getActionAuditId());
                actionCode = db.getActionCode(bn.getActionAuditId(), bn.getActionId());
                if ("".equals(actionCode)) {
                    logger.warn("Can not get actioncode " + bn.getActionId()
                            + " actionId " + bn.getActionAuditId());
                    bn.setResultCode("E12");
                    bn.setDescription("Can not get actioncode " + bn.getActionId());
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                } else {
                    bn.setActionCode(actionCode);
                }
//                Check have config in action_reason
                logger.info("Start check having config ActionReason for actionId " + bn.getActionAuditId());
                if (!db.checkActionReason(bn.getActionCode(), bn.getReasonId(), bn.getActionAuditId())) {
                    logger.warn("Do not have config ActionReason for ActionCode " + bn.getActionCode()
                            + " ReasonId " + bn.getReasonId() + " actionId " + bn.getActionAuditId());
                    bn.setResultCode("E11");
                    bn.setDescription("Do not have config ActionReason");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//                Check to make sure not yet processing this record, not duplicate process for each record (base on action_audit_id)
                logger.info("Start check to make sure not duplicate process actionId " + bn.getActionAuditId());
                if (db.checkAlreadyProcessRecord(bn.getActionAuditId())) {
                    logger.warn("Already process record actionId " + bn.getActionAuditId());
                    bn.setResultCode("E10");
                    bn.setDescription("Already process record");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//                Check allow channel type
                logger.info("Start check allow channel actionid " + bn.getActionAuditId() + " isdn " + isdn);
                isAllowChannel = db.checkAllowChannel(agent.getChannelTypeId(), isdn);
                if (!isAllowChannel) {
                    logger.warn("Channel type is not allowed actionid " + bn.getActionAuditId()
                            + " isdn: " + isdn + " channelTypeId " + agent.getChannelTypeId());
                    bn.setResultCode("E03");
                    bn.setDescription("Channel type is not allowed");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//                Check sub info
                if (bn.getPkId() == null) {
                    logger.warn("Do not have Sub or Cust Id, actionId " + bn.getActionAuditId() + " isdn: " + isdn);
//                    bn.setResultCode("E04");
//                    bn.setDescription("Do not have Sub or Cust Id");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
                } else {
                    logger.info("Start check SubInfo actionId " + bn.getActionAuditId() + " pkid " + bn.getPkId() + " isdn " + isdn);
                    sub = db.getSubInfoBySubId(bn.getPkId(), isdn);
                    if (sub == null || sub.getProductCode() == null || sub.getProductCode().length() <= 0) {
                        logger.warn("Can not find subscriber info actionId " + bn.getActionAuditId()
                                + " pkid " + bn.getPkId() + " isdn: " + isdn);
                        bn.setResultCode("E05");
                        bn.setDescription("Can not find subscriber info");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    } else {
                        bn.setProductCode(sub.getProductCode());
                        bn.setActiveStatus(sub.getActStatus());
                        bn.setActiveDate(sub.getActiveDate());
                        //                Check allow product       
                        logger.info("Start check allow Product actionId " + bn.getActionAuditId() + " isdn " + isdn);
                        isAllowProduct = db.checkProductAllow(sub.getProductCode(), isdn);
                        if (!isAllowProduct) {
                            logger.warn("Product is not allowed actionId " + bn.getActionAuditId()
                                    + " isdn: " + isdn + " productCode " + sub.getProductCode());
                            bn.setResultCode("E06");
                            bn.setDescription("Product is not allowed");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            continue;
                        }
                    }
                }
//              Get item fee
                logger.info("Start check ItemFee actionId " + bn.getActionAuditId() + " isdn " + isdn);
                itemfee = db.getItemFee(agent.getChannelTypeId(), "", bn.getActionCode(), bn.getReasonId(), isdn);
                if (itemfee == null || itemfee.getAmount() <= 0) {
                    logger.warn("Can not find item_fee info or amount less than 0 actionId " + bn.getActionAuditId()
                            + " isdn: " + isdn);
                    bn.setResultCode("E07");
                    bn.setDescription("Can not find item_fee info or amount less than 0");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                } else {
                    bn.setAmount(itemfee.getAmount());
                    bn.setItemFeeId(itemfee.getItemFeeId());
                }
////                Check profile if request (checprofile ==1)
//                logger.info("Start check Profile if request actionId " + bn.getActionAuditId()
//                        + " checkprofile " + itemfee.getCheckProfile() + " isdn " + isdn);
//                if (itemfee.getCheckProfile() == 1) {
//                    aProfile = db.checkProfile(bn.getActionAuditId());
//                    bn.setActionProfileId(aProfile.getActionProfileId());
//                    if (!aProfile.isEnoughProfile()) {
//                        logger.warn("Profile do not have or not enough or invalid or not validated, actionId "
//                                + bn.getActionAuditId() + " isdn: " + isdn);
//                        bn.setResultCode("E11");
//                        bn.setDescription("Do not have or not enough profile");
//                        bn.setDuration(System.currentTimeMillis() - timeSt);
//                        continue;
//                    }
//                }
//              get Current total value
                if (mapCurrAddValue.get(isdn) == null || mapCurrAddValue.get(isdn) <= 0) {
                    totalCurrValue = db.getCurrentValueInDay(isdn);
                    mapCurrAddValue.put(isdn, totalCurrValue);
                } else {
                    totalCurrValue = mapCurrAddValue.get(isdn);
                }
                if (mapCurrAddTimes.get(isdn) == null || mapCurrAddTimes.get(isdn) <= 0) {
                    totalCurrAddTime = db.getCurrentTimeAddInDay(isdn);
                    mapCurrAddTimes.put(isdn, totalCurrAddTime);
                } else {
                    totalCurrAddTime = mapCurrAddTimes.get(isdn);
                }
                if (totalCurrValue == -1 || totalCurrAddTime == -1) {
                    logger.error("System error had exception actionId, isdn: " + isdn);
                    bn.setResultCode("E99");
                    bn.setDescription("System error, can not get total value or total addtimes int day");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//              Check over max_addtime or max value
                logger.info("Start check over max value, max times: " + isdn);
                isMaxValueOrAddTimes = db.checkMaxAddMaxValueInDay(agent.getChannelTypeId(), isdn, totalCurrValue, totalCurrAddTime);
                if (isMaxValueOrAddTimes) {
                    logger.warn("Limited total value or addtimes in day actionId " + bn.getActionAuditId() + ", isdn: " + isdn);
                    bn.setTotalCurrentValue(totalCurrValue);
                    bn.setTotalCurrentAddTimes(totalCurrAddTime);
                    bn.setResultCode("E08");
                    bn.setDescription("Limited total value or addtimes in day");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//              Call proceduce to make request adding Anypay
                response = db.callSmChangeCommAccount(agent.getAccountId(), "Register customer info for Smartphone",
                        itemfee.getAmount(), bn.getActionAuditId(), isdn);
                if (response != null && "OK".equals(response.getErrCode())) {
                    logger.info("Make request to add anypay success for actionId " + bn.getActionAuditId() + " isdn_account "
                            + bn.getIsdnCustomer() + " amount " + itemfee.getAmount());
                    bn.setTotalCurrentValue(totalCurrValue + (int) itemfee.getAmount());
                    bn.setTotalCurrentAddTimes(totalCurrAddTime + 1);
                    mapCurrAddValue.put(isdn, totalCurrValue + (int) itemfee.getAmount());
                    mapCurrAddTimes.put(isdn, totalCurrAddTime + 1);
                    bn.setResultCode("0");
                    bn.setDescription("Make request to add anypay success " + bn.getIsdnCustomer() + " amount " + itemfee.getAmount()
                            + " comm_account_book request_id " + response.getRequestId());
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    logger.info("Make sendsms for isdn " + isdn);
                    db.insertSendSms(isdn, bn.getIsdnCustomer(), response.getRequestId(), bn.getActionAuditId());
                    continue;
                } else {
                    logger.error("Making request to add anypay failed for actionId " + bn.getActionAuditId()
                            + " isdn " + isdn + " amount " + itemfee.getAmount());
                    bn.setResultCode("E09");
                    bn.setDescription("Make request to add anypay fail for isdn " + isdn + " amount " + itemfee.getAmount());
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getActionAuditId()
                        + " so continue with other transaction");
                continue;
            }
        }
        listRecord.clear();
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tID|").
                append("|\tACTION_PROFILE_ID|").
                append("|\tRECEIVE_DATE|").
                append("|\tPK_ID\t|").
                append("|\tISDN\t|").
                append("|\tUSER\t|").
                append("|\tACTION\t|").
                append("|\tREASON\t|").
                append("|\tISSUE_DATE\t|").
                append("|SHOP|");
        for (Record record : listRecord) {
            Bonus bn = (Bonus) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getActionAuditId()).
                    append("||\t").
                    append(bn.getActionProfileId()).
                    append("||\t").
                    append((bn.getReceiverDate() != null ? sdf.format(bn.getReceiverDate()) : null)).
                    append("||\t").
                    append(bn.getPkId()).
                    append("||\t").
                    append(bn.getIsdnCustomer()).
                    append("||\t").
                    append(bn.getUserName()).
                    append("||\t").
                    append(bn.getActionId()).
                    append("||\t").
                    append(bn.getReasonId()).
                    append("||\t").
                    append((bn.getIssueDateTime() != null ? sdf.format(bn.getIssueDateTime()) : null)).
                    append("||\t").
                    append(bn.getShopCode());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
//        logger.warn("TEMPLATE process exception record: " + ex.toString());
//        for (Record record : listRecord) {
//            logger.info("TEMPLATE let convert to recort type you want and then set errCode, errDesc at here");
////            MoRecord moRecord = (MoRecord) record;
////            moRecord.setMessage("Thao tac that bai!");
////            moRecord.setErrCode("-5");
//        }
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
}
