///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbEmoneyProcessor;
////import com.viettel.paybonus.database.DbProcessor_1;
//import com.viettel.paybonus.obj.Agent;
//import com.viettel.paybonus.obj.Bonus;
//import com.viettel.paybonus.obj.ItemFee;
//import com.viettel.paybonus.obj.SubInfo;
//import com.viettel.paybonus.service.Exchange;
//import com.viettel.threadfw.manager.AppManager;
//import java.util.List;
//import org.apache.log4j.Logger;
//import com.viettel.threadfw.process.ProcessRecordAbstract;
//import com.viettel.vas.util.ExchangeClientChannel;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.Map;
//import java.util.ResourceBundle;
//import java.util.concurrent.ConcurrentHashMap;
//
///**
// *
// * @author HuyNQ13
// * @version 1.0
// * @since 24-03-2016
// */
//public class PayEmoneyInActionProfile extends ProcessRecordAbstract {
//
//    Exchange pro;
//    DbEmoneyProcessor db;
//    String isdnWarning;
//    String[] listIsdnWarn;
//    long limitValueBonus;
//    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
//
//    public PayEmoneyInActionProfile() {
//        super();
//        logger = Logger.getLogger(PayEmoneyInActionProfile.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
//        db = new DbEmoneyProcessor();
////        DbProcessor_1 test = new DbProcessor_1();
////        test.getPinCodeBySerial("123", "123", "C10");
//        isdnWarning = ResourceBundle.getBundle("configPayBonus").getString("pay_bonus_isdn");
//        listIsdnWarn = isdnWarning.split("\\|");
//        limitValueBonus = Long.valueOf(ResourceBundle.getBundle("configPayBonus").getString("limitValueBonus"));
//    }
//
//    @Override
//    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
//        return listRecord;
//    }
//
//    @Override
//    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
//        List<Record> listResult = new ArrayList<Record>();
//        Agent agent;
//        String staffCode;
//        String isdn;
//        boolean isAllowChannel;
//        boolean isAllowProduct;
//        SubInfo sub;
//        ItemFee itemfee;
////        int totalCurrValue;
////        int totalCurrAddTime;
////        boolean isMaxValueOrAddTimes;
////        boolean isWarnMaxValueOrAddTimes;
//        String msgWarn;
//        String transId;
//        String msg;
//        String msgInvalidProfileInfo;
//        String msgInvalidProfileImage;
//        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
//        SimpleDateFormat sdf2 = new SimpleDateFormat("dd/MM/yyyy");
////        ActionProfile aProfile;
////        ConcurrentHashMap<String, Integer> mapCurrAddTimes = new ConcurrentHashMap<String, Integer>();
////        ConcurrentHashMap<String, Integer> mapCurrAddValue = new ConcurrentHashMap<String, Integer>();
//        ConcurrentHashMap<String, Bonus> mapToSumMoney = new ConcurrentHashMap<String, Bonus>();
//        String eWalletResponse;
//        long timeSt = System.currentTimeMillis();
//        String actionCode;
//        for (Record record : listRecord) {
//            agent = null;
//            staffCode = "";
//            isdn = "";
//            isAllowChannel = false;
//            isAllowProduct = false;
//            sub = null;
//            itemfee = null;
////            totalCurrValue = 0;
////            totalCurrAddTime = 0;
////            isMaxValueOrAddTimes = true;
////            isWarnMaxValueOrAddTimes = true;
//            transId = sdf.format(new Date());
//            eWalletResponse = "";
//            actionCode = "";
//            msgInvalidProfileInfo = ResourceBundle.getBundle("configPayBonus").getString("msgInvalidProfileInfo");
//            msgInvalidProfileImage = ResourceBundle.getBundle("configPayBonus").getString("msgInvalidProfileImage");
//            Bonus bn = (Bonus) record;
//            listResult.add(bn);
//            if ("0".equals(bn.getResultCode())) {
////                Check account ewallet on SM to get isdn ewallet, staff_code
//                staffCode = bn.getUserName();
//                if ("".equals(staffCode)) {
//                    logger.warn("UserName in action audit is null or empty actionAuditId " + bn.getActionAuditId());
//                    bn.setResultCode("E01");
//                    bn.setDescription("UserName in action audit is null or empty");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                } else {
//                    bn.setStaffCode(staffCode);
//                }
//                logger.info("Start get AgentInfo " + bn.getActionAuditId());
//                agent = db.getAgentInfoByUser(staffCode);
//                if (agent == null || agent.getIsdnWallet() == null || agent.getIsdnWallet().length() <= 0) {
//                    logger.warn("Can not get Agent Info, user: " + staffCode + " actionId " + bn.getActionAuditId()
//                            + " isdnEmola: " + isdn);
//                    bn.setResultCode("E02");
//                    bn.setDescription("Can not get Agent Info");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                } else {
//                    isdn = agent.getIsdnWallet();
//                    bn.setIsdn(isdn);
//                    bn.setAgentId(agent.getStaffId());
//                    bn.setChannelTypeId(agent.getChannelTypeId());
//                }
////                Send sms for channel if checkinfo is equal 1, 2 it's mean invalid profile, info
//                if ("3".equals(bn.getCheckInfo().trim())) {
//                    logger.warn("Invalid profile info, actionId " + bn.getActionAuditId()
//                            + " isdnEmola: " + isdn);
//                    bn.setResultCode("E13");
//                    bn.setDescription("Invalid profile info");
//                    msgInvalidProfileInfo = msgInvalidProfileInfo.replace("XXX", bn.getIsdnCustomer());
//                    bn.setMessage(msgInvalidProfileInfo);
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
//                if ("2".equals(bn.getCheckInfo().trim())) {
//                    logger.warn("Invalid profile image, actionId " + bn.getActionAuditId()
//                            + " isdnEmola: " + isdn);
//                    bn.setResultCode("E14");
//                    bn.setDescription("Invalid profile image");
//                    msgInvalidProfileImage = msgInvalidProfileImage.replace("XXX", bn.getIsdnCustomer());
//                    bn.setMessage(msgInvalidProfileImage);
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
////                Get actioncode
//                logger.info("Start get actioncode for actionId " + bn.getActionAuditId());
//                actionCode = db.getActionCode(bn.getActionAuditId(), bn.getActionId());
//                if ("".equals(actionCode)) {
//                    logger.warn("Can not get actioncode " + bn.getActionId()
//                            + " actionId " + bn.getActionAuditId());
//                    bn.setResultCode("E12");
//                    bn.setDescription("Can not get actioncode " + bn.getActionId());
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                } else {
//                    bn.setActionCode(actionCode);
//                }
////                Check have config in action_reason
//                logger.info("Start check having config ActionReason for actionId " + bn.getActionAuditId());
//                if (!db.checkActionReason(bn.getActionCode(), bn.getReasonId(), bn.getActionAuditId())) {
//                    logger.warn("Do not have config ActionReason for ActionCode " + bn.getActionCode()
//                            + " ReasonId " + bn.getReasonId() + " actionId " + bn.getActionAuditId());
//                    bn.setResultCode("E11");
//                    bn.setDescription("Do not have config ActionReason");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
////                Check to make sure not yet processing this record, not duplicate process for each record (base on action_audit_id)
//                logger.info("Start check to make sure not duplicate process actionId " + bn.getActionAuditId());
//                if (db.checkAlreadyProcessRecord(bn.getActionAuditId())) {
//                    logger.warn("Already process record actionId " + bn.getActionAuditId());
//                    bn.setResultCode("E10");
//                    bn.setDescription("Already process record");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
////                Check allow channel type
//                logger.info("Start check allow channel actionid " + bn.getActionAuditId() + " isdnEmola " + isdn);
//                isAllowChannel = db.checkAllowChannel(agent.getChannelTypeId(), isdn);
//                if (!isAllowChannel) {
//                    logger.warn("Channel type is not allowed actionid " + bn.getActionAuditId()
//                            + " isdn: " + isdn + " channelTypeId " + agent.getChannelTypeId());
//                    bn.setResultCode("E03");
//                    bn.setDescription("Channel type is not allowed");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
////                Check sub info
//                if (bn.getPkId() == null || bn.getPkId() <= 0) {
//                    logger.warn("Do not have Sub or Cust Id, actionId " + bn.getActionAuditId() + " isdnEmola: " + isdn);
////                    bn.setResultCode("E04");
////                    bn.setDescription("Do not have Sub or Cust Id");
////                    bn.setDuration(System.currentTimeMillis() - timeSt);
////                    continue;
//                } else {
//                    logger.info("Start check SubInfo actionId " + bn.getActionAuditId() + " pkid " + bn.getPkId() + " isdnEmola " + isdn);
//                    sub = db.getSubInfoBySubId(bn.getPkId(), isdn);
//                    if (sub == null || sub.getProductCode() == null || sub.getProductCode().length() <= 0) {
//                        logger.warn("Can not find subscriber info actionId " + bn.getActionAuditId()
//                                + " pkid " + bn.getPkId() + " isdnEmola: " + isdn);
//                        bn.setResultCode("E05");
//                        bn.setDescription("Can not find subscriber info");
//                        bn.setDuration(System.currentTimeMillis() - timeSt);
//                        continue;
//                    } else {
//                        bn.setProductCode(sub.getProductCode());
//                        bn.setActiveStatus(sub.getActStatus());
//                        bn.setActiveDate(sub.getActiveDate());
//                        //                Check allow product       
//                        logger.info("Start check allow Product actionId " + bn.getActionAuditId() + " isdnEmola " + isdn);
//                        isAllowProduct = db.checkProductAllow(sub.getProductCode(), isdn);
//                        if (!isAllowProduct) {
//                            logger.warn("Product is not allowed actionId " + bn.getActionAuditId()
//                                    + " isdnEmola: " + isdn + " productCode " + sub.getProductCode());
//                            bn.setResultCode("E06");
//                            bn.setDescription("Product is not allowed");
//                            bn.setDuration(System.currentTimeMillis() - timeSt);
//                            continue;
//                        }
//                    }
//                }
////              Get item fee
//                logger.info("Start check ItemFee actionId " + bn.getActionAuditId() + " isdnEmola " + isdn);
//                itemfee = db.getItemFee(agent.getChannelTypeId(), "", bn.getActionCode(), bn.getReasonId(), isdn);
//                if (itemfee == null || itemfee.getAmount() <= 0) {
//                    logger.warn("Can not find item_fee info or amount less than 0 actionId " + bn.getActionAuditId()
//                            + " isdnEmola: " + isdn);
//                    bn.setResultCode("E07");
//                    bn.setDescription("Can not find item_fee info or amount less than 0");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                } else {
//                    bn.setAmount(itemfee.getAmount());
//                    bn.setItemFeeId(itemfee.getItemFeeId());
//                }
//////                Check profile if request (checprofile ==1)
////                logger.info("Start check Profile if request actionId " + bn.getActionAuditId()
////                        + " checkprofile " + itemfee.getCheckProfile() + " isdnEmola " + isdn);
////                if (itemfee.getCheckProfile() == 1) {
////                    aProfile = db.checkProfile(bn.getActionAuditId());
////                    bn.setActionProfileId(aProfile.getActionProfileId());
////                    if (!aProfile.isEnoughProfile()) {
////                        logger.warn("Profile do not have or not enough or invalid or not validated, actionId "
////                                + bn.getActionAuditId() + " isdnEmola: " + isdn);
////                        bn.setResultCode("E11");
////                        bn.setDescription("Do not have or not enough profile");
////                        bn.setDuration(System.currentTimeMillis() - timeSt);
////                        continue;
////                    }
////                }
////              get Current total value
////                if (mapCurrAddValue.get(isdn) == null || mapCurrAddValue.get(isdn) <= 0) {
////                    totalCurrValue = db.getCurrentValueInDay(isdn);
////                    mapCurrAddValue.put(isdn, totalCurrValue);
////                } else {
////                    totalCurrValue = mapCurrAddValue.get(isdn);
////                }
////                if (mapCurrAddTimes.get(isdn) == null || mapCurrAddTimes.get(isdn) <= 0) {
////                    totalCurrAddTime = db.getCurrentTimeAddInDay(isdn);
////                    mapCurrAddTimes.put(isdn, totalCurrAddTime);
////                } else {
////                    totalCurrAddTime = mapCurrAddTimes.get(isdn);
////                }
////                if (totalCurrValue == -1 || totalCurrAddTime == -1) {
////                    logger.error("System error had exception actionId, isdn: " + isdn);
////                    bn.setResultCode("E99");
////                    bn.setDescription("System error, can not get total value or total addtimes int day");
////                    bn.setDuration(System.currentTimeMillis() - timeSt);
////                    continue;
////                }
////              Check over max_addtime or max value
////                logger.info("Start check over max value, max times: " + isdn);
////                isMaxValueOrAddTimes = db.checkMaxAddMaxValueInDay(agent.getChannelTypeId(), isdn, totalCurrValue, totalCurrAddTime);
////                if (isMaxValueOrAddTimes) {
////                    logger.warn("Limited total value or addtimes in day actionId " + bn.getActionAuditId() + ", isdnEmola: " + isdn);
////                    bn.setTotalCurrentValue(totalCurrValue);
////                    bn.setTotalCurrentAddTimes(totalCurrAddTime);
////                    bn.setResultCode("E08");
////                    bn.setDescription("Limited total value or addtimes in day");
////                    bn.setDuration(System.currentTimeMillis() - timeSt);
////                    continue;
////                }
////              Check over warn_addtime or warn value
////                logger.info("Start check to warn over limit value, limit times: " + isdn);
////                isWarnMaxValueOrAddTimes = db.checkWarnMaxAddMaxValueInDay(agent.getChannelTypeId(), isdn, totalCurrValue, totalCurrAddTime);
////                if (isWarnMaxValueOrAddTimes) {                
////                    logger.warn("Warning limited total value or addtimes in day actionId "
////                            + bn.getActionAuditId() + ", isdnEmola: " + isdn + " channel " + staffCode);
////                    String[] listIsdn = isdnWarning.split("\\|");
////                    msgWarn = ResourceBundle.getBundle("configPayBonus").getString("ms_pay_bonus_warning");
////                    msgWarn = msgWarn.replace("%CHANNEL%", staffCode);
////                    msgWarn = msgWarn.replace("%MONEY%", totalCurrValue + "");
////                    for (String i : listIsdn) {
////                        db.sendSms(i, msgWarn, "86904");
////                    }
////                }
////                Huynq13 20171102 change to pay one time, must sum money.
//                if (mapToSumMoney.get(isdn) == null) {
//                    Bonus bonusSumMoney = new Bonus();
//                    bonusSumMoney.setActionAuditId(bn.getActionAuditId());
//                    bonusSumMoney.setChannelTypeId(agent.getChannelTypeId());
//                    bonusSumMoney.setIsdn(isdn);
//                    bonusSumMoney.setSumMoneyToPay(itemfee.getAmount());
//                    bonusSumMoney.setActionCode(bn.getActionCode());
//                    bonusSumMoney.setStaffCode(staffCode);
//                    bonusSumMoney.setTransId(transId);
//                    bonusSumMoney.setAmount(itemfee.getAmount());
//                    bonusSumMoney.setCountCorrectProfilePayEmoney(1);
//                    mapToSumMoney.put(isdn, bonusSumMoney);
//                } else {
//                    mapToSumMoney.get(isdn).setSumMoneyToPay(mapToSumMoney.get(isdn).getSumMoneyToPay() + itemfee.getAmount());
//                    mapToSumMoney.get(isdn).setCountCorrectProfilePayEmoney(
//                            mapToSumMoney.get(isdn).getCountCorrectProfilePayEmoney() + 1);
//                }
////              Call eWallet to add bonus
////                eWalletResponse = pro.callEwallet(bn.getActionAuditId(), agent.getChannelTypeId(), isdn, itemfee.getAmount(),
////                        bn.getActionCode(), staffCode, transId, db);
////                bn.seteWalletErrCode(eWalletResponse);
////                if ("01".equals(eWalletResponse)) {
////                    logger.info("Pay Bonus success for actionId " + bn.getActionAuditId() + " isdnEmola "
////                            + isdn + " amount " + itemfee.getAmount());
////                    bn.setTotalCurrentValue(totalCurrValue + (int) itemfee.getAmount());
////                    bn.setTotalCurrentAddTimes(totalCurrAddTime + 1);
////                    mapCurrAddValue.put(isdn, totalCurrValue + (int) itemfee.getAmount());
////                    mapCurrAddTimes.put(isdn, totalCurrAddTime + 1);
////                    bn.setResultCode("0");
////                    bn.setDescription("Pay Bonus success for isdnEmola " + isdn + " amount " + itemfee.getAmount());
////                    msg = msg.replace("XXX", "" + itemfee.getAmount());
////                    msg = msg.replace("YYY", isdn);
////                    msg = msg.replace("ZZZ", "" + (totalCurrAddTime + 1));
////                    msg = msg.replace("WWW", "" + (totalCurrValue + (int) itemfee.getAmount()));
////                    bn.setMessage(msg);
////                    logger.info("Message will send to isdnEmola " + isdn + " msg: " + msg);
////                    bn.setDuration(System.currentTimeMillis() - timeSt);
////                    continue;
////                } else {
////                    logger.error("Pay Bonus fail for actionId " + bn.getActionAuditId()
////                            + " isdnEmola " + isdn + " amount " + itemfee.getAmount());
////                    bn.setResultCode("E09");
////                    bn.setDescription("Pay Bonus fail for isdnEmola " + isdn + " amount " + itemfee.getAmount());
////                    bn.setDuration(System.currentTimeMillis() - timeSt);
////                    continue;
////                }
//            } else {
//                logger.warn("After validate respone code is fail actionId " + bn.getActionAuditId()
//                        + " so continue with other transaction");
//                continue;
//            }
//        }
////        After process all records, finish summing money, now start paing
//        for (Map.Entry<String, Bonus> entry : mapToSumMoney.entrySet()) {
//            String key = entry.getKey();
//            Bonus value = entry.getValue();
//            eWalletResponse = "";
//            if (value.getSumMoneyToPay() > limitValueBonus) {
//                logger.warn("Warning limited total value actionId " + value.getActionAuditId()
//                        + ", isdnEmola: " + key + " channel " + value.getStaffCode() + " totalMoneyPay " + value.getSumMoneyToPay()
//                        + " totalProfileCorrectToPay " + value.getCountCorrectProfilePayEmoney());
//                msgWarn = ResourceBundle.getBundle("configPayBonus").getString("ms_pay_bonus_warning");
//                msgWarn = msgWarn.replace("%CHANNEL%", value.getStaffCode());
//                msgWarn = msgWarn.replace("%MONEY%", value.getSumMoneyToPay() + "");
//                for (String i : listIsdnWarn) {
//                    db.sendSms(i, msgWarn, "86142");
//                }
//            }
//            logger.info("Start paing for actionId " + value.getActionAuditId()
//                    + ", isdnEmola: " + key + " channel " + value.getStaffCode() + " totalMoneyPay " + value.getSumMoneyToPay()
//                    + " totalProfileCorrectToPay " + value.getCountCorrectProfilePayEmoney());
//            eWalletResponse = pro.callEwallet(value.getActionAuditId(), value.getChannelTypeId(), key, value.getSumMoneyToPay(),
//                    value.getActionCode(), value.getStaffCode(), value.getTransId(), db);
//            if ("01".equals(eWalletResponse)) {
//                msg = ResourceBundle.getBundle("configPayBonus").getString("ms_pay_bonus_emoney");
////                msg = msg.replace("%FROMDATE%", db.getFromDate());
////                msg = msg.replace("%TODATE%", sdf2.format(new Date()));
//                Calendar cal = Calendar.getInstance();
//                cal.setTime(new Date());
//                cal.add(Calendar.DATE, -1);
//                msg = msg.replace("%YESTERDAY%", sdf2.format(cal.getTime()));
//                msg = msg.replace("%QUANTITY%", "" + value.getCountCorrectProfilePayEmoney());
//                msg = msg.replace("%MONEY%", "" + value.getSumMoneyToPay());
////                logger.info("Message will send to isdnEmola " + key + " msg: " + msg);
//                db.sendSms(key, msg, "86142");
//            }
//            for (Record rc : listResult) {
//                Bonus bn = (Bonus) rc;
//                if (bn.getIsdn() != null && bn.getIsdn().equals(key)) {
//                    if ("01".equals(eWalletResponse)) {
//                        logger.info("Pay Bonus success for actionId " + value.getActionAuditId() + " isdnEmola "
//                                + key + " sumMoney " + value.getSumMoneyToPay());
//                        bn.setResultCode("0");
//                        bn.setDescription("Pay Bonus success for isdnEmola " + key + " sumMoney " + value.getSumMoneyToPay());
//                    } else {
//                        logger.error("Pay Bonus fail for actionId " + value.getActionAuditId()
//                                + " isdnEmola " + key + " sumMoney " + value.getSumMoneyToPay());
//                        bn.setResultCode("E09");
//                        bn.setDescription("Pay Bonus fail for isdnEmola " + key + " sumMoney " + value.getSumMoneyToPay());
//                    }
//                    bn.seteWalletErrCode(eWalletResponse);
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                }
//            }
//        }
//        listRecord.clear();
//        return listResult;
//    }
//
//    @Override
//    public void printListRecord(List<Record> listRecord) throws Exception {
//        StringBuilder br = new StringBuilder();
//        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
//        br.setLength(0);
//        br.append("\r\n").
//                append("|\tID|").
//                append("|\tACTION_PROFILE_ID|").
//                append("|\tRECEIVE_DATE|").
//                append("|\tPK_ID\t|").
//                append("|\tISDN\t|").
//                append("|\tUSER\t|").
//                append("|\tACTION\t|").
//                append("|\tREASON\t|").
//                append("|\tISSUE_DATE\t|").
//                append("|SHOP|");
//        for (Record record : listRecord) {
//            Bonus bn = (Bonus) record;
//            br.append("\r\n").
//                    append("|\t").
//                    append(bn.getActionAuditId()).
//                    append("||\t").
//                    append(bn.getActionProfileId()).
//                    append("||\t").
//                    append((bn.getReceiverDate() != null ? sdf.format(bn.getReceiverDate()) : null)).
//                    append("||\t").
//                    append(bn.getPkId()).
//                    append("||\t").
//                    append(bn.getIsdnCustomer()).
//                    append("||\t").
//                    append(bn.getUserName()).
//                    append("||\t").
//                    append(bn.getActionId()).
//                    append("||\t").
//                    append(bn.getReasonId()).
//                    append("||\t").
//                    append((bn.getIssueDateTime() != null ? sdf.format(bn.getIssueDateTime()) : null)).
//                    append("||\t").
//                    append(bn.getShopCode());
//        }
//        logger.info(br);
//    }
//
//    @Override
//    public List<Record> processException(List<Record> listRecord, Exception ex) {
////        logger.warn("TEMPLATE process exception record: " + ex.toString());
////        for (Record record : listRecord) {
////            logger.info("TEMPLATE let convert to recort type you want and then set errCode, errDesc at here");
//////            MoRecord moRecord = (MoRecord) record;
//////            moRecord.setMessage("Thao tac that bai!");
//////            moRecord.setErrCode("-5");
////        }
//        return listRecord;
//    }
//
//    @Override
//    public boolean startProcessRecord() {
//        return true;
//    }
//}
