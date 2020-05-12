///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbEmoneyProcessor;
//import com.viettel.paybonus.obj.ActionProfile;
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
//import java.util.Date;
//import java.util.ResourceBundle;
//import java.util.concurrent.ConcurrentHashMap;
//
///**
// *
// * @author HuyNQ13
// * @version 1.0
// * @since 24-03-2016
// */
//public class PayBonusInActionAudit extends ProcessRecordAbstract {
//
//    Exchange pro;
//    DbEmoneyProcessor db;
//
//    public PayBonusInActionAudit() {
//        super();
//        logger = Logger.getLogger(PayBonusInActionAudit.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
//        db = new DbEmoneyProcessor();
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
//        int totalCurrValue;
//        int totalCurrAddTime;
//        boolean isMaxValueOrAddTimes;
//        String transId;
//        String msg;
//        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
//        ActionProfile aProfile;
//        ConcurrentHashMap<String, Integer> mapCurrAddTimes = new ConcurrentHashMap<String, Integer>();
//        ConcurrentHashMap<String, Integer> mapCurrAddValue = new ConcurrentHashMap<String, Integer>();
//        String eWalletResponse;
//        long timeSt;
//        for (Record record : listRecord) {
//            timeSt = System.currentTimeMillis();
//            agent = null;
//            staffCode = "";
//            isdn = "";
//            isAllowChannel = false;
//            isAllowProduct = false;
//            sub = null;
//            itemfee = null;
//            totalCurrValue = 0;
//            totalCurrAddTime = 0;
//            isMaxValueOrAddTimes = true;
//            transId = sdf.format(new Date());
//            eWalletResponse = "";
//            msg = ResourceBundle.getBundle("configPayBonus").getString("message");
//            Bonus bn = (Bonus) record;
//            listResult.add(bn);
//            if ("0".equals(bn.getResultCode())) {
////                Check to make sure not yet processing this record, not duplicate process for each record (base on action_audit_id)
//                logger.info("Start check to make sure not duplicate process actionId " + bn.getActionAuditId());
//                if (db.checkAlreadyProcessRecord(bn.getActionAuditId())) {
//                    logger.warn("Already process record actionId " + bn.getActionAuditId());
//                    bn.setResultCode("E10");
//                    bn.setDescription("Already process record");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
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
//                if (bn.getPkId() == null) {
//                    logger.warn("Do not have Sub or Cust Id, actionId " + bn.getActionAuditId() + " isdnEmola: " + isdn);
//                    bn.setResultCode("E04");
//                    bn.setDescription("Do not have Sub or Cust Id");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
//                logger.info("Start check SubInfo actionId " + bn.getActionAuditId() + " pkid " + bn.getPkId() + " isdnEmola " + isdn);
//                sub = db.getSubInfoBySubId(bn.getPkId(), isdn);
//                if (sub == null || sub.getProductCode() == null || sub.getProductCode().length() <= 0) {
//                    logger.warn("Can not find subscriber info actionId " + bn.getActionAuditId()
//                            + " pkid " + bn.getPkId() + " isdnEmola: " + isdn);
//                    bn.setResultCode("E05");
//                    bn.setDescription("Can not find subscriber info");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                } else {
//                    bn.setProductCode(sub.getProductCode());
//                    bn.setActiveStatus(sub.getActStatus());
//                    bn.setActiveDate(sub.getActiveDate());
//                }
////                Check allow product       
//                logger.info("Start check allow Product actionId " + bn.getActionAuditId() + " isdnEmola " + isdn);
//                isAllowProduct = db.checkProductAllow(sub.getProductCode(), isdn);
//                if (!isAllowProduct) {
//                    logger.warn("Product is not allowed actionId " + bn.getActionAuditId()
//                            + " isdnEmola: " + isdn + " productCode " + sub.getProductCode());
//                    bn.setResultCode("E06");
//                    bn.setDescription("Product is not allowed");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
////              Get item fee
//                logger.info("Start check ItemFee actionId " + bn.getActionAuditId() + " isdnEmola " + isdn);
//                itemfee = db.getItemFee(agent.getChannelTypeId(), sub.getProductCode(), bn.getActionCode(), bn.getReasonId(), isdn);
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
////                Check profile if request (checprofile ==1)
//                logger.info("Start check Profile if request actionId " + bn.getActionAuditId()
//                        + " checkprofile " + itemfee.getCheckProfile() + " isdnEmola " + isdn);
//                if (itemfee.getCheckProfile() == 1) {
//                    aProfile = db.checkProfile(bn.getActionAuditId());
//                    bn.setActionProfileId(aProfile.getActionProfileId());
//                    if (!aProfile.isEnoughProfile()) {
//                        logger.warn("Profile do not have or not enough or invalid or not validated, actionId "
//                                + bn.getActionAuditId() + " isdnEmola: " + isdn);
//                        bn.setResultCode("E11");
//                        bn.setDescription("Do not have or not enough profile");
//                        bn.setDuration(System.currentTimeMillis() - timeSt);
//                        continue;
//                    }
//                }
////              get Current total value
//                if (mapCurrAddValue.get(isdn) == null || mapCurrAddValue.get(isdn) <= 0) {
//                    totalCurrValue = db.getCurrentValueInDay(isdn);
//                    mapCurrAddValue.put(isdn, totalCurrValue);
//                } else {
//                    totalCurrValue = mapCurrAddValue.get(isdn);
//                }
//                if (mapCurrAddTimes.get(isdn) == null || mapCurrAddTimes.get(isdn) <= 0) {
//                    totalCurrAddTime = db.getCurrentTimeAddInDay(isdn);
//                    mapCurrAddTimes.put(isdn, totalCurrAddTime);
//                } else {
//                    totalCurrAddTime = mapCurrAddTimes.get(isdn);
//                }
//                if (totalCurrValue == -1 || totalCurrAddTime == -1) {
//                    logger.error("System error had exception actionId, isdn: " + isdn);
//                    bn.setResultCode("E99");
//                    bn.setDescription("System error, can not get total value or total addtimes int day");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
////              Check over max_addtime or max value
//                logger.info("Start check over max value, max times: " + isdn);
//                isMaxValueOrAddTimes = db.checkMaxAddMaxValueInDay(agent.getChannelTypeId(), isdn, totalCurrValue, totalCurrAddTime);
//                if (isMaxValueOrAddTimes) {
//                    logger.warn("Limited total value or addtimes in day actionId " + bn.getActionAuditId() + ", isdnEmola: " + isdn);
//                    bn.setTotalCurrentValue(totalCurrValue);
//                    bn.setTotalCurrentAddTimes(totalCurrAddTime);
//                    bn.setResultCode("E08");
//                    bn.setDescription("Limited total value or addtimes in day");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
////              Call eWallet to add bonus
//                eWalletResponse = pro.callEwallet(bn.getActionAuditId(), agent.getChannelTypeId(), isdn, itemfee.getAmount(),
//                        bn.getActionCode(), staffCode, transId, db);
//                bn.seteWalletErrCode(eWalletResponse);
//                if ("01".equals(eWalletResponse)) {
//                    logger.info("Pay Bonus success for actionId " + bn.getActionAuditId() + " isdnEmola " 
//                            + isdn + " amount " + itemfee.getAmount());
//                    bn.setTotalCurrentValue(totalCurrValue + (int) itemfee.getAmount());
//                    bn.setTotalCurrentAddTimes(totalCurrAddTime + 1);
//                    mapCurrAddValue.put(isdn, totalCurrValue + (int) itemfee.getAmount());
//                    mapCurrAddTimes.put(isdn, totalCurrAddTime + 1);
//                    bn.setResultCode("0");
//                    bn.setDescription("Pay Bonus success for isdnEmola " + isdn + " amount " + itemfee.getAmount());
//                    msg = msg.replace("XXX", "" + itemfee.getAmount());
//                    msg = msg.replace("YYY", isdn);
//                    msg = msg.replace("ZZZ", "" + (totalCurrAddTime + 1));
//                    msg = msg.replace("WWW", "" + (totalCurrValue + (int) itemfee.getAmount()));
//                    bn.setMessage(msg);
//                    logger.info("Message will send to isdnEmola " + isdn + " msg: " + msg);
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                } else {
//                    logger.error("Pay Bonus fail for actionId " + bn.getActionAuditId() +
//                            " isdnEmola " + isdn + " amount " + itemfee.getAmount());
//                    bn.setResultCode("E09");
//                    bn.setDescription("Pay Bonus fail for isdnEmola " + isdn + " amount " + itemfee.getAmount());
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
//            } else {
//                logger.warn("After validate respone code is fail actionId " + bn.getActionAuditId()
//                        + " so continue with other transaction");
//                continue;
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
//                append("|\tPK_ID\t|").
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
//                    append(bn.getPkId()).
//                    append("||\t").
//                    append(bn.getUserName()).
//                    append("||\t").
//                    append(bn.getActionCode()).
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
