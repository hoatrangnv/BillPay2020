///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbBocProcessor;
//import com.viettel.paybonus.database.DbSubProfileProcessor;
//import com.viettel.paybonus.obj.LogUpdateInfo;
//import com.viettel.paybonus.service.Exchange;
//import com.viettel.threadfw.manager.AppManager;
//import java.util.List;
//import org.apache.log4j.Logger;
//import com.viettel.threadfw.process.ProcessRecordAbstract;
//import com.viettel.vas.util.ExchangeClientChannel;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.ResourceBundle;
//
///**
// *
// * @author HuyNQ13
// * @version 1.0
// * @since 24-03-2016
// */
//public class AddMoneyForCust extends ProcessRecordAbstract {
//
//    Exchange pro;
//    DbBocProcessor db;
//    DbSubProfileProcessor dbSubInf;
//    String moneyAdd;
//    String accountCode;
//    String countryCode;
//    String msg;
//    String pricePlanId;
//    String pricePlanEffectTime;
//    String pricePlanAddDay;
//    String msgChannelUpdateIncorrectImage;
//    String msgChannelUpdateIncorrectInfo;
//    String msgChannelUpdateBadQuality;
//    String msgChannelUpdateIncorrectBoth;
//    String msgChannelUpdateCorrectInfo;
//    String msgCusUpdateIncorrectImage;
//    String msgCusUpdateIncorrectInfo;
//    String msgCusUpdateBadQuality;
//    String msgCusUpdateIncorrectBoth;
//    String msgCusUpdateCorrectInfo;
//    String msgFailToCustomer;
//    String msgMissingImage;
//    String msgInvalidStudentCard;
//    String msgMissingStudentCard;
//
//    public AddMoneyForCust() {
//        super();
//        logger = Logger.getLogger(AddMoneyForCust.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        moneyAdd = ResourceBundle.getBundle("configPayBonus").getString("money_add");
//        accountCode = ResourceBundle.getBundle("configPayBonus").getString("account_code");
//        pricePlanId = ResourceBundle.getBundle("configPayBonus").getString("pricePlanId");
//        if ((moneyAdd == null && pricePlanId == null) || ("".equals(moneyAdd.trim()) && "".equals(pricePlanId.trim()))) {
//            throw new Exception("Must config moneyAdd or PricePlanId");
//        }
//        pricePlanEffectTime = ResourceBundle.getBundle("configPayBonus").getString("pricePlanEffectTime");
//        pricePlanAddDay = ResourceBundle.getBundle("configPayBonus").getString("pricePlanAddDay");
//        countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
//
//        msgFailToCustomer = ResourceBundle.getBundle("configPayBonus").getString("msgFalseCustomer");
//
//        msgChannelUpdateIncorrectImage = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpdateIncorrectImage");
//        msgChannelUpdateIncorrectInfo = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpdateIncorrectInfo");
//        msgChannelUpdateBadQuality = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpdateBadQuality");
//        msgChannelUpdateIncorrectBoth = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpdateIncorrectBoth");
//        msgChannelUpdateCorrectInfo = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpdateCorrectInfo");
//
//        msgCusUpdateIncorrectImage = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpdateIncorrectImage");
//        msgCusUpdateIncorrectInfo = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpdateIncorrectInfo");
//        msgCusUpdateBadQuality = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpdateBadQuality");
//        msgCusUpdateIncorrectBoth = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpdateIncorrectBoth");
//        msgCusUpdateCorrectInfo = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpdateCorrectInfo");
//
//
//        msgMissingImage = ResourceBundle.getBundle("configPayBonus").getString("msgMissingImage");
//        msgMissingStudentCard = ResourceBundle.getBundle("configPayBonus").getString("msgMissingStudentCard");
//        msgInvalidStudentCard = ResourceBundle.getBundle("configPayBonus").getString("msgFalseCustomer");
//
//        msg = ResourceBundle.getBundle("configPayBonus").getString("message_add_money");
//        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
//        db = new DbBocProcessor();
//        dbSubInf = new DbSubProfileProcessor();
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
//        String addMoneyResult;
//        String openFlagResult;
//        String openFlagPLMResult;
//        String openFlagBAOCResult;
//        String activeOCS;
//        Long checkStatus;
//        Long updateSubProfileInfo;
//        String openFlagGPRSResult;
//        String openFlagBAICResult;
//        for (Record record : listRecord) {
//            addMoneyResult = "";
//            openFlagResult = "";
//            openFlagPLMResult = "";
//            openFlagBAOCResult = "";
//            activeOCS = "";
//            openFlagGPRSResult = "";
//            openFlagBAICResult = "";
//            checkStatus = 0l;
//            LogUpdateInfo bn = (LogUpdateInfo) record;
//            listResult.add(bn);
////            checkStatus = db.getProfileCheckStatus(bn.getIsdn());
//            checkStatus = bn.getCheckStatus();
//            if (checkStatus == 1) {
//                //Profile  correct >>>> add 50MT using 7 days.
//                if ("0".equals(bn.getResultCode())) {
//
////LamNT opend block one way(Request of MR.DAT 05072018 block new register error information)
////                Open USSD flag on HLR for customer -- HLR_HW_MOD_PLMNSS
//                    openFlagPLMResult = pro.activeFlagPLMNSS("258" + bn.getIsdn());
//                    if ("0".equals(openFlagPLMResult)) {
//                        logger.info("Open flag PLMNSS successfully for sub when register info " + bn.getIsdn());
//                    } else {
//                        logger.warn("Fail to open flag BAOC for sub when register info " + bn.getIsdn());
//                    }
////                Open BAOC flag on HLR for customer -- HLR_HW_MOD_BAOC
//                    openFlagBAOCResult = pro.activeFlagBAOC("258" + bn.getIsdn());
//                    if ("0".equals(openFlagBAOCResult)) {
//                        logger.info("Open flag BAOC successfully for sub when register info " + bn.getIsdn());
//                    } else {
//                        logger.warn("Fail to open flag BAOC for sub when register info " + bn.getIsdn());
//                    }
//                    openFlagBAICResult = pro.activeFlagBAIC("258" + bn.getIsdn());
//                    if ("0".equals(openFlagBAICResult)) {
//                        logger.info("Open flag BAIC successfully for sub when register info " + bn.getIsdn());
//                    } else {
//                        logger.warn("Fail to open flag BAIC for sub when register info " + bn.getIsdn());
//                    }
////               Update block_ocs_hlr=0 is unblock one way
//                    updateSubProfileInfo = dbSubInf.updateSubProfileInfo(bn.getIsdn());
//                    if (updateSubProfileInfo == 1) {
//                        logger.info("Update open block one way on OCS_HLR " + bn.getIsdn());
//                    } else {
//                        logger.warn("Fail to Update open block one way on OCS_HLR" + bn.getIsdn());
//                    }
////               Active on OCS OCSHW_MODI_VALIDITY to request time block two way
//                    activeOCS = pro.activeOCS("258" + bn.getIsdn());
//                    if ("0".equals(activeOCS)) {
//                        logger.info("Active flag OCSHW_MODI_VALIDITY successfully for sub " + bn.getIsdn());
//                    } else {
//                        logger.warn("Fail to Active flag OCSHW_MODI_VALIDITY successfully for sub  " + bn.getIsdn());
//                    }
////LamNT end Active block two way
//
//                    openFlagGPRSResult = pro.activeFlagGPRSLCK("258" + bn.getIsdn());
//                    if ("0".equals(openFlagGPRSResult)) {
//                        logger.info("Open flag GPRS successfully for sub when register info " + bn.getIsdn());
//                    } else {
//                        logger.warn("Fail to open flag GPRS for sub when register info " + bn.getIsdn());
//                    }
////                Open ODBOC flag on HLR for customer
//                    openFlagResult = pro.activeFlagSABLCK(countryCode + bn.getIsdn());
//                    if ("0".equals(openFlagResult)) {
//                        logger.info("Open flag SABLCK successfully for sub when updating info " + bn.getIsdn());
//                    } else {
//                        logger.warn("Fail to open flag SABLCK for sub when updating info " + bn.getIsdn());
//                    }
////                Check to make sure not yet processing this record, not duplicate process for each record (base on log_update_info_id)
//                    logger.info("Start check to make sure not duplicate process id " + bn.getLogUpdateInfoId());
//                    if (db.checkAlreadyProcessRecord(bn.getLogUpdateInfoId())) {
//                        logger.warn("Already process record actionId " + bn.getLogUpdateInfoId() + " isdn " + bn.getIsdn());
//                        bn.setResultCode("E01");
//                        bn.setDescription("Already process record");
//                        continue;
//                    }
////                Check to make sure each isdn only receive bonus only one time, although updating many times
//                    logger.info("Start check to make sure only receive bonus one time id " + bn.getLogUpdateInfoId());
//                    if (db.checkBonusOneTime(bn.getIsdn())) {
//                        logger.warn("The isdn " + bn.getIsdn() + " already received bonus before so not get more");
//                        bn.setResultCode("E02");
//                        bn.setDescription("Already received bonus before so not get more");
//                        continue;
//                    }
////                Add money Or Add PricePlan
//                    if (!"".equals(pricePlanId)) {
//                        addMoneyResult = pro.addPrice(countryCode + bn.getIsdn(), pricePlanId, pricePlanEffectTime, pricePlanAddDay);
//                    } else {
//                        addMoneyResult = pro.addMoney(countryCode + bn.getIsdn(), moneyAdd, accountCode);
//                    }
//                    if ("0".equals(addMoneyResult)) {
//                        logger.info("Add money success for id " + bn.getLogUpdateInfoId() + " isdn "
//                                + bn.getIsdn() + " amount " + moneyAdd + " priceplan " + pricePlanId);
//                        bn.setResultCode("0");
//                        bn.setDescription("Add money success for " + bn.getIsdn() + " amount " + moneyAdd + " priceid " + pricePlanId);
//                        bn.setMessage(msgCusUpdateCorrectInfo.replace("%XYZ%", bn.getIsdn()));
//                        if (!"".equals(moneyAdd)) {
//                            bn.setMoneyAdd(Long.valueOf(moneyAdd));
//                        }
//                        bn.setAccountCode(accountCode);
//                        logger.info("Message will send to isdnEmola " + bn.getIsdn() + " msg: " + msg);
//                        String tel = db.getTelByStaffCode(bn.getStaffCode());
////                        String serial = db.getSerialByIsdnCustomer(bn.getIsdn());
//                        String tempMsg = msgChannelUpdateCorrectInfo.replace("%XYZ%", bn.getIsdn());
//                        db.sendSms("258" + tel, tempMsg, "86904");
//                        logger.info("Message send to staff: " + tempMsg + " isdn " + tel);
//                        continue;
//                    } else {
//                        logger.error("Add money fail for id " + bn.getLogUpdateInfoId()
//                                + " isdn " + bn.getIsdn() + " amount " + moneyAdd);
//                        bn.setResultCode("E03");
//                        bn.setDescription("Add money fail for pro resultcode " + addMoneyResult);
//                        continue;
//                    }
//                } else {
//                    logger.warn("After validate respone code is fail id " + bn.getLogUpdateInfoId() + " isdn " + bn.getIsdn()
//                            + " so continue with other transaction");
//                    continue;
//                }
//            } else {
//                //Profile not correct...send sms to customer to re-update again.
//                logger.warn("Profile not correct " + bn.getLogUpdateInfoId() + " isdn " + bn.getIsdn());
//                if (checkStatus.equals(2L)) {
//                    logger.warn("Incorrect Image, id " + bn.getId() + " isdn " + bn.getIsdn());
//                    bn.setResultCode("E05");
//                    bn.setDescription("Invalid profile Image");
//                    bn.setMessage(msgCusUpdateIncorrectImage.replace("%XYZ%", bn.getIsdn()));
//                    String tel = db.getTelByStaffCode(bn.getStaffCode());
////                    String serial = db.getSerialByIsdnCustomer(bn.getIsdn());
//                    String tempMsg = msgChannelUpdateIncorrectImage.replace("%XYZ%", bn.getIsdn());
//                    db.sendSms("258" + tel, tempMsg, "86904");
//                    logger.info("Message send to staff: " + tempMsg + " isdn " + tel);
//                    continue;
//                }
//                if (checkStatus.equals(3L)) {
//                    logger.warn("Incorrect Infomation, id " + bn.getId() + " isdn " + bn.getIsdn());
//                    bn.setResultCode("E06");
//                    bn.setDescription("Invalid profile Info");
//                    bn.setMessage(msgCusUpdateIncorrectInfo.replace("%XYZ%", bn.getIsdn()));
//                    String tel = db.getTelByStaffCode(bn.getStaffCode());
////                    String serial = db.getSerialByIsdnCustomer(bn.getIsdn());
//                    String tempMsg = msgCusUpdateIncorrectInfo.replace("%XYZ%", bn.getIsdn());
//                    db.sendSms("258" + tel, tempMsg, "86904");
//                    logger.info("Message send to staff: " + tempMsg + " isdn " + tel);
//                    continue;
//                }
//                if (checkStatus.equals(4L)) {
//                    logger.warn("Incorrect Both, id " + bn.getId() + " isdn " + bn.getIsdn());
//                    bn.setResultCode("E07");
//                    bn.setDescription("Invalid profile both information");
//                    bn.setMessage(msgCusUpdateIncorrectBoth.replace("%XYZ%", bn.getIsdn()));
//                    String tel = db.getTelByStaffCode(bn.getStaffCode());
////                    String serial = db.getSerialByIsdnCustomer(bn.getIsdn());
//                    String tempMsg = msgChannelUpdateIncorrectBoth.replace("%XYZ%", bn.getIsdn());
//                    db.sendSms("258" + tel, tempMsg, "86904");
//                    logger.info("Message send to staff: " + tempMsg + " isdn " + tel);
//                    continue;
//                }
//                if (checkStatus.equals(5L)) {
//                    logger.warn("Bad quality image, id " + bn.getId() + " isdn " + bn.getIsdn());
//                    bn.setResultCode("E08");
//                    bn.setDescription("Bad quality image");
//                    bn.setMessage(msgCusUpdateBadQuality.replace("%XYZ%", bn.getIsdn()));
//                    String tel = db.getTelByStaffCode(bn.getStaffCode());
////                    String serial = db.getSerialByIsdnCustomer(bn.getIsdn());
//                    String tempMsg = msgCusUpdateBadQuality.replace("%XYZ%", bn.getIsdn());
//                    db.sendSms("258" + tel, tempMsg, "86904");
//                    logger.info("Message send to staff: " + tempMsg + " isdn " + tel);
//                    continue;
//                }
//                if (checkStatus.equals(6L)) {
//                    logger.warn("Miss student card, id " + bn.getId() + " isdn " + bn.getIsdn());
//                    bn.setResultCode("E09");
//                    bn.setDescription("Miss student card");
//                    bn.setMessage(msgFailToCustomer.replace("%PHONE%", bn.getIsdn()));
//                    String tel = db.getTelByStaffCode(bn.getStaffCode());
////                    String serial = db.getSerialByIsdnCustomer(bn.getIsdn());
//                    String tempMsg = msgMissingStudentCard.replace("%XYZ%", bn.getIsdn());
//                    db.sendSms("258" + tel, tempMsg, "86904");
//                    logger.info("Message send to staff: " + tempMsg + " isdn " + tel);
//                    continue;
//                }
//                if (checkStatus.equals(7L)) {
//                    logger.warn("Missing Image, id " + bn.getId() + " isdn " + bn.getIsdn());
//                    bn.setResultCode("E10");
//                    bn.setDescription("Missing Image");
//                    bn.setMessage(msgFailToCustomer.replace("%PHONE%", bn.getIsdn()));
//                    String tel = db.getTelByStaffCode(bn.getStaffCode());
////                    String serial = db.getSerialByIsdnCustomer(bn.getIsdn());
//                    String tempMsg = msgMissingImage.replace("%XYZ%", bn.getIsdn());
//                    db.sendSms("258" + tel, tempMsg, "86904");
//                    logger.info("Message send to staff: " + tempMsg + " isdn " + tel);
//                    continue;
//                }
//                if (checkStatus.equals(8L)) {
//                    logger.warn("Invalid student card, id " + bn.getId() + " isdn " + bn.getIsdn());
//                    bn.setResultCode("E11");
//                    bn.setDescription("Invalid profile Info");
//                    bn.setMessage(msgFailToCustomer.replace("%PHONE%", bn.getIsdn()));
//                    String tel = db.getTelByStaffCode(bn.getStaffCode());
////                    String serial = db.getSerialByIsdnCustomer(bn.getIsdn());
//                    String tempMsg = msgInvalidStudentCard.replace("%XYZ%", bn.getIsdn());
//                    db.sendSms("258" + tel, tempMsg, "86904");
//                    logger.info("Message send to staff: " + tempMsg + " isdn " + tel);
//                    continue;
//                } else {
//                    logger.warn("Invalid result check, id " + bn.getId() + " isdn " + bn.getIsdn());
//                    bn.setResultCode("E12");
//                    bn.setDescription("Invalid result check");
//                    bn.setMessage(msgCusUpdateIncorrectBoth.replace("%XYZ%", bn.getIsdn()));
//                    String tel = db.getTelByStaffCode(bn.getStaffCode());
////                    String serial = db.getSerialByIsdnCustomer(bn.getIsdn());
//                    String tempMsg = msgChannelUpdateIncorrectBoth.replace("%XYZ%", bn.getIsdn());
//                    db.sendSms("258" + tel, tempMsg, "86904");
//                    logger.info("Message send to staff: " + tempMsg + " isdn " + tel);
//                    continue;
//                }
//            }
//
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
//                append("|\tSTAFFCODE\t|").
//                append("|\tISDN\t|").
//                append("|\tCHECK_STATUS\t|").
//                append("|\tCREATE_TIME\t|");
//        for (Record record : listRecord) {
//            LogUpdateInfo bn = (LogUpdateInfo) record;
//            br.append("\r\n").
//                    append("|\t").
//                    append(bn.getLogUpdateInfoId()).
//                    append("||\t").
//                    append(bn.getStaffCode()).
//                    append("||\t").
//                    append(bn.getIsdn()).
//                    append("||\t").
//                    append(bn.getCheckStatus()).
//                    append("||\t").
//                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null));
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
