///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbBocProcessor;
//import com.viettel.paybonus.obj.LogUpdateInfo;
//import com.viettel.paybonus.service.Exchange;
//import com.viettel.threadfw.manager.AppManager;
//import java.util.List;
//import org.apache.log4j.Logger;
//import com.viettel.threadfw.process.ProcessRecordAbstract;
//import com.viettel.vas.util.ExchangeClientChannel;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//
///**
// *
// * @author HuyNQ13
// * @version 1.0
// * @since 24-03-2016
// */
//public class Temp extends ProcessRecordAbstract {
//
//    Exchange pro;
//    DbBocProcessor db;
//
//    public Temp() {
//        super();
//        logger = Logger.getLogger(Temp.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
//        db = new DbBocProcessor();
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
//
//
////LamNT opend block one way(Request of MR.DAT 05072018 block new register error information)
////                Open USSD flag on HLR for customer -- HLR_HW_MOD_PLMNSS
//            openFlagPLMResult = pro.activeFlagPLMNSS("258" + bn.getIsdn());
//            if ("0".equals(openFlagPLMResult)) {
//                logger.info("Open flag PLMNSS successfully for sub when register info " + bn.getIsdn());
//            } else {
//                logger.warn("Fail to open flag BAOC for sub when register info " + bn.getIsdn());
//            }
////                Open BAOC flag on HLR for customer -- HLR_HW_MOD_BAOC
//            openFlagBAOCResult = pro.activeFlagBAOC("258" + bn.getIsdn());
//            if ("0".equals(openFlagBAOCResult)) {
//                logger.info("Open flag BAOC successfully for sub when register info " + bn.getIsdn());
//            } else {
//                logger.warn("Fail to open flag BAOC for sub when register info " + bn.getIsdn());
//            }
//            openFlagBAICResult = pro.activeFlagBAIC("258" + bn.getIsdn());
//            if ("0".equals(openFlagBAICResult)) {
//                logger.info("Open flag BAIC successfully for sub when register info " + bn.getIsdn());
//            } else {
//                logger.warn("Fail to open flag BAIC for sub when register info " + bn.getIsdn());
//            }
////               Active on OCS OCSHW_MODI_VALIDITY to request time block two way
//            activeOCS = pro.activeOCS("258" + bn.getIsdn());
//            if ("0".equals(activeOCS)) {
//                logger.info("Active flag OCSHW_MODI_VALIDITY successfully for sub " + bn.getIsdn());
//            } else {
//                logger.warn("Fail to Active flag OCSHW_MODI_VALIDITY successfully for sub  " + bn.getIsdn());
//            }
////LamNT end Active block two way
//
//            openFlagGPRSResult = pro.activeFlagGPRSLCK("258" + bn.getIsdn());
//            if ("0".equals(openFlagGPRSResult)) {
//                logger.info("Open flag GPRS successfully for sub when register info " + bn.getIsdn());
//            } else {
//                logger.warn("Fail to open flag GPRS for sub when register info " + bn.getIsdn());
//            }
////                Open ODBOC flag on HLR for customer
//            openFlagResult = pro.activeFlagSABLCK("258" + bn.getIsdn());
//            if ("0".equals(openFlagResult)) {
//                logger.info("Open flag SABLCK successfully for sub when updating info " + bn.getIsdn());
//            } else {
//                logger.warn("Fail to open flag SABLCK for sub when updating info " + bn.getIsdn());
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
