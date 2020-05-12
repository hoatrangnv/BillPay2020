///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbBonusReport;
//import com.viettel.paybonus.obj.BonusReport;
//import java.util.List;
//import org.apache.log4j.Logger;
//import com.viettel.threadfw.process.ProcessRecordAbstract;
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
//public class PayBonusReportDaily extends ProcessRecordAbstract {
//
//    DbBonusReport db;
//    String msg;
//
//    public PayBonusReportDaily() {
//        super();
//        logger = Logger.getLogger(PayBonusReportDaily.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        msg = ResourceBundle.getBundle("configPayBonus").getString("msgBonusReportDailyBegin");
//        db = new DbBonusReport();
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
//        String msgWarn;
//        for (Record record : listRecord) {
//            msgWarn = ResourceBundle.getBundle("configPayBonus").getString("msgBonusReportDaily");
//            BonusReport bn = (BonusReport) record;
//            listResult.add(bn);
//            String tel = db.getTelByStaffCode(bn.getStaffCode());
//            if (db.checkAlreadyWarningInDay(tel, msg)
//                    || db.checkAlreadyWarningInDayQueue(tel, msg)) {
//                logger.info("already send report so not send more, go next " + bn.getStaffCode() + " isdn " + tel);
//                continue;
//            }
//            db.getResultCheck(bn);
////            Step2 send sms
//            msgWarn = msgWarn.replace("%TOTAL%", bn.getTotalProfile() + "");
//            msgWarn = msgWarn.replace("%CORRECT%", bn.getTotalCorrect() + "");
//            msgWarn = msgWarn.replace("%INCORRECT%", bn.getTotalIncorrect() + "");
//            msgWarn = msgWarn.replace("%NOTCHECK%", bn.getTotalNotCheck() + "");
////            Step 3 update after warning
//            if (!tel.startsWith("258")) {
//                tel = "258" + tel;
//            }
//            db.sendSms(tel, msgWarn, "86142");
//            continue;
//        }
//        listRecord.clear();
//        Thread.sleep(300000);
//        return listResult;
//    }
//
//    @Override
//    public void printListRecord(List<Record> listRecord) throws Exception {
//        StringBuilder br = new StringBuilder();        
//        br.setLength(0);
//        br.append("\r\n").
//                append("|\tSTAFF_CODE|").
//                append("|\tTOTAL_PROFILE\t|");
//        for (Record record : listRecord) {
//            BonusReport bn = (BonusReport) record;
//            br.append("\r\n").
//                    append("|\t").
//                    append(bn.getStaffCode()).
//                    append("||\t").
//                    append(bn.getTotalProfile());
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
