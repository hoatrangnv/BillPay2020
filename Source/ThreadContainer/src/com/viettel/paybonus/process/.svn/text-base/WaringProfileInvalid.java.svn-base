///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbProfileWarning;
//import com.viettel.paybonus.obj.Bonus;
//import java.util.List;
//import org.apache.log4j.Logger;
//import com.viettel.threadfw.process.ProcessRecordAbstract;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.ResourceBundle;
//
///**
// *
// * @author HuyNQ1
// * @version 1.0
// * @since 24-03-2016
// */
//public class WaringProfileInvalid extends ProcessRecordAbstract {
//
//    DbProfileWarning db;
//    String msg;
//
//    public WaringProfileInvalid() {
//        super();
//        logger = Logger.getLogger(WaringProfileInvalid.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        msg = ResourceBundle.getBundle("configPayBonus").getString("profile_invalid_message");
//        db = new DbProfileWarning();
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
//        for (Record record : listRecord) {
//            Bonus bn = (Bonus) record;
//            listResult.add(bn);
////            Step 1 Check do not have correct profile
//            if (!db.checkHaveCorrectProfile(bn.getIsdnCustomer())
//                    && !db.checkAlreadyWarningInDay(bn.getIsdnCustomer(), msg)
//                    && !db.checkAlreadyWarningInDayQueue(bn.getIsdnCustomer(), msg)) {
//                db.sendSms(bn.getIsdnCustomer(), msg, "86904");
//            }
//        }
//        listRecord.clear();
//        Thread.sleep(3000);
//        return listResult;
//    }
//
//    @Override
//    public void printListRecord(List<Record> listRecord) throws Exception {
//        StringBuilder br = new StringBuilder();
//        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
//        br.setLength(0);
//        br.append("\r\n").
//                append("|\tISDN|").
//                append("|\tSTAFF_CODE\t|").
//                append("|\tACTION_AUDIT_ID\t|");
//        for (Record record : listRecord) {
//            Bonus bn = (Bonus) record;
//            br.append("\r\n").
//                    append("|\t").
//                    append(bn.getIsdnCustomer()).
//                    append("|\t").
//                    append(bn.getStaffCode()).
//                    append("|\t").
//                    append(bn.getId());
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
