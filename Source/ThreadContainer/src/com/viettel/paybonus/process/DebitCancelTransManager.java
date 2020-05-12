///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbDebitRollback;
//import com.viettel.paybonus.obj.EwalletDebitLog;
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
//public class DebitCancelTransManager extends ProcessRecordAbstract {
//
//    Exchange pro;
//    DbDebitRollback db;
//
//    public DebitCancelTransManager() {
//        super();
//        logger = Logger.getLogger(DebitCancelTransManager.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
//        db = new DbDebitRollback("dbsm", logger);
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
//            String eWalletResponse = "";
//            EwalletDebitLog bn = (EwalletDebitLog) record;
//            listResult.add(bn);
//            if ("0".equals(bn.getResultCode())) {
//                logger.info("Start Revert money for staff " + bn.getStaffCode() + " money " + bn.getMoney()
//                        + ", orgRequestId " + bn.getRequestId() + " ewallet_debig_log_id " + bn.getId());
//                eWalletResponse = pro.callEwalletRollback(bn, db);
//                if ("01".equals(eWalletResponse)) {
//                    logger.info("Revert successfully for staff " + bn.getStaffCode()
//                            + ", orgRequestId " + bn.getRequestId() + " ewallet_debig_log_id " + bn.getId());
//                    db.updateEwalletDebitLog(bn.getStaffCode(), bn.getId(), bn.getRequestId(), "REVERT FOR CANCEL TRANS SUCCESS");
//                } else {
//                    logger.info("Revert fail for staff " + bn.getStaffCode()
//                            + ", orgRequestId " + bn.getRequestId() + " ewallet_debig_log_id " + bn.getId());
//                    db.updateEwalletDebitLog(bn.getStaffCode(), bn.getId(), bn.getRequestId(), "REVERT FOR CANCEL TRANS FAIL");
//                }
//            } else {
//                logger.warn("After validate respone code is fail actionId " + bn.getId()
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
//                append("|\tISDN\t|").
//                append("|\tRECEIVE_DATE\t|").
//                append("|\tMONEY\t|").
//                append("|\tORG_REQUEST_ID\t|").
//                append("|\tSTAFF_CODE\t|");
//        for (Record record : listRecord) {
//            EwalletDebitLog bn = (EwalletDebitLog) record;
//            br.append("\r\n").
//                    append("|\t").
//                    append(bn.getId()).
//                    append("||\t").
//                    append(bn.getIsdn()).
//                    append("||\t").
//                    append((bn.getLogTime() != null ? sdf.format(bn.getLogTime()) : null)).
//                    append("||\t").
//                    append(bn.getMoney()).
//                    append("||\t").
//                    append(bn.getRequestId()).
//                    append("||\t").
//                    append(bn.getStaffCode());
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
