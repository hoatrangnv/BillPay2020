///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.paybonus.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbAgentReportScratchCardOrder;
//import com.viettel.paybonus.obj.AgentReportInfo;
//import com.viettel.paybonus.obj.SaleTransOrder;
//import java.util.List;
//import org.apache.log4j.Logger;
//import com.viettel.threadfw.process.ProcessRecordAbstract;
//import java.math.BigDecimal;
//import java.math.RoundingMode;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.ResourceBundle;
//
///**
// *
// * @author dev_linh
// */
//public class AgentReportScratchCardOrder extends ProcessRecordAbstract {
//
//    DbAgentReportScratchCardOrder db;
//    SimpleDateFormat sdf;
//    SimpleDateFormat sdfHours;
//    SimpleDateFormat sdfDate;
//    String agentOrderReceiveReport;
//    String[] arrAgentOrderReceiveReport;
//
//    public AgentReportScratchCardOrder() {
//        super();
//        logger = Logger.getLogger(AgentReportScratchCardOrder.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        db = new DbAgentReportScratchCardOrder("dbsm", logger);
//        sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//        sdfHours = new SimpleDateFormat("HHmmss");
//        sdfDate = new SimpleDateFormat("yyyyMMdd");
//        agentOrderReceiveReport = ResourceBundle.getBundle("configPayBonus").getString("agentOrderReceiveReport");
//        arrAgentOrderReceiveReport = agentOrderReceiveReport.split("\\|");
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
//
//        logger.info("Step 0: Reset data for all branch first...");
//        db.resetDataReport();
//
//        for (Record record : listRecord) {
//            SaleTransOrder bn = (SaleTransOrder) record;
//            listResult.add(bn);
//            if ("0".equals(bn.getResultCode())) {
//                logger.info("Step 2: Update data newest..., saleTransOrderId: " + bn.getSaleTransOrderId());
//                List<AgentReportInfo> lstAgentReport = db.getListReportInfo();
//                if (lstAgentReport.size() == 13) {
//                    logger.info("Step 2: Update basic data for each branch..., saleTransOrderId: " + bn.getSaleTransOrderId());
//                    for (AgentReportInfo obj : lstAgentReport) {
//                        db.updateBasicInfo(obj);
//                    }
//                } else {
//                    logger.info("Have problem when get report data, not enough data for 13 Branch, saleTransOrderId: " + bn.getSaleTransOrderId());
//                    db.sendSmsV2("258870093239", "Not enough data for all branch.", "86952");
//                }
//
//            } else {
//                logger.warn("After validate respone code is fail actionId " + bn.getStaffId()
//                        + " so continue with other transaction");
//                continue;
//            }
//        }
//
//        logger.info("Step 4: Complete update data, now send sms report final");
//        for (String isdn : arrAgentOrderReceiveReport) {
//            db.sendSmsReport(isdn);
//        }
//        listRecord.clear();
//
//        return listResult;
//    }
//
//    @Override
//    public void printListRecord(List<Record> listRecord) throws Exception {
//        StringBuilder br = new StringBuilder();
//        br.setLength(0);
//        br.append("\r\n").
//                append("|\tSALE_TRANS_ORDER_ID|").
//                append("|\tSALE_TRANS_DATE\t|").
//                append("|\tRECEIVER_ID\t|").
//                append("|\tSYS_DATE\t|").
//                append("|\tSHOP_ID\t|").
//                append("|\tSTAFF_ID\t|").
//                append("|\t\t|");
//        for (Record record : listRecord) {
//            SaleTransOrder bn = (SaleTransOrder) record;
//            br.append("\r\n").
//                    append("|\t").
//                    append(bn.getSaleTransOrderId()).
//                    append("||\t").
//                    append(bn.getSaleTransDate()).
//                    append("||\t").
//                    append(bn.getReceiverId()).
//                    append("||\t").
//                    append(bn.getSysdate()).
//                    append("||\t").
//                    append(bn.getShopId()).
//                    append("||\t").
//                    append(bn.getStaffId()).
//                    append("||\t").
//                    append(bn.getSaleTransOrderId());
//        }
//        logger.info(br);
//    }
//
//    @Override
//    public List<Record> processException(List<Record> listRecord, Exception ex) {
//        ex.printStackTrace();
//        logger.warn("TEMPLATE process exception record: " + ex.toString());
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
