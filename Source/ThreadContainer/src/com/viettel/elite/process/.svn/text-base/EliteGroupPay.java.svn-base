///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.viettel.elite.process;
//
//import com.viettel.cluster.agent.integration.Record;
//import com.viettel.paybonus.database.DbEliteGroupPay;
//import com.viettel.paybonus.obj.EliteGroupInput;
//import com.viettel.paybonus.service.Exchange;
//import com.viettel.threadfw.manager.AppManager;
//import java.util.List;
//import org.apache.log4j.Logger;
//import com.viettel.threadfw.process.ProcessRecordAbstract;
//import com.viettel.vas.util.ExchangeClientChannel;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.ResourceBundle;
//
///**
// *
// * @author LamNT
// * @version 1.0
// * @since 05-01-2019
// */
//public class EliteGroupPay extends ProcessRecordAbstract {
//
//    Exchange pro;
//    DbEliteGroupPay db;
//    SimpleDateFormat sdf = new SimpleDateFormat("MMyyyy");
//    ArrayList<String> listErrNotEnoughBalance;
//    String eliteMsgBuyFail;
//
//    public EliteGroupPay() {
//        super();
//        logger = Logger.getLogger(EliteGroupPay.class);
//    }
//
//    @Override
//    public void initBeforeStart() throws Exception {
//        db = new DbEliteGroupPay();
//        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
//        listErrNotEnoughBalance = new ArrayList<String>(Arrays.asList(ResourceBundle.getBundle("configPayBonus").getString("ERR_BALANCE_NOT_ENOUGH").split("\\|")));
//        eliteMsgBuyFail = ResourceBundle.getBundle("configPayBonus").getString("lwMsgSytemFail");
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
//        long moneyFee;
//        String resultChargeMoney;
//        for (Record record : listRecord) {
//            resultChargeMoney = "";
//            moneyFee = 0;
//            EliteGroupInput moRecord = (EliteGroupInput) record;
//            listResult.add(moRecord);
//
//            moneyFee = moRecord.getMoneyFee();
//            // Add bonus for winner
//            resultChargeMoney = pro.topupPrePaid(moRecord.getMsisdn(), "" + moneyFee);
//            if (!"0".equals(resultChargeMoney)) {
//                logger.warn("Fail to charge money " + moRecord.getMsisdn() + " money " + moneyFee
//                        + " errcode chargemoney " + resultChargeMoney);
//                moRecord.setResultCode("E03");
//                moRecord.setDesctiption("Fail to charge money");
//                continue;
//            }
//            moRecord.setResultCode("0");
//            moRecord.setDesctiption("Success to change product");
//            continue;
//
//        }
//        listRecord.clear();
//        return listResult;
//    }
//
//    @Override
//    public void printListRecord(List<Record> listRecord) throws Exception {
//        StringBuilder br = new StringBuilder();
//        br.setLength(0);
//        br.append("\r\n").
//                append("|\tKIT_VAS_ID|").
//                append("|\tISDN|").
//                append("|\tSERIAL\t|").
//                append("|\tCREATE_USER\t|").
//                append("|\tPRODUCT_CODE\t|");
//        for (Record record : listRecord) {
//            EliteGroupInput bn = (EliteGroupInput) record;
//            br.append("\r\n").
//                    append("|\t").
//                    append(bn.getMoId()).
//                    append("||\t").
//                    append(bn.getMsisdn()).
//                    append("||\t").
//                    append(bn.getCommand()).
//                    append("||\t").
//                    append(bn.getReceiveTime()).
//                    append("||\t").
//                    append(bn.getChannelType());
//        }
//        logger.info(br);
//    }
//
//    @Override
//    public List<Record> processException(List<Record> listRecord, Exception ex) {
//        return listRecord;
//    }
//
//    @Override
//    public boolean startProcessRecord() {
//        return true;
//    }
//}
