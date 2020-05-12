/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.bank.manage;

import com.viettel.paybonus.process.*;
import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbPaymentByBank;
import com.viettel.paybonus.obj.BankFileDetail;
import com.viettel.paybonus.obj.SubAdslLLPrepaid;
import com.viettel.paybonus.service.Service;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.ResourceBundle;
import sun.misc.BASE64Encoder;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class PaymentByBank extends ProcessRecordAbstract {

    Service services;
    DbPaymentByBank db;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private String wsPay;

    public PaymentByBank() {
        super();
        logger = Logger.getLogger(KitRegisterVas.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        services = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), logger);
        db = new DbPaymentByBank("dbPayByBank", logger);
        wsPay = ResourceBundle.getBundle("cfgBankFile").getString("wsPay");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            BankFileDetail moRecord = (BankFileDetail) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
//        boolean isFtthPre;
//        long excessMoney;
//        long payVal;
//        SubAdslLLPrepaid subPrepaid;
//        String account;
        for (Record record : listRecord) {
//            isFtthPre = false;
//            excessMoney = 0;
//            payVal = 0;
//            subPrepaid = null;
//            account = "";
            BankFileDetail bn = (BankFileDetail) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                long contractId = db.getContractFbbByRefer(bn.getReference());
//                if (contractId <= 0) {
//                    logger.warn("Invalid reference " + bn.getBankFileDetailId() + ", isdn: " + bn.getReference());
//                    bn.setResultCode("E3");
//                    bn.setDescription("After validate respone code is fail");
//                    continue;
//                }
//                20200116 Commend for BCCS 3.0
//                subPrepaid = db.checkFtthPrepaid(contractId, bn.getReference());
//                if (subPrepaid != null && subPrepaid.getExpireTime() != null) {
//                    isFtthPre = true;
//                    excessMoney = subPrepaid.getExcessMoney();
//                    account = subPrepaid.getAccount();
//                    logger.info("Is prepaid FTTH and excess_money is " + excessMoney + " " + bn.getBankFileDetailId() + ", isdn: "
//                            + bn.getReference() + " contractId " + contractId + " account " + account);
//                }
//                20200116 Commend for BCCS 3.0
//                payVal = bn.getValuePay() + excessMoney;
//                if (payVal > 100000) {
//                    logger.warn("Too big money, over 100.000 MT so not support " + bn.getBankFileDetailId() + ", isdn: " + bn.getReference());
//                    bn.setResultCode("E4");
//                    bn.setDescription("Too big money, over 100.000 MT so not support");
//                    continue;
//                }
                String requestId = sdf.format(new Date()) + bn.getBankFileDetailId();
                String signature = PaymentByBank.hashSHA1(bn.getReference() + bn.getValuePay() + requestId);
                String resultRegister = services.callPaymentGw(requestId, bn.getReference(), bn.getValuePay() + "", signature,
                        "mov", "1", "1", "movPayForBank", "movPayForBank@2018", wsPay);
//                if ("36".equals(resultRegister)) {
//                    if (isFtthPre) {
//                        logger.warn("Is prepaid FTTH and invalid money so now save excess_money, old value "
//                                + excessMoney + " new value " + payVal
//                                + bn.getBankFileDetailId() + ", isdn: " + bn.getReference()
//                                + " contractId " + contractId + " account " + account);
//                        db.saveExcessMoney(contractId, account, payVal);
//                        bn.setResultCode("E5");
//                        bn.setDescription("Is prepaid FTTH and invalid money so must save excess_money old value "
//                                + excessMoney + " new value " + payVal + " errcode " + resultRegister);
//                    } else {
//                        logger.warn("Error occur when callPaymentGw BankFileDetailID : " + bn.getBankFileDetailId()
//                                + ", isdn: " + bn.getReference());
//                        bn.setResultCode("E1");
//                        bn.setDescription("Error occur when callPaymentGw " + resultRegister);
//                    }
//                } else 
                if (!"0".equals(resultRegister)) {
                    logger.warn("Error occur when callPaymentGw BankFileDetailID : " + bn.getBankFileDetailId()
                            + ", isdn: " + bn.getReference());
                    bn.setResultCode("E1");
                    bn.setDescription("Error occur when callPaymentGw " + resultRegister);
                } else {
                    logger.warn("callPaymentGw successfully, BankFileDetailID: " + bn.getBankFileDetailId() + ", isdn: " + bn.getReference());
                    bn.setResultCode("0");
                    bn.setDescription("callPaymentGw successfully");
//                    if (isFtthPre) {
//                        logger.warn("Is prepaid FTTH and sucess so now reset excess_money, old value "
//                                + excessMoney + " new value " + 0
//                                + bn.getBankFileDetailId() + ", isdn: " + bn.getReference()
//                                + " contractId " + contractId + " account " + account);
//                        db.saveExcessMoney(contractId, account, 0);
//                    }
                }
            } else {
                logger.warn("After validate respone code is fail bankFileDetailId " + bn.getID()
                        + " so continue with other transaction");
                bn.setResultCode("E2");
                bn.setDescription("After validate respone code is fail");
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
                append("|\tbank_file_detail_id|").
                append("|\tbank_file_info_id|").
                append("|\treference\t|").
                append("|\tvalue_pay\t|").
                append("|\timport_time\t|");
        for (Record record : listRecord) {
            BankFileDetail bn = (BankFileDetail) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getBankFileDetailId()).
                    append("||\t").
                    append(bn.getBankFileInfoId()).
                    append("||\t").
                    append(bn.getReference()).
                    append("||\t").
                    append(bn.getValuePay()).
                    append("||\t").
                    append(sdf.format(bn.getImportTime()));
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

    public static synchronized String hashSHA1(String plaintext) throws Exception {
        MessageDigest md = null;
        md = MessageDigest.getInstance("SHA-1"); //step 2
        md.update(plaintext.getBytes("UTF-8")); //step 3
        byte raw[] = md.digest(); //step 4
        String hash = (new BASE64Encoder()).encode(raw); //step 5
        return hash; //step 6
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
}
