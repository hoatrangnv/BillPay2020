/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.bank.manage;

import com.viettel.paybonus.process.*;
import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbPaymentByBank;
import com.viettel.paybonus.obj.BankFileDetail;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import sun.misc.BASE64Encoder;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class ClearDebitByBank extends ProcessRecordAbstract {

//    Service services;
    DbPaymentByBank db;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public ClearDebitByBank() {
        super();
        logger = Logger.getLogger(KitRegisterVas.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
//        services = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), logger);
        db = new DbPaymentByBank("dbPayByBank", logger);
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
        for (Record record : listRecord) {
            BankFileDetail bn = (BankFileDetail) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                Get staffId
                long staffId = db.getStaffByRefer(bn.getReference());
                if (staffId <= 0) {
                    logger.warn("Not exist Staff relate to reference " + bn.getReference() + ", id: " + bn.getBankFileDetailId());
                    bn.setResultCode("EC1");
                    bn.setDescription("Not exist Staff relate to reference");
                    continue;
                }
//                Get saleTrans
                long[] saleTrans = db.getSaleTransByStaff(staffId, bn.getValuePay());
                if (saleTrans == null || saleTrans[0] <= 0) {
                    logger.warn("Not exist sale trans relate to staffId " + staffId + " amountTax " + bn.getValuePay() + ", id: " + bn.getBankFileDetailId());
                    bn.setResultCode("EC2");
                    bn.setDescription("Not exist sale trans relate to staffId " + staffId + " amountTax " + bn.getValuePay());
                    continue;
                }
                if (saleTrans[1] > 0) {
                    logger.warn("Already clear by hand staffId " + staffId + " saleTransId " + saleTrans[0]
                            + " amountTax " + bn.getValuePay() + ", id: " + bn.getBankFileDetailId());
                    bn.setResultCode("EC5");
                    bn.setDescription("Already clear by hand staffId " + staffId + " saleTransId " + saleTrans[0]);
                    continue;
                }
//                Clear Debit
                int clearDebit = db.clearDebitBankPOS(staffId, saleTrans[0], bn.getBankFileDetailId());
                if (clearDebit <= 0) {
                    logger.warn("Failt to update clear debit staffId " + staffId + " saleTransId " + saleTrans[0] + ", id: " + bn.getBankFileDetailId());
                    bn.setResultCode("EC3");
                    bn.setDescription("Failt to update clear debit staffId " + staffId + " saleTransId " + saleTrans[0]);
                    continue;
                } else {
                    logger.warn("Success to update clear debit staffId " + staffId + " saleTransId " + saleTrans[0] + ", id: " + bn.getBankFileDetailId());
                    bn.setResultCode("0");
                    bn.setDescription("Success to update clear debit staffId " + staffId + " saleTransId " + saleTrans[0]);
                    continue;
                }
            } else {
                logger.warn("After validate respone code is fail bankFileDetailId " + bn.getID()
                        + " so continue with other transaction");
                bn.setResultCode("EC4");
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
