/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbFixBroadband;
import com.viettel.paybonus.obj.SubAdslLLPrepaid;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.ResourceBundle;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class FixBroadbandWarnThread extends ProcessRecordAbstract {

    DbFixBroadband db;
    String msg;
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

    public FixBroadbandWarnThread() {
        super();
        logger = Logger.getLogger(FixBroadbandWarnThread.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        msg = ResourceBundle.getBundle("configPayBonus").getString("fbb_msg_warn_expire");
        db = new DbFixBroadband("cm_pos", logger);
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String isdnContract;
        String ref;
        String msgWarn;
        for (Record record : listRecord) {
            isdnContract = "";
            ref = "";
            SubAdslLLPrepaid bn = (SubAdslLLPrepaid) record;
            listResult.add(bn);
            msgWarn = msg.replace("%ACCOUNT%", bn.getAccount());
            Date current = new Date();
            long diff = bn.getExpireTime().getTime() - current.getTime();
            long remainDay = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
            msgWarn = msgWarn.replace("%DAY%", remainDay + "");

            msgWarn = msgWarn.replace("%EXPIRE%", sdf.format(bn.getExpireTime()));
            ref = db.getRefOfContract(bn.getContractId(), bn.getAccount());
            if (ref == null || ref.trim().length() <= 0) {
                ref = genReferenceId(bn.getContractId() + "");
                if (ref == null || ref.trim().length() <= 0) {
                    db.updateContract(bn.getContractId(), ref);
                    msgWarn = msgWarn.replace("%REF%", ref);
                }
            } else {
                msgWarn = msgWarn.replace("%REF%", ref);
            }
//            Step 1 get phone of Contract
            isdnContract = db.getPhoneOfContract(bn.getContractId(), bn.getAccount());
//            Step2 send sms using 866123123 to send vodacom, mcell            
            db.sendSms(isdnContract, msgWarn, "866123123", bn.getId());
//            Step 3 update after warning
            db.updateWarning(bn.getId(), bn.getAccount());
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
                append("|\tSUB_ADSL_LL_PREPAID_ID|").
                append("|\tSUB_ID|").
                append("|\tCONTRACT_ID|").
                append("|\tACCOUNT\t|").
                append("|\tNEW_PRODUCT_CODE\t|").
                append("|\tPREPAID_TYPE\t|").
                append("|\tCREATE_SHOP\t|").
                append("|\tCREATE_USER\t|").
                append("|\tCREATE_TIME\t|").
                append("|\tEXPIRE_TIME\t|").
                append("|\tWARNING_COUNT\t|");
        for (Record record : listRecord) {
            SubAdslLLPrepaid bn = (SubAdslLLPrepaid) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getSubAdslLlPrepaidId()).
                    append("||\t").
                    append(bn.getSubId()).
                    append("||\t").
                    append(bn.getContractId()).
                    append("||\t").
                    append(bn.getAccount()).
                    append("||\t").
                    append(bn.getNewProductCode()).
                    append("||\t").
                    append(bn.getPrepaidType()).
                    append("||\t").
                    append(bn.getCreateShop()).
                    append("||\t").
                    append(bn.getCreateUser()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
                    append("||\t").
                    append((bn.getExpireTime() != null ? sdf.format(bn.getExpireTime()) : null)).
                    append("||\t").
                    append(bn.getWarningCount());
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

    @Override
    public boolean startProcessRecord() {
        return true;
    }

    public static void main(String[] args) {
        SimpleDateFormat myFormat = new SimpleDateFormat("dd MM yyyy");
        String inputString1 = "05 03 2018";
        String inputString2 = "10 03 2018";

        try {
            Date date1 = myFormat.parse(inputString1);
            Date date2 = myFormat.parse(inputString2);
            long diff = date2.getTime() - date1.getTime();
            System.out.println("Days: " + TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public String genReferenceId(String contractId) throws Exception {
        StringBuffer digitos = new StringBuffer();
        String fullContract = contractId;
        String referenceId;
        if (contractId == null || contractId.trim().length() <= 0) {
            logger.warn("ContractId is null or empty");
            return "";
        }
        if (contractId.length() > 7) {
            logger.warn("ContractId is to long over 7 character " + contractId);
            return "";
        }
        if (contractId.length() < 7) {
            for (int i = 0; i < 7 - contractId.length(); i++) {
                fullContract = "0" + fullContract;
            }
            logger.warn("ContractId is to shorter 7 character, the value after modify " + fullContract);
        }
        fullContract = fullContract + "08"; //Fix using month 08
        digitos.append("86871"); //Fix entidade for Movitel Payment System
        digitos.append(fullContract);
        digitos.append("");//Fix amount is empty
        int s = 0;
        int p = 0;
        for (int i = 0; i < digitos.length(); i++) {
            s = Integer.parseInt(String.valueOf(digitos.charAt(i))) + p;
            p = s * 10 % 97;
        }
        p = p * 10 % 97;
        int checkDigitoCalculado = 98 - p;
        if (checkDigitoCalculado < 10) {
            referenceId = fullContract + "0" + checkDigitoCalculado;
        } else {
            referenceId = fullContract + "" + checkDigitoCalculado;
        }
        logger.info("ReferenceId : " + referenceId + " contractId " + contractId);
        return referenceId;
    }
}
