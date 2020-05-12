/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbDebitRollback;
import com.viettel.paybonus.database.DbSaleFloatRollback;
import com.viettel.paybonus.obj.EwalletDebitLog;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class SaleFloatRollbackManager extends ProcessRecordAbstract {

    Exchange pro;
    DbSaleFloatRollback db;

    public SaleFloatRollbackManager() {
        super();
        logger = Logger.getLogger(SaleFloatRollbackManager.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbSaleFloatRollback("dbsm", logger);
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        for (Record record : listRecord) {
            String eWalletResponse = "";
            EwalletDebitLog bn = (EwalletDebitLog) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                logger.info("Step 1: Get status of transaction for staff " + bn.getStaffCode() + " money " + bn.getMoney()
                        + ", orgRequestId " + bn.getRequestId() + " ewallet_sale_float_id " + bn.getId());
                eWalletResponse = pro.getStatusTransaction(bn, new DbDebitRollback());
                if ("01".equals(eWalletResponse)) {
                    logger.info("Transaction successfully for staff " + bn.getStaffCode()
                            + ", orgRequestId " + bn.getRequestId() + " ewallet_sale_float_id " + bn.getId());
//                    Step 3: update status ewallet_sale_float
                    db.updateEwalletSaleFloatLog(bn.getStaffCode(), bn.getId(), bn.getRequestId(), "Transaction success. No need to cancel transaction.");
//                    Step 3.1: Send sms
                    String phone = db.getTelByStaffCode(bn.getStaffCode());
                    db.sendSms("258" + phone, "Venda de float para o  Agente " + bn.getListSaleTrans() + ", montante " + bn.getOtp() + " MT com sucesso. Obrigado", "86142");
                } else {
                    logger.info("Transaction fail for staff " + bn.getStaffCode()
                            + ", orgRequestId " + bn.getRequestId() + " ewallet_sale_float_id " + bn.getId());
                    db.updateEwalletSaleFloatLog(bn.getStaffCode(), bn.getId(), bn.getRequestId(), "Transaction not success. eWalletCode: " + eWalletResponse + ". Process will be cancel transaction.");
//                    Cancel transaction...
                    db.cancelSaleTrans(bn.getEwalletDebitLogId(), "PROCESS_DEBIT", "Transaction sale float not success. Process will be cancel transaction for user.");
                    String phone = db.getTelByStaffCode(bn.getStaffCode());
                    db.sendSms("258" + phone, "Venda de float para o  Agente " + bn.getListSaleTrans() + ", montante " + bn.getOtp() + " MT sem sucesso. Obrigado", "86142");
                }
            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getId()
                        + " so continue with other transaction");
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
                append("|\tID|").
                append("|\tREQUEST_ID\t|").
                append("|\tLOG_TIME\t|").
                append("|\tSTAFF_CODE\t|").
                append("|\tSALE_TRANS_ID\t|");
        for (Record record : listRecord) {
            EwalletDebitLog bn = (EwalletDebitLog) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getRequestId()).
                    append("||\t").
                    append((bn.getLogTime() != null ? sdf.format(bn.getLogTime()) : null)).
                    append("||\t").
                    append(bn.getStaffCode()).
                    append("||\t").
                    append(bn.getEwalletDebitLogId());
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
}
