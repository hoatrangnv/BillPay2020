/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbDebitRollback;
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
public class DebitRollbackManager extends ProcessRecordAbstract {

    Exchange pro;
    DbDebitRollback db;

    public DebitRollbackManager() {
        super();
        logger = Logger.getLogger(DebitRollbackManager.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbDebitRollback("dbsm", logger);
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
                        + ", orgRequestId " + bn.getRequestId() + " ewallet_debig_log_id " + bn.getId());
                eWalletResponse = pro.getStatusTransaction(bn, db);
                if ("01".equals(eWalletResponse)) {
                    logger.info("Transaction successfully for staff " + bn.getStaffCode()
                            + ", orgRequestId " + bn.getRequestId() + " ewallet_debig_log_id " + bn.getId());
                    String transTypeCode = "";
//                    Step 3: Check transaction is SaleTrans or Deposit or Payment
//                    Step 3.1: Get list_sale_trans and spilit get first element check in 3 tables: sale_trans, deposit, payment_contract
                    String lstSaleTrans = bn.getListSaleTrans();
                    String[] arrTrans = lstSaleTrans.split("\\|");
                    String tmpTrans = arrTrans[0];
                    if (bn.getClearDebitType() != 9) {
                        transTypeCode = db.checkTransactions(Long.valueOf(tmpTrans));
                    } else {
                        logger.info("Is Sale Float Emola " + bn.getStaffCode());
                        transTypeCode = "04";
                    }
                    List<Long> saleTransIds = new ArrayList<Long>();
                    for (String saleTransId : arrTrans) {
                        saleTransIds.add(Long.valueOf(saleTransId));
                    }
//                    Check transType and update clear debit status...
                    if (!transTypeCode.isEmpty()) {
                        db.updateSaleTransClearDebit(bn.getStaffCode(), bn.getRequestId(), saleTransIds, transTypeCode);
//                    Step 2: Update Ewallet Debit Log >>> Timeout >>> Success
                        db.updateEwalletDebitLog(bn.getStaffCode(), bn.getId(), bn.getRequestId(), "PROCESS UPDATE CLEAR DEBIT STATUS SUCCESS");
                    }

//                    
                } else {
                    logger.info("Transaction fail for staff " + bn.getStaffCode()
                            + ", orgRequestId " + bn.getRequestId() + " ewallet_debig_log_id " + bn.getId());
                    db.updateEwalletDebitLog(bn.getStaffCode(), bn.getId(), bn.getRequestId(), "TRANSACTIONS NOT SUCCESS. EWALLET RESPONSE CODE: " + eWalletResponse);
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
                append("|\tISDN\t|").
                append("|\tRECEIVE_DATE\t|").
                append("|\tMONEY\t|").
                append("|\tORG_REQUEST_ID\t|").
                append("|\tCLEAR_DEBIT_TYPE\t|").
                append("|\tSTAFF_CODE\t|");
        for (Record record : listRecord) {
            EwalletDebitLog bn = (EwalletDebitLog) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append((bn.getLogTime() != null ? sdf.format(bn.getLogTime()) : null)).
                    append("||\t").
                    append(bn.getMoney()).
                    append("||\t").
                    append(bn.getRequestId()).
                    append("||\t").
                    append(bn.getClearDebitType()).
                    append("||\t").
                    append(bn.getStaffCode());
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
