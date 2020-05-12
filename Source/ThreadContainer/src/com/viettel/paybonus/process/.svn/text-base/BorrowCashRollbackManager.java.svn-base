/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbBorrowCashRollbackProcessor;
import com.viettel.paybonus.obj.EwalletDebitLog;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.util.ArrayList;
import java.util.Random;
import java.util.ResourceBundle;

/**
 *
 * @author dev_linh
 * @version 1.0
 * @since 15-03-2020
 */
public class BorrowCashRollbackManager extends ProcessRecordAbstract {
    
    Exchange pro;
    DbBorrowCashRollbackProcessor db;
    String lstIsdnReceiveError;
    String[] arrIsdnReceiveError;
    
    public BorrowCashRollbackManager() {
        super();
        logger = Logger.getLogger(BorrowCashRollbackManager.class);
    }
    
    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbBorrowCashRollbackProcessor("dbsm", logger);
        lstIsdnReceiveError = ResourceBundle.getBundle("configPayBonus").getString("lstIsdnReceiveError");
        arrIsdnReceiveError = lstIsdnReceiveError.split("\\|");
    }
    
    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }
    
    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        for (Record record : listRecord) {
//            String eWalletResponse = "";
            String eWalletErrCode = "";
            String orgRequestId = "";
            EwalletDebitLog bn = (EwalletDebitLog) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                logger.info("Step 1: Get status of transaction of staff " + bn.getActionUser() + " money " + bn.getAmount()
                        + ", requestId " + bn.getRequestId() + " emola_debit_log_id " + bn.getId());
                String eWalletResponse = pro.getStatusTransactionV2(bn);
                if (eWalletResponse != null && !eWalletResponse.isEmpty()) {
                    logger.info("Result of transaction: " + eWalletResponse + ", emolaDebitLogId: " + bn.getId());
                    String[] arrResponse = eWalletResponse.split("\\|");
                    eWalletErrCode = arrResponse[0];
                    if ("01".equals(eWalletErrCode)) {
                        orgRequestId = arrResponse[1];
                    }
                } else {
                    logger.info("Cannot get limit of staff: " + bn.getActionUser() + ", emolaDebitLogId: " + bn.getId());
                    bn.setDescription("Cannot check status of transaction, emolaDebitLogId: " + bn.getId());
                    continue;
                }
                if ("01".equals(eWalletErrCode)) {
                    logger.info("Transaction successfully of staff " + bn.getActionUser()
                            + ", requestId " + bn.getRequestId() + " emola_debit_log_id " + bn.getId());
                    String shopPath = db.getShopPathByStaffCode(bn.getActionUser());
                    String[] arrShopPath = shopPath.split("\\_");//_7282_shopConfig_shopChildren
                    String shopId = "";
                    if (arrShopPath.length > 2) {
                        shopId = arrShopPath[2];
                    } else {
                        shopId = arrShopPath[1];
                    }
                    Long limitStaff = db.getEmolaDebitLimitStaff(shopId, bn.getActionUser());
                    if (limitStaff == null) {
                        logger.info("Cannot get limit of staff: " + bn.getActionUser() + ", emolaDebitLogId: " + bn.getId());
                        bn.setDescription("Cannot get total cash debit of the staff");
                        continue;
                    }
                    Long debitCurrentAmount = db.getDebitCurrentAmount(bn.getActionUser());
                    if (debitCurrentAmount == null) {
                        logger.info("Cannot get total cash debit of staff: " + bn.getActionUser() + ", emolaDebitLogId: " + bn.getId());
                        bn.setDescription("Cannot get total cash debit of the staff");
                        continue;
                    }
                    String bodUser = db.getBodUserBranch(shopId);
                    String shopCode = db.getShopCode(shopId);
                    if (shopCode == null || shopCode.length() <= 0) {
                        logger.info("Can not find shopCode from shopId, shopId: " + shopId + ", user: " + bn.getActionUser() + ", emolaDebitLogId: " + bn.getId());
                        bn.setDescription("Can not find shopCode from shopId");
                        continue;
                    }
                    Long limitBranch = db.getDebitLimit(bodUser);
                    if (limitBranch == null || limitBranch <= 0) {
                        logger.info("Limit invalid, please define the limit first.");
                        bn.setDescription("Limit invalid, please define the limit first");
                        continue;
                    }
                    Long branchDebit = db.getTotalEmolaDebit(shopId);
                    if (branchDebit + bn.getAmount() > limitBranch) {
                        logger.info("Over limit branch.");
                        bn.setDescription("Over limit branch");
                        continue;
                    }
                    int result = db.checkUserBorrowMoney(bn.getActionUser());
                    if (result == 1) {
                        logger.info("User " + bn.getActionUser() + ", already borrow money before, check agentId and update...");
                        int rsUpdate = db.updateEmolaDebitInfo(bn.getActionUser(), Long.valueOf(bn.getAmount()), 0L, shopId, "");
                        if (rsUpdate != 1) {
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "Update float debit fail, debitUser: " + bn.getActionUser() + ", bn.getAmount(): " + bn.getAmount(), "86904");
                            }
                        }
                    } else if (result == 0) {
                        logger.info("User " + bn.getActionUser() + ", not yet borrow float before, insert new record...");
                        int rsInsert = db.insertEmolaDebitInfo(shopId, bn.getActionUser(), Long.valueOf(bn.getAmount()), 0L, "");
                        if (rsInsert != 1) {
                            for (String isdn : arrIsdnReceiveError) {
                                db.sendSms(isdn, "Insert float debit fail, debitUser: " + bn.getActionUser() + ", bn.getAmount(): " + bn.getAmount(), "86904");
                            }
                        }
                    } else {
                        logger.info("Maybe have exception when check borrow cash, bn.getActionUser(): " + bn.getActionUser());
                        bn.setDescription("Maybe have exception when check borrow cash");
                        continue;
                    }
                    db.insertEmolaDebitLog(shopId, bn.getActionUser().toUpperCase(), 2, Long.valueOf(bn.getAmount()), bn.getActionOtp(), "", "MOVITEL100", limitStaff,
                            bn.getRequest(), "Process auto make log record for transaction time out", "01", "", bn.getRequestId(), orgRequestId,
                            "", "", bn.getAgentMobile(), 0L, "", -1, debitCurrentAmount, bn.getActionUser(), "", "", "");
                    db.updateEmolaDebitOtp(bn.getActionOtp(), bn.getActionUser());
                    Random rand = new Random();
                    StringBuilder transId = new StringBuilder();
                    transId.setLength(0);
                    for (int i = 0; i < 10; i++) {
                        int n = rand.nextInt(10);
                        transId.append(n);
                    }
                    Long debitCurrent = db.getDebitCurrentAmount(bn.getActionUser());
                    String message = "Transaction ID: " + transId + ". Your user already deposits " + bn.getAmount() + " MT for customer " + bn.getAgentMobile() + ". New eMola debit is " + debitCurrent + " MT. Thank you!";
                    String isdnStaff = db.getTelByStaffCode(bn.getActionUser());
                    db.sendSms(isdnStaff, message, "86904");
                    String messageRollback = "[Borrow Cash] - Rollback emolaDebitLogId: " + bn.getId() + " successfully.";
                    for (String tmpIsdn : arrIsdnReceiveError) {
                        db.sendSms(tmpIsdn, messageRollback, "86904");
                    }
                    bn.setDescription("Rollback successully.");
//                    
                } else {
                    logger.info("Transaction fail for staff " + bn.getStaffCode()
                            + ", orgRequestId " + bn.getRequestId() + " ewallet_debig_log_id " + bn.getId());
                    bn.setDescription("Transaction unsuccess. No need make log record and increase debit");
                    continue;
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
        br.setLength(0);
        br.append("\r\n").
                append("|\tID|").
                append("|\tBRANCH\t|").
                append("|\tACTION_USER\t|").
                append("|\tAMOUNT\t|").
                append("|\tACTION_OTP\t|").
                append("|\tEWALLET_REQUEST_ID\t|").
                append("|\tDEBIT_USER\t|");
        for (Record record : listRecord) {
            EwalletDebitLog bn = (EwalletDebitLog) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getBranch()).
                    append("||\t").
                    append(bn.getActionUser()).
                    append("||\t").
                    append(bn.getAmount()).
                    append("||\t").
                    append(bn.getActionOtp()).
                    append("||\t").
                    append(bn.getRequest()).
                    append("||\t").
                    append(bn.getDebitUser());
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
