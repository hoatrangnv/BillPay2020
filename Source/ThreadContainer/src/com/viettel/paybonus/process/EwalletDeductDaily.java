/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbEwalletDeductDaily;
import com.viettel.paybonus.obj.ResponseWallet;
import com.viettel.paybonus.obj.TopupLog;
import com.viettel.paybonus.service.Exchange;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 *
 * @author LinhNBV
 * @version 1.0
 * @since 22-11-2018
 */
public class EwalletDeductDaily extends ProcessRecordAbstract {

    DbEwalletDeductDaily db;
    String isdn;
    String[] listIsdnWarning;
    Exchange exch;
    String smsSuccessDeductDaily;
    String smsFailDeductDaily;
    String wsuser;

    public EwalletDeductDaily() {
        super();
        logger = Logger.getLogger(EwalletDeductDaily.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        isdn = ResourceBundle.getBundle("configPayBonus").getString("isdnWarningDaily");
        listIsdnWarning = isdn.split("\\|");
        db = new DbEwalletDeductDaily();
        exch = new Exchange(ExchangeClientChannel.getInstance("../etc/exchange_client.cfg").getInstanceChannel(), logger);
        smsSuccessDeductDaily = ResourceBundle.getBundle("configPayBonus").getString("smsSuccessDeductDaily");
        smsFailDeductDaily = ResourceBundle.getBundle("configPayBonus").getString("smsFailDeductDaily");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        for (Record record : listRecord) {
            TopupLog bn = (TopupLog) record;
            String deductFund;
            long timeStart = System.currentTimeMillis();
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                deductFund = "";
                double topupFee = bn.getMoneyAfterDiscount();
                Locale locale = Locale.ENGLISH;
                NumberFormat nf = NumberFormat.getNumberInstance(locale);
                nf.setMinimumFractionDigits(2);
                nf.setMaximumFractionDigits(2);
                String money = nf.format(topupFee);
                money = money.replace(",", "");
//                Step 0: Check already process record (LinhNBV 20190501)
                if (db.checkAlreadyProcessRecord(bn.getBillCycleDate())) {
                    logger.warn("Already deduct money for partner " + bn.getClient() + ", no need deduct more.");
                    bn.setDuration(System.currentTimeMillis() - timeStart);
                    bn.setDescription("Already deduct money in day. No need deduct more.");
                    bn.setResultCode("E00");
                    continue;
                }
//                Step 1: Deduct total money.
//                method chargeEmolaEpay for partner UTTM, DING, be careful before call method...
                ResponseWallet responseWallet = exch.chargeEmolaEpay(bn.getRequestId(), bn.getFundCode(), money, bn.getRequestId() + topupFee, bn.getClient(), db, bn.getFundCode());
//                method chargeEmola for partner RechargeAki
//                ResponseWallet responseWallet = exch.chargeEmola(bn.getRequestId(), bn.getFundCode(), topupFee + "", bn.getRequestId() + topupFee, bn.getClient(), this.db, bn.getFundCode());
//                for test
//                ResponseWallet responseWallet = new ResponseWallet();
//                responseWallet.setResponseCode("01");
//                responseWallet.setResponseMessage("Deduct successfully. Test message.");
                if (responseWallet != null) {
                    deductFund = responseWallet.getResponseCode();
                } else {
//                  thanh cong theo cau sql  hoan toan khong con no thi danh dau
                    // danh dau la no do tru that bai
                    db.updateDebitInWsuser("1", db.getAllDebit(), bn.getClient());
                    logger.warn("Fail deduct emoney fundCode " + bn.getFundCode() + " not receive any response from eMola system.");
                    bn.setDuration(System.currentTimeMillis() - timeStart);
                    bn.setDescription("Fail to deduct fund. ResponseWallet is null.");
                    bn.setResultCode("E01");
                    continue;
                }
                if (!"01".equals(deductFund)) {
                    // danh dau la no do tru that bai
                    db.updateDebitInWsuser("1", db.getAllDebit(), bn.getClient());
                    logger.warn("Fail deduct emoney fundCode " + bn.getFundCode() + " response message: " + responseWallet.getResponseMessage());
                    bn.setDuration(System.currentTimeMillis() - timeStart);
                    bn.setDescription("Fail to deduct fund|" + responseWallet.getResponseMessage());
                    bn.setResultCode("E02");
                    for (String tmpIsdn : listIsdnWarning) {
                        db.sendSms(tmpIsdn, smsFailDeductDaily + ". \n Money: " + topupFee + "\nRequestId: "
                                + bn.getRequestId() + " \n Pay_cycle " + bn.getBillCycleDate() + " will be tried after one hour, error code "
                                + responseWallet.getResponseCode() + "\n Response message: " + responseWallet.getResponseMessage(), "86904");
                    }
                    continue;
                } else {
                    logger.info("Deduct successfully emoney fundCode " + bn.getFundCode());
//                  Step 2: Send sms
                    for (String tmpIsdn : listIsdnWarning) {
                        db.sendSms(tmpIsdn, smsSuccessDeductDaily + ". \n Money: " + topupFee + "\nRequestId: "
                                + bn.getRequestId() + " \n Pay_cycle " + bn.getBillCycleDate(), "86904");
                    }
                    bn.setDuration(System.currentTimeMillis() - timeStart);
                    bn.setDescription("Deduct successfully emoney fundCode " + bn.getFundCode());
                    bn.setResultCode("0");
                    //thanh cong theo cau sql hoan toan khong con no thi danh dau
                    String debit = db.getDebitInWsUser(bn.getClient());
                    double afterTopup = Double.parseDouble(debit) - topupFee;
                    if (afterTopup <= 0) {
                        db.clearDebitInWsuser("0", bn.getClient());
                    } else {
                        db.updateDebitInWsuser("1", afterTopup + "", bn.getClient());
                    }
                    continue;
                }
            } else {
                logger.warn("After validate respone code is fail id so continue with other transaction");
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
                append("|\tTOTAL_MONEY|").
                append("|\tMONEY_AFTER_DISCOUNT\t|").
                append("|\tclient\t|").
                append("|\tfund_code\t|").
                append("|\trequest_id\t|").
                append("|\tfrom_topup_log_id\t|").
                append("|\tto_topup_log_id\t|").
                append("|\tbill_cycle\t|").
                append("|\tTOTAL_RECORD\t|");
        for (Record record : listRecord) {
            TopupLog bn = (TopupLog) record;
            br.append("\r\n").
                    append("|\t\t").
                    append(bn.getTotalMoney()).
                    append("||\t\t").
                    append(bn.getMoneyAfterDiscount()).
                    append("||\t\t").
                    append(bn.getClient()).
                    append("||\t\t").
                    append(bn.getFundCode()).
                    append("||\t\t").
                    append(bn.getRequestId()).
                    append("||\t\t").
                    append(bn.getFromTopupLogId()).
                    append("||\t\t").
                    append(bn.getToTopupLogId()).
                    append("||\t\t").
                    append(bn.getBillCycleDate()).
                    append("||\t\t").
                    append(bn.getTotalRecord());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        logger.warn("TEMPLATE process exception record: " + ex.toString());
        for (String tmpIsdn : listIsdnWarning) {
            db.sendSms(tmpIsdn, "Have exception when deduct daily for partner UTTM. Check now...Exception: " + ex.getLocalizedMessage(), "86904");
        }
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
}
