/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbDebitStaff;
import com.viettel.paybonus.obj.StaffInfo;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ1
 * @version 1.0
 * @since 24-03-2016
 */
public class DebitOpenManager extends ProcessRecordAbstract {

    DbDebitStaff db;
    String msgToStaff;
    String roleCode;
    String[] listRoleCode;

    public DebitOpenManager() {
        super();
        logger = Logger.getLogger(DebitOpenManager.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        msgToStaff = ResourceBundle.getBundle("configPayBonus").getString("DebitMsgOpenToStaff");
        roleCode = ResourceBundle.getBundle("configPayBonus").getString("DebitListRoleCode");
        listRoleCode = roleCode.split("\\,");
        db = new DbDebitStaff("dbsm", logger);
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        for (Record record : listRecord) {
            StaffInfo bn = (StaffInfo) record;
            listResult.add(bn);
            boolean isLock = false;
            String tel = "";
            logger.info("Start checking debit to open for staff: " + bn.getStaffCode());
//                        Step 1 Check do have sale transaction over time limit
            if (db.checkHaveSaleTransOverTime(bn.getStaffCode(), bn.getStaffId(), bn.getLimitDay())) {
                logger.warn("Staff " + bn.getStaffCode() + " have sale trans over time limit " + bn.getLimitDay());
                isLock = true;
            }
            if (!isLock) {
                if (db.checkHaveEmolaTransOverTime(bn.getStaffCode(), bn.getStaffId(), bn.getLimitDay())) {
                    logger.warn("Staff " + bn.getStaffCode() + " have Emola Float trans over time limit " + bn.getLimitDay());
                    isLock = true;
                }
            }
            if (!isLock) {
                if (db.checkHavePaymentTransOverTime(bn.getStaffCode(), bn.getStaffId(), bn.getLimitDay())) {
                    logger.warn("Staff " + bn.getStaffCode() + " have payment trans over time limit " + bn.getLimitDay());
                    isLock = true;
                }
            }
            if (!isLock) {
                if (db.checkHaveDepositTransOverTime(bn.getStaffCode(), bn.getStaffId(), bn.getLimitDay())) {
                    logger.warn("Staff " + bn.getStaffCode() + " have deposit trans over time limit " + bn.getLimitDay());
                    isLock = true;
                }
            }
            //            Step 2 Check do have sum transaction over money limit
            if (!isLock) {
                double debitSale = db.getMoneySaleTrans(bn.getStaffCode(), bn.getStaffId());
                double debitEmola = db.getMoneyEmola(bn.getStaffCode(), bn.getStaffId());
                double debitPayment = db.getMoneyPaymentTrans(bn.getStaffCode(), bn.getStaffId());
                double debitDeposit = db.getMoneyDepositTrans(bn.getStaffCode(), bn.getStaffId());
                double debitTotal = debitSale + debitPayment + debitDeposit + debitEmola;
                double limitMoney = Double.valueOf(bn.getLimitMoney());
                logger.info("Staff " + bn.getStaffCode() + " debitSale " + debitSale + " debitEmola " + debitEmola
                        + " debitPayment " + debitPayment
                        + " debitDeposit " + debitDeposit + " debitTotal " + debitTotal + " limitMoney " + limitMoney);
                if (debitTotal >= limitMoney) {
                    logger.warn("Staff " + bn.getStaffCode() + " have total current money over momey limit " + bn.getLimitMoney());
                    isLock = true;
                }
            }
            //            Step 3 Lock and send warning message and save log if isLock = true
            if (!isLock) {
                //                Lock all roles
                for (String role : listRoleCode) {
                    logger.info("Start open role " + role + " for staff " + bn.getStaffCode());
                    db.openRoleUser(bn.getStaffCode(), role);
                }
                //                Send sms to staff
                tel = db.getTelByStaffCode(bn.getStaffCode());
                if (tel != null && tel.trim().length() > 0) {
                    db.sendSms(tel, msgToStaff, "86142");
                } else {
                    logger.warn("Don't have cellphone to send notify sms " + bn.getStaffCode() + " tel " + tel);
                }
                //                Save log
                logger.info("Start update STAFF after opening " + bn.getStaffCode());
                db.updateOpenStaff(bn.getStaffCode(), bn.getStaffId());
                logger.info("Start insert ActionLog after opening " + bn.getStaffCode());
                db.saveActionLog(bn.getStaffCode(), bn.getStaffId(), "Open roles: " + roleCode, "DEBIT_OPEN");
            } else {
                logger.info("Don't open because there're still debit transactions " + bn.getStaffCode());
            }
        }
        listRecord.clear();
        Thread.sleep(120000); //sleep 2 minute after process all staffs
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tSTAFF_ID|").
                append("|\tSTAFF_CODE\t|").
                append("|\tSHOP_ID\t|").
                append("|\tLIMIT_DAY\t|").
                append("|\tLIMIT_MONEY\t|").
                append("|\tLIMIT_CREATE_USER\t|").
                append("|\tLIMIT_APPROVE_USER\t|").
                append("|\tLIMIT_OVER_STATUS\t|").
                append("|\tLIMIT_OVER_LAST_TIME\t|");
        for (Record record : listRecord) {
            StaffInfo bn = (StaffInfo) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getStaffId()).
                    append("|\t").
                    append(bn.getStaffCode()).
                    append("|\t").
                    append(bn.getShopId()).
                    append("|\t").
                    append(bn.getLimitDay()).
                    append("|\t").
                    append(bn.getLimitMoney()).
                    append("|\t").
                    append(bn.getLimitCreateUser()).
                    append("|\t").
                    append(bn.getLimitApproveUser()).
                    append("|\t").
                    append((bn.getLimitOverLastTime() != null ? sdf.format(bn.getLimitOverLastTime()) : null));
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
