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
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ1
 * @version 1.0
 * @since 24-03-2016
 */
public class DebitBlockManager extends ProcessRecordAbstract {

    DbDebitStaff db;
    String msgToStaff;
    String msgToManager;
    String roleCode;
    String[] listRoleCode;
    String[] listDefaultManagerTel;
    String defaultManagerTel;
    String debitLimitDayForSpecialTrans;
    String[] listDebitLimitDayForSpecialTrans;
    ArrayList<HashMap> lstMapSpecialStaff;

    public DebitBlockManager() {
        super();
        logger = Logger.getLogger(DebitBlockManager.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        msgToStaff = ResourceBundle.getBundle("configPayBonus").getString("DebitMsgToStaff");
        msgToManager = ResourceBundle.getBundle("configPayBonus").getString("DebitMsgToManager");
        roleCode = ResourceBundle.getBundle("configPayBonus").getString("DebitListRoleCode");
        listRoleCode = roleCode.split("\\,");
        defaultManagerTel = ResourceBundle.getBundle("configPayBonus").getString("DebitDefaultManagerTel");
        listDefaultManagerTel = defaultManagerTel.split("\\|");
        debitLimitDayForSpecialTrans = ResourceBundle.getBundle("configPayBonus").getString("DebitLimitDayForSpecialTrans");
        listDebitLimitDayForSpecialTrans = debitLimitDayForSpecialTrans.split("\\|");
        lstMapSpecialStaff = new ArrayList<HashMap>();
        for (String staff : listDebitLimitDayForSpecialTrans) {
            String[] tmp = staff.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapSpecialStaff.add(map);
        }
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
            int limitDayForSpecialUser = 0;
            String tel = "";
            String branchId = "";
            ArrayList<String> listManager = new ArrayList<String>();
            String actionType = "";
//            Step 1: Check special user (special trans)
            for (int i = 0; i < lstMapSpecialStaff.size(); i++) {
                if (lstMapSpecialStaff.get(i).containsKey(bn.getStaffCode().trim().toUpperCase())) {
                    limitDayForSpecialUser = Integer.valueOf(lstMapSpecialStaff.get(i).get(bn.getStaffCode().trim().toUpperCase()).toString());
                    break;
                }
            }
            if (limitDayForSpecialUser > 0) {
                logger.info("Staff " + bn.getStaffCode() + " have special trans, so only check to warn about limitDay " + limitDayForSpecialUser);
                if (db.checkHaveSaleTransOverTime(bn.getStaffCode(), bn.getStaffId(), limitDayForSpecialUser + "")) {
                    logger.warn("Staff " + bn.getStaffCode() + " have sale trans over time limit " + bn.getLimitDay());
                    isLock = true;
                    actionType = "WARN_DEBIT_SALE_TRANS";
                } else if (db.checkHavePaymentTransOverTime(bn.getStaffCode(), bn.getStaffId(), limitDayForSpecialUser + "")) {
                    logger.warn("Staff " + bn.getStaffCode() + " have payment trans over time limit " + bn.getLimitDay());
                    isLock = true;
                    actionType = "WARN_DEBIT_PAYMENT_TRANS";
                } else if (db.checkHaveDepositTransOverTime(bn.getStaffCode(), bn.getStaffId(), limitDayForSpecialUser + "")) {
                    logger.warn("Staff " + bn.getStaffCode() + " have deposit trans over time limit " + bn.getLimitDay());
                    isLock = true;
                    actionType = "WARN_DEBIT_DEPOSIT_TRANS";
                }
                if (isLock) {
                    //                Send sms to staff
                    tel = db.getTelByStaffCode(bn.getStaffCode());
                    if (tel != null && tel.trim().length() > 0
                            && !db.checkAlreadyWarningInDay(tel, msgToStaff) && !db.checkAlreadyWarningInDayQueue(tel, msgToStaff)) {
                        db.sendSms(tel, msgToStaff, "86142");
//                    Send sms to manager
                        branchId = db.getBranchId(bn.getStaffCode(), bn.getShopId());
                        if (branchId != null && branchId.trim().length() > 0 && !branchId.equals("7282")) {
                            listManager = db.getListBodFinanceOfBranch(bn.getStaffCode(), Long.valueOf(branchId));
                            if (listManager != null && listManager.size() > 0) {
                                for (int i = 0; i < listManager.size(); i++) {
                                    String telOwner = db.getTelByStaffCode(listManager.get(i));
                                    if (telOwner != null && telOwner.trim().length() > 0) {
                                        logger.info("Start send warning message to manager " + listManager.get(i)
                                                + " of staff " + bn.getStaffCode());
                                        db.sendSms(telOwner, msgToManager.replace("%STAFF_CODE%", bn.getStaffCode()), "86142");
                                    }
                                }
                            } else {
                                logger.warn("Do not have any manager of staff " + bn.getStaffCode() + " for warning, so send to defaul manager"
                                        + defaultManagerTel);
                                for (String defaul : listDefaultManagerTel) {
                                    db.sendSms(defaul, msgToManager.replace("%STAFF_CODE%", bn.getStaffCode()), "86142");
                                }
                            }
                        } else {
                            logger.warn("Staff belong directly company so send to default manager " + defaultManagerTel
                                    + " of staff " + bn.getStaffCode());
                            for (String defaul : listDefaultManagerTel) {
                                db.sendSms(defaul, msgToManager.replace("%STAFF_CODE%", bn.getStaffCode()), "86142");
                            }
                        }
                        //                Save log
                        logger.info("Start insert ActionLog after waring special user " + bn.getStaffCode());
                        db.saveActionLog(bn.getStaffCode(), bn.getStaffId(), "Warning special user limitDay " + limitDayForSpecialUser,
                                actionType);
                    } else {
                        logger.warn("Don't have cellphone or already warned for waring special user " + bn.getStaffCode() + " tel " + tel);
                    }
                } else {
                    logger.info("Don't have any trans over limit for special staff " + bn.getStaffCode());
                }
            } else {
                logger.info("Staff " + bn.getStaffCode() + " not special trans");
                //            Step 1 Check do have sale transaction over time limit
                if (db.checkHaveSaleTransOverTime(bn.getStaffCode(), bn.getStaffId(), bn.getLimitDay())) {
                    logger.warn("Staff " + bn.getStaffCode() + " have sale trans over time limit " + bn.getLimitDay());
                    isLock = true;
                    actionType = "LOCK_DEBIT_SALE_TRANS";
                }
                if (!isLock) {
                    if (db.checkHaveEmolaTransOverTime(bn.getStaffCode(), bn.getStaffId(), bn.getLimitDay())) {
                        logger.warn("Staff " + bn.getStaffCode() + " have Emola Float trans over time limit " + bn.getLimitDay());
                        isLock = true;
                        actionType = "LOCK_DEBIT_EMOLA_TRANS";
                    }
                }
                if (!isLock) {
                    if (db.checkHavePaymentTransOverTime(bn.getStaffCode(), bn.getStaffId(), bn.getLimitDay())) {
                        logger.warn("Staff " + bn.getStaffCode() + " have payment trans over time limit " + bn.getLimitDay());
                        isLock = true;
                        actionType = "LOCK_DEBIT_PAYMENT_TRANS";
                    }
                }
                if (!isLock) {
                    if (db.checkHaveDepositTransOverTime(bn.getStaffCode(), bn.getStaffId(), bn.getLimitDay())) {
                        logger.warn("Staff " + bn.getStaffCode() + " have deposit trans over time limit " + bn.getLimitDay());
                        isLock = true;
                        actionType = "LOCK_DEBIT_DEPOSIT_TRANS";
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
                        actionType = "LOCK_DEBIT_OVER_TOTAL";
                    }
                }
                //            Step 3 Lock and send warning message and save log if isLock = true
                if (isLock) {
                    //                Lock all roles
                    for (String role : listRoleCode) {
                        logger.info("Start lock role " + role + " for staff " + bn.getStaffCode());
                        db.lockRoleUser(bn.getStaffCode(), role);
                    }
                    //                Send sms to staff
                    tel = db.getTelByStaffCode(bn.getStaffCode());
                    if (tel != null && tel.trim().length() > 0
                            && !db.checkAlreadyWarningInDay(tel, msgToStaff) && !db.checkAlreadyWarningInDayQueue(tel, msgToStaff)) {
                        db.sendSms(tel, msgToStaff, "86142");
                        //                    Send sms to manager
                        branchId = db.getBranchId(bn.getStaffCode(), bn.getShopId());
                        if (branchId != null && branchId.trim().length() > 0 && !branchId.equals("7282")) {
                            listManager = db.getListBodFinanceOfBranch(bn.getStaffCode(), Long.valueOf(branchId));
                            if (listManager != null && listManager.size() > 0) {
                                for (int i = 0; i < listManager.size(); i++) {
                                    String telOwner = db.getTelByStaffCode(listManager.get(i));
                                    if (telOwner != null && telOwner.trim().length() > 0) {
                                        logger.info("Start send warning message to manager " + listManager.get(i)
                                                + " of staff " + bn.getStaffCode());
                                        db.sendSms(telOwner, msgToManager.replace("%STAFF_CODE%", bn.getStaffCode()), "86142");
                                    }
                                }
                            } else {
                                logger.warn("Do not have any manager of staff " + bn.getStaffCode() + " for warning, so send to defaul manager"
                                        + defaultManagerTel);
                                for (String defaul : listDefaultManagerTel) {
                                    db.sendSms(defaul, msgToManager.replace("%STAFF_CODE%", bn.getStaffCode()), "86142");
                                }
                            }
                        } else {
                            logger.warn("Staff belong directly company so send to default manager " + defaultManagerTel
                                    + " of staff " + bn.getStaffCode());
                            for (String defaul : listDefaultManagerTel) {
                                db.sendSms(defaul, msgToManager.replace("%STAFF_CODE%", bn.getStaffCode()), "86142");
                            }
                        }
                    } else {
                        logger.warn("Don't have cellphone or already warned user " + bn.getStaffCode() + " tel " + tel);
                    }
                    //                Save log
                    logger.info("Start update STAFF after locking " + bn.getStaffCode());
                    db.updateLockStaff(bn.getStaffCode(), bn.getStaffId());
                    logger.info("Start insert ActionLog after locking " + bn.getStaffCode());
                    db.saveActionLog(bn.getStaffCode(), bn.getStaffId(), "Lock roles: " + roleCode, actionType);
                } else {
                    logger.info("Don't have any trans over limit staff " + bn.getStaffCode());
                }
            }
        }
        listRecord.clear();
        Thread.sleep(60000); //sleep 1 minute after process all staffs
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
