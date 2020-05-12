/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbLimitControl;
import com.viettel.paybonus.obj.Staff;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class LimitControl extends ProcessRecordAbstract {

    DbLimitControl db;
    String limitControlIsdnWarning;
    String[] arrLimitControlIsdnWarning;

    public LimitControl() {
        super();
        logger = Logger.getLogger(LimitControl.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbLimitControl("dbsm", logger);
        limitControlIsdnWarning = ResourceBundle.getBundle("configPayBonus").getString("limitControlIsdnWarning");
        arrLimitControlIsdnWarning = limitControlIsdnWarning.split("\\|");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        for (Record record : listRecord) {
            Staff bn = (Staff) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                logger.info("Step 1: Rollback limit for staff: " + bn.getStaffCode() + ", oldLimitMoney: "
                        + bn.getOldLimitMoney() + ", oldLimitDay: " + bn.getOldLimitDay() + ", currentLimitDay: "
                        + bn.getLimitDay() + ", currentLimitMoney: " + bn.getLimitMoney());
                int result = db.rollbackLimitStaff(bn.getStaffId(), bn.getStaffCode());
                if (result != 1) {
                    for (String isdn : arrLimitControlIsdnWarning) {
                        db.sendSms(isdn, "Rollback limit for staff: " + bn.getStaffCode() + " is fail. Check and rollback by hand.", "86904");
                    }
                    continue;
                }
                logger.info("Step 2: Rollback success, now save log...");
                Long actionLogId = db.getSequence("ACTION_LOG_SEQ", "dbsm");
                db.insertActionLog(actionLogId, bn.getStaffCode(), bn.getStaffId());
                
                db.insertActionLogDetail(actionLogId, "STAFF", "LIMIT_DAY", bn.getLimitDay(), bn.getOldLimitDay(), bn.getStaffId());
                db.insertActionLogDetail(actionLogId, "STAFF", "LIMIT_MONEY", bn.getLimitMoney(), bn.getOldLimitMoney(), bn.getStaffId());

                for (String isdn : arrLimitControlIsdnWarning) {
                    db.sendSms(isdn, "Rollback limit for staff: " + bn.getStaffCode() + " is success.", "86904");
                }
            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getStaffId()
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
                append("|\tSTAFF_CODE\t|").
                append("|\tLIMIT_END_TIME\t|").
                append("|\tLIMIT_MONEY\t|").
                append("|\tLIMIT_DAY\t|").
                append("|\tOLD_LIMIT_MONEY\t|").
                append("|\t\t|");
        for (Record record : listRecord) {
            Staff bn = (Staff) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getStaffId()).
                    append("||\t").
                    append(bn.getStaffCode()).
                    append("||\t").
                    append((bn.getLimitEndTime() != null ? sdf.format(bn.getLimitEndTime()) : null)).
                    append("||\t").
                    append(bn.getLimitMoney()).
                    append("||\t").
                    append(bn.getLimitDay()).
                    append("||\t").
                    append(bn.getOldLimitMoney()).
                    append("||\t").
                    append(bn.getOldLimitDay());
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
