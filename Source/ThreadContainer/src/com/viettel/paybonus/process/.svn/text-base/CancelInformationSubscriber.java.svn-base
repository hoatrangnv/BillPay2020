/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbCancelInformationSub;
import com.viettel.paybonus.obj.Bonus;
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
public class CancelInformationSubscriber extends ProcessRecordAbstract {

    Exchange pro;
    DbCancelInformationSub db;

    public CancelInformationSubscriber() {
        super();
        logger = Logger.getLogger(CancelInformationSubscriber.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbCancelInformationSub();

    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        for (Record record : listRecord) {
            Bonus bn = (Bonus) record;
            if ("0".equals(bn.getResultCode())) {
                boolean isActiveIsdn = pro.checkActiveIsdn("258" + bn.getIsdnCustomer());
                //Step 1 - Update custId into field delete_cust_id on table: sub_profile_info
                int updateCustId = db.updateCustIdSubProfileInfo(bn.getIsdnCustomer(), String.valueOf(bn.getPkId()));
                if (updateCustId != 0) {
                    if (!isActiveIsdn) {
                        //Step 2 - Delete custId in subMb --> register information again.  
                        int clearCustId = db.clearCustIdWhenNotActive(bn.getIsdnCustomer());
                        if (clearCustId != 0) {
                            //Step 3 - Save logs into action_audit & action_detail
                            long actionAuditId = db.getActionAuditIdSeq();
                            int actionAudit = db.insertActionAudit(bn, actionAuditId);
                            if (actionAudit != 0) {
                                db.insertActionDetail(bn, actionAuditId);
                            } else {
                                //rollback action audit
                                db.updateCustIdSubProfileInfo(bn.getIsdnCustomer(), "");
                                continue;
                            }
                        } else {
                            //update custId = null <rollback>
                            db.updateCustIdSubProfileInfo(bn.getIsdnCustomer(), "");
                            continue;
                        }
                    }
                } else {
                    logger.warn("update field deleteCustId in SubProfileInfo is fail custId " + bn.getPkId()
                            + " so continue with other transaction");

                    continue;
                }
            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getActionAuditId()
                        + " id " + bn.getId() + " isdn: " + bn.getIsdnCustomer()
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
                append("|\tACTION_AUDIT_ID|").
                append("|\tPK_ID|").
                append("|\tCREATE_STAFF\t|").
                append("|\tCREATE_SHOP\t|").
                append("|\tCREATE_TIME\t|").
                append("|\tCHECK_STAFF\t|").
                append("|\tCHECK_INFO\t|").
                append("|\tCHECK_TIME\t|").
                append("|\tISDN\t|").
                append("|\tBONUS_STATUS\t|");
        for (Record record : listRecord) {
            Bonus bn = (Bonus) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getActionAuditId()).
                    append("||\t").
                    append(bn.getPkId()).
                    append("||\t").
                    append(bn.getUserName()).
                    append("||\t").
                    append(bn.getShopCode()).
                    append("||\t").
                    append((bn.getIssueDateTime() != null ? sdf.format(bn.getIssueDateTime()) : null)).
                    append("||\t").
                    append(bn.getStaffCheck()).
                    append("||\t").
                    append(bn.getCheckInfo()).
                    append("||\t").
                    append((bn.getTimeCheck() != null ? sdf.format(bn.getTimeCheck()) : null)).
                    append("||\t").
                    append(bn.getIsdnCustomer()).
                    append("||\t").
                    append(bn.getBonusStatus());
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
