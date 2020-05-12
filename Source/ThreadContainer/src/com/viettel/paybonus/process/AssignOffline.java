/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbCmpreProcessor;
import com.viettel.paybonus.database.DbSubProfileProcessor;
import com.viettel.paybonus.obj.Bonus;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Random;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class AssignOffline extends ProcessRecordAbstract {

    Exchange pro;
    DbCmpreProcessor db;
    String countryCode;
    private Long sleepTime = 10 * 60 * 1000L;
    private Long sleepAssign = 10 * 1000L;

    public AssignOffline() {
        super();
        logger = Logger.getLogger(AssignOffline.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbCmpreProcessor();
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
//        Step1: check online staff
        List<String> check = db.getUserOnline();
        List<Record> listResult = new ArrayList<Record>();
//        Step2: if have online staff => assign profile => continue
        if (!check.isEmpty()) {
            for (Record record : listRecord) {
                List<String> checkAgain = db.getUserOnline();
                Bonus bn = (Bonus) record;
                listResult.add(bn);
                if ("0".equals(bn.getResultCode()) && !checkAgain.isEmpty()) {
                    logger.warn("-------------- Start assign profile to staff with sub_profile_id = " + bn.getID() + "-----------");
                    if (checkAgain.size() <= 5) {
                        Random rand = new Random();
                        int random = rand.nextInt(checkAgain.size());
                        db.updateAssign(checkAgain.get(random), Integer.parseInt(bn.getID()));
                        Thread.sleep(sleepAssign);
                        continue;
                    } else {
                        Random rand = new Random();
                        int random = rand.nextInt(checkAgain.size());
                        db.updateAssign(checkAgain.get(random), Integer.parseInt(bn.getID()));
                        continue;
                    }

                } else {
                    logger.warn("Error assign profile to staff with sub_profile_id = " + bn.getID());
                    continue;
                }
            }            
        } //        Step3: incase online staff => Sleep 10 minute => Step1
        else {
            logger.info("Sleeping " + sleepTime);
            Thread.sleep(sleepTime);
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
                append("|\tBONUS_STATUS\t|").
                append("|\tREASON_ID\t|").
                append("|\tBLOCK_OCS_HLR\t|");
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
                    append(bn.getBonusStatus()).
                    append("||\t").
                    append(bn.getReasonId()).
                    append("||\t").
                    append(bn.getBlockOcsHlr());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
}
