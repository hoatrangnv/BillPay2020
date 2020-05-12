/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbKitVipImport;
import com.viettel.paybonus.obj.KitWarning;
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
public class KitVipImport extends ProcessRecordAbstract {

    DbKitVipImport db;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String lstIsdnReceiveError;
    String[] arrIsdnReceiveError;

    public KitVipImport() {
        super();
        logger = Logger.getLogger(KitVipImport.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbKitVipImport(logger);
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
        ArrayList<KitWarning> listKitWarning = new ArrayList<KitWarning>();
        boolean isProcessed = false;
        int count = 0;
        for (int i = 0; i < listRecord.size(); i++) {
            KitWarning bn = (KitWarning) listRecord.get(i);
            listResult.add(bn);
            if (!isProcessed) {
                if (db.checkAlreadyProcessed(bn.getActionAuditId())) {
                    logger.info("Already import data for to day, importCount: " + bn.getActionAuditId());
                    break;
                } else {
                    isProcessed = true;
                }
            }
            if (db.checkAlreadyImported(bn.getIsdn(), bn.getProductCode(), bn.getEffectDate())) {
                logger.info("Already import subscriber for to day, importCount: " + bn.getActionAuditId()
                        + ", isdn: " + bn.getIsdn() + ", productCode: " + bn.getProductCode() + ", effectDate: " + bn.getEffectDate());
                continue;
            }
            //Insert by batch...
            listKitWarning.add(bn);
            count++;
            if (count > 499) {
                int[] rs = db.insertKitVipWarning(listKitWarning);
                if (rs.length != listKitWarning.size()) {
                    for (String isdn : arrIsdnReceiveError) {
                        db.sendSms(isdn, "Import data kit_vip_warning fail, please check and import again. Import count: " + bn.getActionAuditId(), "86909");
                    }
                }
                logger.info("Insert batch successfully, clear list and continue, importCount: " + bn.getActionAuditId());
                listKitWarning.clear();
                count = 0;
            }
            if (i == (listRecord.size() - 1)) {
                logger.info("End of list, but count not enough, insert total record:" + listKitWarning.size() + ", importCount: " + bn.getActionAuditId());
                int[] rs = db.insertKitVipWarning(listKitWarning);
                if (rs.length != listKitWarning.size()) {
                    for (String isdn : arrIsdnReceiveError) {
                        db.sendSms(isdn, "Import data kit_vip_warning fail, please check and import again. Import count: " + bn.getActionAuditId(), "86909");
                    }
                }
            }
        }
        listKitWarning.clear();
        listRecord.clear();
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tISDN\t|").
                append("|\tproduct_code\t|").
                append("|\teffect_date\t|");
        for (Record record : listRecord) {
            KitWarning bn = (KitWarning) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getProductCode()).
                    append("\t||\t").
                    append(bn.getEffectDate());
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
