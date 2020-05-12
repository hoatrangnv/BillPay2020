/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbVipSubWarning;
import com.viettel.paybonus.obj.VipSubDetail;
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
public class VipSubWarningNextProcess extends ProcessRecordAbstract {

    DbVipSubWarning db;
    String msg;

    public VipSubWarningNextProcess() {
        super();
        logger = Logger.getLogger(VipSubWarningNextProcess.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        msg = ResourceBundle.getBundle("configPayBonus").getString("vip_sub_message");
        db = new DbVipSubWarning();
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String[] custInfo;

        String msgWarn;
        for (Record record : listRecord) {
            msgWarn = ResourceBundle.getBundle("configPayBonus").getString("vip_sub_message");
            VipSubDetail bn = (VipSubDetail) record;
            listResult.add(bn);
//            Step 1 get cust_name
            custInfo = db.getCustInfo(bn.getId());
            if (custInfo == null || custInfo[0] == null || custInfo[0].length() == 0 || custInfo[1] == null || custInfo[1].length() == 0) {
                logger.debug("Cannot get customer information...cus name vip sub_id " + bn.getId());
                continue;
            }
//            Step2 send sms
            msgWarn = msgWarn.replace("%CUSNAME%", custInfo[0]);
            db.sendSms(custInfo[1], msgWarn, "86904");
            db.updateWarningVipSubInfo(bn.getId());
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
                append("|\tVIP_SUB_INFO_ID|");
        for (Record record : listRecord) {
            VipSubDetail bn = (VipSubDetail) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId());
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
