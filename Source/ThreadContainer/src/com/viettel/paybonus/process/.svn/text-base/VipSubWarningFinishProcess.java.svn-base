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
public class VipSubWarningFinishProcess extends ProcessRecordAbstract {

    DbVipSubWarning db;
    String msg;
    String isdn;

    public VipSubWarningFinishProcess() {
        super();
        logger = Logger.getLogger(VipSubWarningFinishProcess.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        msg = ResourceBundle.getBundle("configPayBonus").getString("vip_sub_message");
        isdn = ResourceBundle.getBundle("configPayBonus").getString("vip_sub_isdn");
        db = new DbVipSubWarning();
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String custName;
        String msgWarn;
        Integer totalSub;
        Integer totalSuccess;
        Integer totalFail;
        String listFail;
        for (Record record : listRecord) {
            custName = "";
            totalSub = 0;
            totalSuccess = 0;
            totalFail = 0;
            listFail = "";
            msgWarn = ResourceBundle.getBundle("configPayBonus").getString("vip_sub_message_finish");
            VipSubDetail bn = (VipSubDetail) record;
            listResult.add(bn);
//            Step 1 get cust_name
            custName = db.getCustName(bn.getId());
//            Get total sub
            totalSub = db.getTotalSub(bn.getId());
//            Get total success
            totalSuccess = db.getTotalSuccess(bn.getId());
//            Get total fail
            totalFail = db.getTotalFail(bn.getId());
//            Get list fail
            listFail = db.getListFail(bn.getId());
//            Step2 send sms
            msgWarn = msgWarn.replace("%CUSNAME%", custName);
            msgWarn = msgWarn.replace("%TOTALSUB%", totalSub.toString());
            msgWarn = msgWarn.replace("%TOTALSUCCESS%", totalSuccess.toString());
            msgWarn = msgWarn.replace("%TOTALFAIL%", totalFail.toString());
            if (listFail != null && listFail.length() > 0) {
                msgWarn = msgWarn.replace("%LISTFAIL%", listFail.toString().substring(0, listFail.length() - 1));
            } else {
                msgWarn = msgWarn.replace("ListFail:%LISTFAIL%", "");
            }
            String[] listIsdn = isdn.split("\\|");
            for (String i : listIsdn) {
                db.sendSms(i, msgWarn, "86904");
            }
//            Step 3 update after warning
            db.updateWarningFinish(bn.getId());
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
