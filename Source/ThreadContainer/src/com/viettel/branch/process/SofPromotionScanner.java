/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.branch.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbSofFloodInput;
import com.viettel.paybonus.obj.SofFloodInput;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.ResourceBundle;

/**
 *
 * @author LamNT
 * @version 1.0
 * @since 05-01-2019
 */
public class SofPromotionScanner extends ProcessRecordAbstract {

    Exchange pro;
    DbSofFloodInput db;
    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
    Calendar cal = Calendar.getInstance();
    Calendar calNow = Calendar.getInstance();
    String priceIdAddPolicySofala;

    public SofPromotionScanner() {
        super();
        logger = Logger.getLogger(SofPromotionScanner.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbSofFloodInput();
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        priceIdAddPolicySofala = ResourceBundle.getBundle("configPayBonus").getString("priceIdAddPolicySofala");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        calNow.setTime(new Date());
        calNow.add(Calendar.MINUTE, 5);
        calNow.set(Calendar.SECOND, 00);
        calNow.set(Calendar.MILLISECOND, 00);
        cal.setTime(new Date());
        cal.add(Calendar.DATE, 30);

        String resultAddPro;
        for (Record record : listRecord) {
            resultAddPro = "";
            SofFloodInput moRecord = (SofFloodInput) record;
            listResult.add(moRecord);

            logger.warn(" resultAddPro for sofala branch with isdn : " + moRecord.getMsisdn());
            String enday = sdf2.format(cal.getTime()).substring(0, sdf2.format(cal.getTime()).length() - 6) + "000000";
            String startday = sdf2.format(calNow.getTime());
            //Cong chih sach

            resultAddPro = pro.addPriceV2(moRecord.getMsisdn(), priceIdAddPolicySofala, startday, enday);
            if (!"0".equals(resultAddPro)) {
                logger.warn("Fail to  AddPro for sofala branch  " + moRecord.getMsisdn() + " errcode rs " + resultAddPro);
                moRecord.setResultCode("E01");
                moRecord.setDesctiption("AddPro for sofala branch  " + moRecord.getMsisdn() + " errcode rs " + resultAddPro);
            } else {
                moRecord.setResultCode("0");
                moRecord.setDesctiption("Success to AddPro for sofala branch ");
            }

            continue;
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
                append("|\tISDN\t|").
                append("|\tRegisterTime\t|").
                append("|\tType\t|");
        for (Record record : listRecord) {
            SofFloodInput bn = (SofFloodInput) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getMoId()).
                    append("||\t").
                    append(bn.getMsisdn()).
                    append("||\t").
                    append("||\t").
                    append(bn.getRegistorTime()).
                    append("||\t").
                    append(bn.getType());
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
