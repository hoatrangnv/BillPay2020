/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbOpenFlagRegisterInfo;
import com.viettel.paybonus.obj.LogRegisterInfo;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class OpenFlagRegisterInfo extends ProcessRecordAbstract {

    Exchange pro;
    DbOpenFlagRegisterInfo db;
    String countryCode;

    public OpenFlagRegisterInfo() {
        super();
        logger = Logger.getLogger(OpenFlagRegisterInfo.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbOpenFlagRegisterInfo();
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String openFlagResult;
        String openFlagBAOCResult;
        //LinhNBV 20180612 add variable
        String openFlagBAICResult;
        String openFlagPLMResult;
        String openFlagGPRSResult;
//LamNT 20180907 add variable
        String activeOnOCS;
        for (Record record : listRecord) {
            openFlagResult = "";
            openFlagBAOCResult = "";
            openFlagPLMResult = "";
            openFlagBAICResult = "";
            openFlagGPRSResult = "";
            activeOnOCS = "";
            LogRegisterInfo bn = (LogRegisterInfo) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                bn.setResultCode("0");
                bn.setDescription("Not open flag more, when profile checked correctly the other process will open flag");
//                Open ODBOC flag on HLR for customer
//                openFlagResult = pro.activeFlagSABLCK(countryCode + bn.getIsdn());
////                if ("0".equals(openFlagResult)) {
////                    logger.info("Open flag SABLCK successfully for sub when register info " + bn.getIsdn());
//////                    bn.setResultCode("0");
//////                    bn.setDescription("Open flag SABLCK successfully");
//////                    continue;
////                } else {
////                    logger.warn("Fail to open flag SABLCK for sub when register info " + bn.getIsdn());
////                    bn.setResultCode("E01");
////                    bn.setDescription("Fail to open flag SABLCK when register info");
////                }
//                Open BAOC flag on HLR for customer -- HLR_HW_MOD_BAOC
//                openFlagBAOCResult = pro.activeFlagBAOC(countryCode + bn.getIsdn());
//                if ("0".equals(openFlagBAOCResult)) {
//                    logger.info("Open flag BAOC successfully for sub when register info " + bn.getIsdn());
////                    bn.setResultCode("0");
////                    bn.setDescription("Open flag BAOC successfully");
////                    continue;
//                } else {
//                    logger.warn("Fail to open flag BAOC for sub when register info " + bn.getIsdn());
//                    bn.setResultCode("E02");
//                    bn.setDescription("Fail to open flag BAOC when register info, already open SABLOCK success before");
//                }
//                LinhNBV: Open flag BAIC
//                openFlagBAICResult = pro.activeFlagBAIC(countryCode + bn.getIsdn());
//                if ("0".equals(openFlagBAICResult)) {
//                    logger.info("Open flag BAIC successfully for sub when register info " + bn.getIsdn());
//                } else {
//                    logger.warn("Fail to open flag BAIC for sub when register info " + bn.getIsdn());
//                    bn.setResultCode("E03");
//                    bn.setDescription("Fail to open flag BAIC when register info, already open SABLOCK success before");
//                }
//                db.updateSubMbWhenActive(bn.getIsdn()); //20181112 active first to register Vas service and topup airtime via Emola                
//LamNT: Open flag OCSHW_ACTIVEFIRST
//                        20180924 start change new rule, not active auto, must call 150 or any number to active                
//                activeOnOCS = pro.activeOCSACTIVEFIRST(countryCode + bn.getIsdn());
//                if ("0".equals(activeOnOCS)) {
//                    logger.info("Open flag OCSHW_ACTIVEFIRST successfully for sub when register info " + bn.getIsdn());
////                    Huynq13 20180815 when active success, let update start_time on cm_pre.sub_mb for this sub to support register VAS immediately
//                    db.updateSubMbWhenActive(bn.getIsdn());
//                } else {
//                    logger.warn("Fail to open flag OCSHW_ACTIVEFIRST for sub when register info " + bn.getIsdn());
//                    bn.setResultCode("E04");
//                    bn.setDescription("Fail to open flag OCSHW_ACTIVEFIRST when register info");
//                }
//End LamNT
//                openFlagGPRSResult = pro.activeFlagGPRSLCK("258" + bn.getIsdn());
//                if ("0".equals(openFlagGPRSResult)) {
//                    logger.info("Open flag GPRS successfully for sub when register info " + bn.getIsdn());
//                } else {
//                    logger.warn("Fail to open flag GPRS for sub when register info " + bn.getIsdn());
//                    bn.setResultCode("E05");
//                    bn.setDescription("Fail to open flag GPRS for sub when register info");
//                }
////                end.

////                Open USSD flag on HLR for customer -- HLR_HW_MOD_PLMNSS
//                openFlagPLMResult = pro.activeFlagPLMNSS(countryCode + bn.getIsdn());
//                if ("0".equals(openFlagPLMResult)) {
//                    logger.info("Open flag PLMNSS successfully for sub when register info " + bn.getIsdn());
//                    bn.setResultCode("0");
//                    bn.setDescription("Open flag SABLOCK, BAOC, PLMNSS successfully");
//                    continue;
//                } else {
//                    logger.warn("Fail to open flag BAOC for sub when register info " + bn.getIsdn());
//                    bn.setResultCode("E06");
//                    bn.setDescription("Fail to open flag PLMNSS when register infom, already open SABLOCK, BAOC success before");
//                    continue;
//                }
            } else {
                logger.warn("After validate respone code is fail id " + bn.getLogRegisterInfoId()
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
                append("|\tSTAFFCODE\t|").
                append("|\tISDN\t|").
                append("|\tISSUE_DATE\t|");
        for (Record record : listRecord) {
            LogRegisterInfo bn = (LogRegisterInfo) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getLogRegisterInfoId()).
                    append("||\t").
                    append(bn.getStaffCode()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null));
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
