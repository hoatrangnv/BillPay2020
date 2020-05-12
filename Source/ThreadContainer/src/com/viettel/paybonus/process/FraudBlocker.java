/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbFraudSub;
import com.viettel.paybonus.obj.FraudSubInput;
import com.viettel.paybonus.service.Service;
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
public class FraudBlocker extends ProcessRecordAbstract {

    Service service;
    DbFraudSub db;
    int fraudSMSConfig;
    int fraudDataCofig;
    int fraudPepriodDayConfig;

    public FraudBlocker() {
        super();
        logger = Logger.getLogger(FraudBlocker.class);

    }

    @Override
    public void initBeforeStart() throws Exception {
        try {
            service = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), this.logger);
            db = new DbFraudSub();
            String fraudSMSConfigSring = ResourceBundle.getBundle("configPayBonus").getString("fraudSMSConfig");
            String fraudDataCofigSring = ResourceBundle.getBundle("configPayBonus").getString("fraudDataCofig");
            String fraudPepriodDayConfigSring = ResourceBundle.getBundle("configPayBonus").getString("fraudPepriodDayConfig");
            fraudSMSConfig = Integer.parseInt(fraudSMSConfigSring);
            fraudDataCofig = Integer.parseInt(fraudDataCofigSring);
            fraudPepriodDayConfig = Integer.parseInt(fraudPepriodDayConfigSring);
            if (fraudSMSConfig <= 0 || fraudSMSConfig <= 0 || fraudPepriodDayConfig <= 0) {
                throw new Exception("Invalid configuration. Please check the configuration....");
            }
        } catch (Exception ex) {
            throw new Exception("Exception While invalid configuration.Please check the configuration....");
        }

    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();

        for (Record record : listRecord) {
            FraudSubInput bn = (FraudSubInput) record;
            listResult.add(bn);
            //Check sub status
            if (bn.getIsdn() == null || bn.getIsdn().trim().length() == 0) {
                logger.info("Input invalid, isdn " + bn.getIsdn());
                bn.setResultCode("01");
                bn.setDescription("Input invalid, the isdn is null or empty");
                continue;
            }

            if ("BLOCK".equals(bn.getActionType())) {
                //<editor-fold defaultstate="collapsed" desc="BLOCK SUB">
                //check active subscriber
                if (!db.checkStatusPhoneByIsdn(bn.getIsdn(), true)) {
                    logger.info("The number was blocked or does not exist, isdn " + bn.getIsdn());
                    bn.setResultCode("02");
                    bn.setDescription("The number was blocked or does not exist");
                    continue;
                }
                String subId = db.getSubIdByIsdn(bn.getIsdn());
                if (subId == null || subId.trim().length() == 0) {
                    logger.info("Cannot find subsciber information, isdn " + bn.getIsdn());
                    bn.setResultCode("03");
                    bn.setDescription("Cannot find subsciber information");
                    continue;
                }
                //Check generate SMS
                boolean isGenerateSMS = db.checkGenerrateSMS(bn.getIsdn(), fraudSMSConfig, fraudPepriodDayConfig);
                if (isGenerateSMS) {
                    logger.info("This number has gernerated SMS, skeep block, isdn " + bn.getIsdn());
                    bn.setResultCode("05");
                    bn.setDescription("This number has gernerated SMS, skeep blocking.");
                    continue;
                }

                //Check generate DATA
                boolean isGenerateData = db.checkGenerrateData(bn.getIsdn(), fraudDataCofig, fraudPepriodDayConfig);
                if (isGenerateData) {
                    logger.info("This number has gernerated Data, skeep block, isdn " + bn.getIsdn());
                    bn.setResultCode("05");
                    bn.setDescription("This number has gernerated Data, skeep blocking.");
                    continue;
                }
                String servicesRegisted = db.getPreVASCode(subId);
                //Block subsctiber
                String blockStatus = service.blockOpenFraudSubscriber(bn.getIsdn(), servicesRegisted, true);
                bn.setOcsResponseCode(blockStatus);
                if ((!"0".equals(blockStatus)) && (!"405000000".equals(blockStatus))) {
                    logger.info("Block subscriber failed, isdn " + bn.getIsdn());
                    bn.setResultCode("04");
                    bn.setDescription("Block subscriber failed");
                    continue;
                }
                int updateSubMb = db.updateActStatusSubMB(subId, "01");
                if (updateSubMb <= 0) {
                    logger.info("Block subscriber successfully,but failed to update status in sub_mb table, isdn " + bn.getIsdn());
                    bn.setDescription("Block subscriber successfully,but failed to update status in sub_mb");
                }
                int inserLog = db.insertActionAudit(subId, "Block fraud sub from Sigos");
                if (inserLog <= 0) {
                    logger.info("Block subscriber successfully,but failed to insert action  log, isdn " + bn.getIsdn());
                    bn.setDescription("Block subscriber successfully,but failed to insert action  log");
                } else {
                    bn.setDescription("Block subscriber successfully");
                }
                logger.info("Block subscriber successfully, isdn " + bn.getIsdn());
                bn.setResultCode("0");
                //</editor-fold> 
            } else if ("UNLOCK".equals(bn.getActionType())) {
                //<editor-fold defaultstate="collapsed" desc="OPEN SUB">
                if (!db.checkStatusPhoneByIsdn(bn.getIsdn(), false)) {
                    logger.info("The number is active or does not exist to unblock, isdn " + bn.getIsdn());
                    bn.setResultCode("10");
                    bn.setDescription("The number is active or does not exist to unblock");
                    db.updateLastProcessInfo(bn.getFraud_sub_input_id(), bn.getIsdn(), 2);//Skeep scan next time
                    continue;
                }
                String subIdOpen = db.getSubIdByIsdn(bn.getIsdn());
                if (subIdOpen == null || subIdOpen.trim().length() == 0) {
                    logger.info("Cannot find subsciber information, isdn " + bn.getIsdn());
                    bn.setResultCode("11");
                    bn.setDescription("Cannot find subsciber information");
                    db.updateLastProcessInfo(bn.getFraud_sub_input_id(), bn.getIsdn(), 2);//Skeep scan next time
                    continue;
                }
                boolean isUnlock = false;
                //Check generating SMS
                boolean isGenerateSMS = db.checkGenerrateSMS(bn.getIsdn(), fraudSMSConfig, fraudPepriodDayConfig);
                if (isGenerateSMS) {
                    logger.info("This number has gernerated SMS,set unlock mode, isdn " + bn.getIsdn());
                    isUnlock = true;
                }
                //Check generating DATA
                if (!isUnlock) {
                    boolean isGenerateData = db.checkGenerrateData(bn.getIsdn(), fraudDataCofig, fraudPepriodDayConfig);
                    if (isGenerateData) {
                        logger.info("This number has gernerated Data, set unlock mode, isdn " + bn.getIsdn());
                        isUnlock = true;
                    }
                }
                if (!isUnlock) {
                    logger.info("This number hasn't gernerated Data or SMS, Skeep unblock, isdn " + bn.getIsdn());
                    bn.setResultCode("12");
                    bn.setDescription("This number hasn't gernerated Data or SMS, Skeep unblock");
                    db.updateLastProcessInfo(bn.getFraud_sub_input_id(), bn.getIsdn(), 0);//Scan again in next time
                    continue;
                }
                String servicesRegisted = db.getPreVASCode(subIdOpen);
                //Block subsctiber
                String openStatus = service.blockOpenFraudSubscriber(bn.getIsdn(), servicesRegisted, false);
                bn.setOcsResponseCode(openStatus);
                if ((!"0".equals(openStatus))) {
                    logger.info("Unblock subscriber failed, isdn " + bn.getIsdn());
                    bn.setResultCode("13");
                    bn.setDescription("Unblock subscriber failed");
                    db.updateLastProcessInfo(bn.getFraud_sub_input_id(), bn.getIsdn(), 0);//Scan again in next time
                    continue;
                }
                int updateSubMb = db.updateActStatusSubMB(subIdOpen, "00");
                if (updateSubMb <= 0) {
                    logger.info("Unblock subscriber successfully,but failed to update status in sub_mb, isdn " + bn.getIsdn());
                    bn.setDescription("Unblock subscriber successfully,but failed to update status in sub_mb");
                }
                int inserLog = db.insertActionAudit(subIdOpen, "Unblock fraud sub by system");
                if (inserLog <= 0) {
                    logger.info("Unblock subscriber successfully,but failed to insert action  log, isdn " + bn.getIsdn());
                    bn.setDescription("Unblock subscriber successfully,but failed to insert action  log");
                } else {
                    bn.setDescription("Unblock subscriber successfully");
                }
                logger.info("Unblock subscriber successfully, isdn " + bn.getIsdn());
                bn.setResultCode("0");
                db.updateLastProcessInfo(bn.getFraud_sub_input_id(), bn.getIsdn(), 1);//Done status
//</editor-fold>
            } else {
                logger.info("Cannot get action type of this subsctiber " + bn.getIsdn());
                bn.setResultCode("99");
                bn.setDescription("Cannot get action type of this subsctiber");
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
                append("|\tFRAUD_SUB_INPUT_ID|").
                append("|\tISDN\t|").
                append("|\tCOMMAND\t|").
                append("|\tINPUT_TIME\t|").
                append("|\tFILE_NAME\t|");
        for (Record record : listRecord) {
            FraudSubInput bn = (FraudSubInput) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getFraud_sub_input_id()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getCommand()).
                    append("||\t").
                    append((bn.getInput_time() != null ? sdf.format(bn.getInput_time()) : null)).
                    append("||\t").
                    append(bn.getFileName());
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
