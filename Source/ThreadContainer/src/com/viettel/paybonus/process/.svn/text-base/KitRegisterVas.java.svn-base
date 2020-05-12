/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbKitRegisterVas;
import com.viettel.paybonus.obj.KitVas;
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
public class KitRegisterVas extends ProcessRecordAbstract {

    Service services;
    DbKitRegisterVas db;
    String urlRegisterMCA;
    String urlCRBT;
    //LinhNBV modified on September 04 2017: Add variables for message

    public KitRegisterVas() {
        super();
        logger = Logger.getLogger(KitRegisterVas.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        services = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), logger);
        db = new DbKitRegisterVas();
        urlRegisterMCA = ResourceBundle.getBundle("configPayBonus").getString("urlRegisterMCA");
        urlCRBT = ResourceBundle.getBundle("configPayBonus").getString("urlCRBT");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            KitVas moRecord = (KitVas) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        for (Record record : listRecord) {
            KitVas bn = (KitVas) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                String resultRegister = services.registerMCA("258" + bn.getIsdn(), urlRegisterMCA);
                if ("ERR".equals(resultRegister)) {
                    logger.warn("Error occur when register MCA, product_code: " + bn.getProductCode() + ", isdn: " + bn.getIsdn());
                } else {
                    logger.warn("Register MCA successfully, product_code: " + bn.getProductCode() + ", isdn: " + bn.getIsdn());
                }
                String resultRegisterCRBT = services.registerCRBT("258" + bn.getIsdn(), urlCRBT);
                if ("ERR".equals(resultRegisterCRBT)) {
                    logger.warn("Error occur when register CRBT, product_code: " + bn.getProductCode() + ", isdn: " + bn.getIsdn());
                } else {
                    logger.warn("Register CRBT successfully, product_code: " + bn.getProductCode() + ", isdn: " + bn.getIsdn());
                }
            } else {
                logger.warn("After validate respone code is fail kit_vas_id " + bn.getKitVasId()
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
                append("|\tKIT_VAS_ID|").
                append("|\tISDN|").
                append("|\tSERIAL\t|").
                append("|\tCREATE_USER\t|").
                append("|\tPRODUCT_CODE\t|");
        for (Record record : listRecord) {
            KitVas bn = (KitVas) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getKitVasId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getSerial()).
                    append("||\t").
                    append(bn.getCreateUser()).
                    append("||\t").
                    append(bn.getProductCode());
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
