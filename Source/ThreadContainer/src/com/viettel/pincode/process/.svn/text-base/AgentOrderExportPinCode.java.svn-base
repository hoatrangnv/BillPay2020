/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.pincode.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbAgentOrderExportPincode;
import com.viettel.paybonus.obj.PinCode;
import com.viettel.paybonus.obj.Serial;
import com.viettel.paybonus.obj.StockModel;
import com.viettel.paybonus.service.Service;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.util.ArrayList;
import java.util.ResourceBundle;

/**
 * @author LinhNBV
 */
public class AgentOrderExportPinCode extends ProcessRecordAbstract {

    DbAgentOrderExportPincode db;
    Service services;
    String saleStaffExportPincode;
    String lstIsdnReceiveError;
    String[] arrIsdnRecevieError;

    public AgentOrderExportPinCode() {
        super();
        logger = Logger.getLogger(AgentOrderExportPinCode.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbAgentOrderExportPincode("dbsm", logger);
        services = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), logger);
        saleStaffExportPincode = ResourceBundle.getBundle("configPayBonus").getString("saleStaffExportPincode");
        lstIsdnReceiveError = ResourceBundle.getBundle("configPayBonus").getString("lstIsdnReceiveError");
        arrIsdnRecevieError = lstIsdnReceiveError.split("\\|");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            PinCode moRecord = (PinCode) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();

        for (Record record : listRecord) {
            PinCode bn = (PinCode) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                if (!db.checkSaleTransOrder(bn.getSaleTransOrderId())) {
                    logger.info("Order Nr." + bn.getSaleTransOrderId() + ", is not new order,"
                            + "saleTransOrderId: " + bn.getSaleTransOrderId());
                    bn.setResultCode("E07");
                    bn.setStatus(2L);
                    bn.setDescription("Order Nr." + bn.getSaleTransOrderId() + ", is not new order.");
                    db.sendSmsByList(arrIsdnRecevieError, "[Auto] - AgentOrderExportPinCode fail|" + bn.getDescription());
                    continue;
                }
                logger.info("Step 1: Get agentCode order, saleTransOrderId: " + bn.getSaleTransOrderId());
                if (!db.checkStaffExportOrder(saleStaffExportPincode)) {
                    logger.info("Staff export is invalid, stop process, staff: " + saleStaffExportPincode
                            + "saleTransOrderId: " + bn.getSaleTransOrderId());
                    bn.setResultCode("E01");
                    bn.setStatus(2L);
                    bn.setDescription("Staff export is invalid.");
                    db.sendSmsByList(arrIsdnRecevieError, "[Auto] - AgentOrderExportPinCode fail|" + bn.getDescription());
                    continue;
                }
                List<StockModel> listStockModel = db.getListStockModel(bn.getSaleTransOrderId());
                if (listStockModel.isEmpty()) {
                    logger.info("Can not get listStockModel, stop process, "
                            + "saleTransOrderId: " + bn.getSaleTransOrderId());
                    bn.setResultCode("E02");
                    bn.setStatus(2L);
                    bn.setDescription("Can not get listStockModel.");
                    db.sendSmsByList(arrIsdnRecevieError, "[Auto] - AgentOrderExportPinCode fail|" + bn.getDescription());
                    continue;
                }
                boolean isExport = true;
                for (StockModel stockModel : listStockModel) {
                    stockModel.setCheckSerial(1L);
                    List<Serial> listSerialAvaiable = db.getListSerialAvailableExport(stockModel.getStockModelId());
                    if (listSerialAvaiable.isEmpty()) {
                        isExport = false;
                        break;
                    }
                    long quantitySaling = stockModel.getQuantitySaling();
                    long tmpQuantity = 0;
                    while (tmpQuantity != quantitySaling) {
                        for (Serial serial : listSerialAvaiable) {
                            logger.info("quantitySaling: " + quantitySaling + ", tmpQuantity: " + tmpQuantity + ", quantitySerial avaiable export: " + serial.getQuantity());
                            if (serial.getQuantity() < quantitySaling && tmpQuantity < quantitySaling) {
                                if (serial.getQuantity() > (quantitySaling - tmpQuantity)) {
                                    long quantityMissing = quantitySaling - tmpQuantity;
                                    String endSerial = String.valueOf(Long.valueOf(serial.getFromSerial()) + quantityMissing - 1);
                                    Serial tmpSerial = new Serial();
                                    tmpSerial.setFromSerial(serial.getFromSerial());
                                    tmpSerial.setToSerial(endSerial);
                                    tmpSerial.setQuantity(quantityMissing);
                                    tmpQuantity += quantityMissing;
                                    stockModel.getListSerial().add(tmpSerial);
                                    break;
                                } else {
                                    stockModel.getListSerial().add(serial);
                                    tmpQuantity += serial.getQuantity();
                                }
                            } else {
                                long quantityMissing = quantitySaling - tmpQuantity;
                                String endSerial = String.valueOf(Long.valueOf(serial.getFromSerial()) + quantityMissing - 1);
                                Serial tmpSerial = new Serial();
                                tmpSerial.setFromSerial(serial.getFromSerial());
                                tmpSerial.setToSerial(endSerial);
                                tmpSerial.setQuantity(quantityMissing);
                                tmpQuantity += quantityMissing;
                                stockModel.getListSerial().add(tmpSerial);
                                break;
                            }

                        }

                    }
                    if (tmpQuantity != quantitySaling) {
                        isExport = false;
                        break;
                    }
                }
                if (!isExport) {
                    logger.info("Serial not available to export, stop process, "
                            + "saleTransOrderId: " + bn.getSaleTransOrderId());
                    bn.setResultCode("E03");
                    bn.setStatus(2L);
                    bn.setDescription("Serial not available to export.");
                    db.sendSmsByList(arrIsdnRecevieError, "[Auto] - AgentOrderExportPinCode fail|" + bn.getDescription());
                    continue;
                }
                //Update token and get token before export
                int resetToken = db.resetToken(saleStaffExportPincode);
                if (resetToken != 1) {
                    logger.info("Reset token saleStaffExport fail, stop process, staffCode: " + saleStaffExportPincode
                            + "saleTransOrderId: " + bn.getSaleTransOrderId());
                    bn.setResultCode("E04");
                    bn.setStatus(2L);
                    bn.setDescription("Reset token saleStaffExport fail.");
                    db.sendSmsByList(arrIsdnRecevieError, "[Auto] - AgentOrderExportPinCode fail|" + bn.getDescription());
                    continue;
                }
                String tokenValue = db.getTokenValue(saleStaffExportPincode);
                if (tokenValue.isEmpty()) {
                    logger.info("TokenValue is empty, stop process, staffCode: " + saleStaffExportPincode
                            + "saleTransOrderId: " + bn.getSaleTransOrderId());
                    bn.setResultCode("E05");
                    bn.setStatus(2L);
                    bn.setDescription("TokenValue is empty.");
                    db.sendSmsByList(arrIsdnRecevieError, "[Auto] - AgentOrderExportPinCode fail|" + bn.getDescription());
                    continue;
                }
                String xmlExport = db.buildRawData(bn.getSaleTransOrderId(), tokenValue, listStockModel);
                logger.info("Begin exportOrder: saleTransOrderId: " + bn.getSaleTransOrderId()
                        + ", request: " + xmlExport);
                String exportOrder = services.exportOrder(xmlExport);
                String[] arrResult = exportOrder.split("\\|"); //TOKEN_INVALID|[WE-005] The session is closed. Please login again!.
                if (arrResult.length == 2) {

                    bn.setDescription(arrResult[1]);
                    if ("0".equals(arrResult[0])) {
                        bn.setStatus(1L);
                        bn.setResultCode("0");
                    } else {
                        bn.setStatus(2L);
                        bn.setResultCode("E06");
                    }
                } else {
                    bn.setResultCode("E06");
                    bn.setStatus(2L);
                    bn.setDescription("Export fail. Unknown reason.");
                    db.sendSmsByList(arrIsdnRecevieError, "[Auto] - AgentOrderExportPinCode fail|" + bn.getDescription());
                    continue;
                }
            } else {
                logger.warn("After validate respone code is fail saleTransOrderId " + bn.getSaleTransOrderId()
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
        br.setLength(0);
        br.append("\r\n").
                append("|\tSALE_TRANS_ORDER_ID\t|").
                append("|\tRECEIVER_ID\t|").
                append("|\tCREATE_STAFF_ID");
        for (Record record : listRecord) {
            PinCode bn = (PinCode) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getSaleTransOrderId()).
                    append("||\t").
                    append(bn.getReceiverId()).
                    append("||\t").
                    append(bn.getCreateStaffId());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        ex.printStackTrace();
        logger.warn("TEMPLATE process exception record: " + ex.toString());
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
}
