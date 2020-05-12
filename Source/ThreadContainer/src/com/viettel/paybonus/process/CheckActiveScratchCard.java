/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbCheckActiveScratchCard;
import com.viettel.paybonus.obj.VcRequest;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 *
 * @author dev_linh
 */
public class CheckActiveScratchCard extends ProcessRecordAbstract {

    DbCheckActiveScratchCard db;
    Exchange exch;

    public CheckActiveScratchCard() {
        super();
        logger = Logger.getLogger(CheckActiveScratchCard.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbCheckActiveScratchCard("dbsm", logger);
        exch = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        boolean isActive;

        for (Record record : listRecord) {
            isActive = false;
            VcRequest bn = (VcRequest) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                logger.info("Step 1: Start...ToSerial: " + bn.getToSerial() + ", saleTransOrderId: " + bn.getSaleTransOrderId()
                        + ", start QueryCard Information.");
                String result = exch.queryScratchCard(bn.getToSerial());

                if ("0".equals(result)) {
                    logger.info("ToSerial: " + bn.getToSerial() + ", saleTransOrderId: " + bn.getSaleTransOrderId() + ", already actived. Start send SMS.");
                    isActive = true;
                } else if ("3005".equals(result)) { //That means... Description>The recharge card information does not exist.</Description>
                    logger.info("ToSerial: " + bn.getToSerial() + ", saleTransOrderId: " + bn.getSaleTransOrderId() + ", errCode = 3005, start check already sale or not.");
                    if (db.checkSaleScratchCard(bn.getToSerial())) {
                        logger.info("ToSerial: " + bn.getToSerial() + ", saleTransOrderId: " + bn.getSaleTransOrderId() + ", already actived.");
                        isActive = true;
                    } else {
                        isActive = false;
                    }
                } else {
                    logger.info("ToSerial: " + bn.getToSerial() + ", saleTransOrderId: " + bn.getSaleTransOrderId() + ", unknown error.");
                    isActive = false;
                }
                if (db.checkExistAgentOrderActiveDuration(bn.getFromSerial(), bn.getToSerial(), bn.getSaleTransOrderId())) {
                    logger.info("ToSerial: " + bn.getToSerial() + ", saleTransOrderId: " + bn.getSaleTransOrderId() + ", Exists on table agent_order_active_duration.");
                    if (isActive) {
                        //check exist on table >>> update status >> 1: active, 0: not yet...
                        logger.info("ToSerial: " + bn.getToSerial() + ", saleTransOrderId: " + bn.getSaleTransOrderId() + ", update status to active.");
                        db.updateStatusAgentOrderDuration(0.05, 1, bn.getFromSerial(), bn.getToSerial(), bn.getSaleTransOrderId());
                    } else {
                        logger.info("ToSerial: " + bn.getToSerial() + ", saleTransOrderId: " + bn.getSaleTransOrderId() + ", update status to not yet active.");
                        db.updateStatusAgentOrderDuration(0.05, 0, bn.getFromSerial(), bn.getToSerial(), bn.getSaleTransOrderId());
                    }
                } else {
                    if (isActive) {
                        logger.info("ToSerial: " + bn.getToSerial() + ", saleTransOrderId: " + bn.getSaleTransOrderId() + ", insert table with status active.");
                        db.insertAgentOrderDuration(1, bn.getFromSerial(), bn.getToSerial(), bn.getSaleTransOrderId());
                    } else {
                        logger.info("ToSerial: " + bn.getToSerial() + ", saleTransOrderId: " + bn.getSaleTransOrderId() + ", insert table with status not yet active.");
                        db.insertAgentOrderDuration(0, bn.getFromSerial(), bn.getToSerial(), bn.getSaleTransOrderId());
                    }
                }
                if (isActive) {
                    logger.info("Step 2: Get Information for sms....");
                    String strIsdn = db.getListIsdnReceiveWarningOrder(String.valueOf(bn.getShopId()));
                    String[] listIsdn = strIsdn.split("\\|");
                    String agentMakeOrder = db.getAgentMakeOrder(bn.getSaleTransOrderId());
                    String isdnAgentMakeOrder = db.getTelByStaffCode(agentMakeOrder);
                    String receiverCode = db.getReceiverCode(bn.getSaleTransOrderId());
                    String isdnReceiverCode = db.getTelByStaffCode(receiverCode);
                    Date sysdate = new Date();
                    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                    String amountTax = db.getAmountTaxOfOrder(bn.getSaleTransOrderId());
                    String nameReceiver = db.getNameOfAgentCode(receiverCode);
                    String priceOfScratchCard = db.getPriceOfScratchCard(bn.getToSerial());

                    //Check serial is the last one...
                    logger.info("ToSerial: " + bn.getToSerial() + ", saleTransOrderId: " + bn.getSaleTransOrderId() + ", start check serial is the last one of range.");
                    //Get count range of serial
                    long totalRange = db.countTotalRangeSerial(bn.getSaleTransOrderId());
                    //Get current range serial active...
                    long totalCurentRange = db.countCurrentRangeSerialActive(bn.getSaleTransOrderId());
                    if (totalRange == totalCurentRange) {
                        logger.info("ToSerial: " + bn.getToSerial() + ", saleTransOrderId: " + bn.getSaleTransOrderId() + ", already active all, start send sms final active.");
                        //all serial active...
                        String message = "Ordem Nr." + bn.getSaleTransOrderId() + " (" + amountTax + " MT) em " + sdf.format(sysdate)
                                + " para " + receiverCode + "-" + nameReceiver + " em " + receiverCode.substring(0, 3).toUpperCase()
                                + " a serie " + bn.getFromSerial() + " a " + bn.getToSerial()
                                + " das recargas de " + priceOfScratchCard + " MT  foi ativada com sucesso! Ordem e ativado com sucesso. Obrigado!";
                        for (String isdn : listIsdn) {
                            db.sendSmsV2(isdn, message, "86952");
                        }
                        db.sendSmsV2(isdnAgentMakeOrder, message, "86952");
                        if (!agentMakeOrder.equalsIgnoreCase(receiverCode)) {
                            db.sendSmsV2(isdnReceiverCode, message, "86952");
                        }
                    } else {
                        String message = "Ordem Nr." + bn.getSaleTransOrderId() + " (" + amountTax + " MT) em " + sdf.format(sysdate)
                                + " para " + receiverCode + "-" + nameReceiver + " em " + receiverCode.substring(0, 3).toUpperCase()
                                + " a serie " + bn.getFromSerial() + " a " + bn.getToSerial()
                                + " (" + totalCurentRange + "/" + totalRange + ")" + " das recargas de " + priceOfScratchCard + " MT  foi ativada com sucesso! Obrigado!";
                        for (String isdn : listIsdn) {
                            db.sendSmsV2(isdn, message, "86952");
                        }
                        db.sendSmsV2(isdnAgentMakeOrder, message, "86952");
                        if (!agentMakeOrder.equalsIgnoreCase(receiverCode)) {
                            db.sendSmsV2(isdnReceiverCode, message, "86952");
                        }
                    }

                }

            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getStaffId()
                        + " so continue with other transaction");
                continue;
            }
        }

        logger.info("Process completed...Thread will be sleep in 3 minutes...");
        Thread.sleep(1000 * 60 * 3);

        listRecord.clear();

        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        br.setLength(0);
        br.append("\r\n").
                append("|\tREQUEST_ID|").
                append("|\tUSER_ID\t|").
                append("|\tFROM_SERIAL\t|").
                append("|\tTO_SERIAL\t|").
                append("|\tSHOP_ID\t|").
                append("|\tSTAFF_ID\t|").
                append("|\tSALE_TRANS_ORDER_ID\t|").
                append("|\t\t|");
        for (Record record : listRecord) {
            VcRequest bn = (VcRequest) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getRequestId()).
                    append("||\t").
                    append(bn.getUserId()).
                    append("||\t").
                    append(bn.getFromSerial()).
                    append("||\t").
                    append(bn.getToSerial()).
                    append("||\t").
                    append(bn.getShopId()).
                    append("||\t").
                    append(bn.getStaffId()).
                    append("||\t").
                    append(bn.getSaleTransOrderId());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        ex.printStackTrace();
        logger.warn("TEMPLATE process exception record: " + ex.toString());
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
