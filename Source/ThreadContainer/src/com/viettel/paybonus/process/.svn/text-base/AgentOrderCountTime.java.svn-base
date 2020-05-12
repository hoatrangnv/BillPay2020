/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbAgentOrderCountTime;
import com.viettel.paybonus.obj.AgentReportInfo;
import com.viettel.paybonus.obj.SaleTransOrder;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.ResourceBundle;

/**
 *
 * @author dev_linh
 */
public class AgentOrderCountTime extends ProcessRecordAbstract {

    DbAgentOrderCountTime db;
    SimpleDateFormat sdf;
    SimpleDateFormat sdfHours;
    SimpleDateFormat sdfDate;
    String agentOrderReceiveReport;
    String[] arrAgentOrderReceiveReport;

    public AgentOrderCountTime() {
        super();
        logger = Logger.getLogger(AgentOrderCountTime.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbAgentOrderCountTime("dbsm", logger);
        sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        sdfHours = new SimpleDateFormat("HHmmss");
        sdfDate = new SimpleDateFormat("yyyyMMdd");
        agentOrderReceiveReport = ResourceBundle.getBundle("configPayBonus").getString("agentOrderReceiveReport");
        arrAgentOrderReceiveReport = agentOrderReceiveReport.split("\\|");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();

        boolean isOver2h = false, isOver24h = false, isOver72h = false;
        logger.info("Step 0: Reset data for all branch first...");
        db.resetDataReport();

        for (Record record : listRecord) {
            SaleTransOrder bn = (SaleTransOrder) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                logger.info("Step 1: Check orderDate between 9 A.M and 16 P.M, saleTransOrderId: " + bn.getSaleTransOrderId());
                Date orderDate = sdf.parse(bn.getSaleTransDate());

                long hoursOrder = Long.parseLong(sdfHours.format(orderDate));
                logger.info("HoursOrder is: " + hoursOrder + ", saleTransOrderId: " + bn.getSaleTransOrderId());
                if (hoursOrder < 90000) {
                    orderDate = sdf.parse(sdfDate.format(orderDate) + "090000");
                    logger.info("Order date less than 9 A.M, set new date of order: " + orderDate.toString());
                } else if (hoursOrder > 160000) {
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(orderDate);
                    if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) {
                        calendar.add(Calendar.DATE, 2);
                    } else {
                        calendar.add(Calendar.DATE, 1);
                    }
                    orderDate = sdf.parse(sdfDate.format(calendar.getTime()) + "090000");
                    logger.info("Order date greater than 16 P.M, set new date of order: " + orderDate.toString());
                } else {
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(orderDate);
                    if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
                        calendar.add(Calendar.DATE, 1);
                        logger.info("Hours of order is normal, but orderDate is Sunday, set to 9 A.M Monday.");
                        orderDate = sdf.parse(sdfDate.format(calendar.getTime()) + "090000");
                    }
                    logger.info("Date of order is normal, between 9 A.M and 16 P.M: " + orderDate.toString());
                }

                Date sysdate = sdf.parse(bn.getSysdate());
                long hoursSysdate = Long.parseLong(sdfHours.format(sysdate));
                logger.info("HoursSysdate is: " + hoursSysdate + ", saleTransOrderId: " + bn.getSaleTransOrderId());
                if (hoursSysdate < 90000) {
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(sysdate);
                    if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY) {
                        calendar.add(Calendar.DATE, -2);
                    } else {
                        calendar.add(Calendar.DATE, -1);
                    }
                    sysdate = calendar.getTime();
                    sysdate = sdf.parse(sdfDate.format(sysdate) + "160000");
                    logger.info("Date of sysdate less than 9 A.M, set new date of sysdate: " + sysdate.toString());
                } else if (hoursSysdate > 160000) {
                    sysdate = sdf.parse(sdfDate.format(sysdate) + "160000");
                    logger.info("Date of sysdate greater than 16 P.M, set new date of sysdate: " + sysdate.toString());
                } else {
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(sysdate);
                    if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
                        calendar.add(Calendar.DATE, -1);
                        logger.info("Hours of order is normal, but orderDate is Sunday, set to 16 P.M Saturday.");
                        sysdate = calendar.getTime();
                        sysdate = sdf.parse(sdfDate.format(sysdate) + "160000");
                    }
                    logger.info("Date of sysdate is normal, between 9 A.M and 16 P.M: " + sysdate.toString());
                }
                double hours = 0;
                if (orderDate.after(sysdate)) {
                    logger.info("Order date after sysdate...no need calculate....");
                } else {
                    logger.info("Order date before sysdate...start calculate");
                    Date tmpOrderDate = sdfDate.parse(sdfDate.format(orderDate));
                    Date tmpSysdate = sdfDate.parse(sdfDate.format(sysdate));
                    Calendar start = Calendar.getInstance();
                    start.setTime(tmpOrderDate);
                    Calendar end = Calendar.getInstance();
                    end.setTime(tmpSysdate);

                    for (Date date = start.getTime(); (start.before(end) || start.equals(end)); start.add(Calendar.DATE, 1), date = start.getTime()) {
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(date);
                        Date startDate = sdf.parse(sdfDate.format(date) + "090000");
                        Date endDate = sdf.parse(sdfDate.format(date) + "160000");
                        if (cal.get(Calendar.DAY_OF_WEEK) != Calendar.SUNDAY) {
                            if (endDate.after(sysdate)) {
                                hours += (sysdate.getTime() - orderDate.getTime()) / (60 * 60 * 1000f);
                            } else {
                                hours += (endDate.getTime() - orderDate.getTime()) / (60 * 60 * 1000f);
                            }
                        } else {
                            logger.info("current day is Sunday no need add hours, saleTransOrderId: " + bn.getSaleTransOrderId());
                        }
                        Calendar tmpCalendar = Calendar.getInstance();
                        tmpCalendar.setTime(startDate);
                        tmpCalendar.add(Calendar.DATE, 1);
                        orderDate = tmpCalendar.getTime();

                    }
                    BigDecimal bd = BigDecimal.valueOf(hours);
                    bd = bd.setScale(2, RoundingMode.HALF_UP);
                    hours = bd.doubleValue();
                    logger.info("total hours after count: " + hours + ", saleTransOrderId: " + bn.getSaleTransOrderId());

                }
                String receiverCode = db.getReceiverCode(bn.getReceiverId());
                if (db.checkOrderPincode(bn.getReceiverId())) {
                    logger.info("Order is pincode, assign receiverCode = PINCODE, saleTransOrderId: " + bn.getSaleTransOrderId());
                    receiverCode = "PINCODE";
                } else {
                    receiverCode = receiverCode.substring(0, 3);
                }
                logger.info("Step 2: Update data newest..., saleTransOrderId: " + bn.getSaleTransOrderId());
                List<AgentReportInfo> lstAgentReport = db.getListReportInfo();
                if (lstAgentReport.size() == 14) {
                    logger.info("Step 2: Update basic data for each branch..., saleTransOrderId: " + bn.getSaleTransOrderId());
                    for (AgentReportInfo obj : lstAgentReport) {
                        db.updateBasicInfo(obj);
                    }
                    logger.info("Step 3: Check KPI hours of agent, saleTransOrderId: " + bn.getSaleTransOrderId());
                    int kpiHours = db.getKPIHours(bn.getReceiverId());
                    if (hours > 2 && hours <= 24 && hours > kpiHours) {
                        isOver2h = true;
                        logger.info("saleTransOrderId: " + bn.getSaleTransOrderId() + ", approve over 2h, receiverId: " + bn.getReceiverId());
                        db.updateAgentOrderOver2h(receiverCode);
                    } else if (hours > 24 && hours <= 72 && hours > kpiHours) {
                        isOver24h = true;
                        logger.info("saleTransOrderId: " + bn.getSaleTransOrderId() + ", approve over 24h, receiverId: " + bn.getReceiverId());
                        db.updateAgentOrderOver24h(receiverCode);
                    } else if (hours > 72 && hours > kpiHours) {
                        isOver72h = true;
                        logger.info("saleTransOrderId: " + bn.getSaleTransOrderId() + ", approve over 72h, receiverId: " + bn.getReceiverId());
                        db.updateAgentOrderOver72h(receiverCode);
                    } else {
                        isOver2h = false;
                        isOver24h = false;
                        isOver72h = false;
                    }
                } else {
                    logger.info("Have problem when get report data, not enough data for 13 Branch, saleTransOrderId: " + bn.getSaleTransOrderId());
                    db.sendSmsV2("258870093239", "Not enough data for all branch.", "86952");
                }

            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getStaffId()
                        + " so continue with other transaction");
                continue;
            }
        }

        logger.info("Step 4: Complete update data, now send sms report final");
        for (String isdn : arrAgentOrderReceiveReport) {
            db.sendSmsReport(isdn);
        }
        listRecord.clear();

        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        br.setLength(0);
        br.append("\r\n").
                append("|\tSALE_TRANS_ORDER_ID|").
                append("|\tSALE_TRANS_DATE\t|").
                append("|\tRECEIVER_ID\t|").
                append("|\tSYS_DATE\t|").
                append("|\tSHOP_ID\t|").
                append("|\tSTAFF_ID\t|").
                append("|\t\t|");
        for (Record record : listRecord) {
            SaleTransOrder bn = (SaleTransOrder) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getSaleTransOrderId()).
                    append("||\t").
                    append(bn.getSaleTransDate()).
                    append("||\t").
                    append(bn.getReceiverId()).
                    append("||\t").
                    append(bn.getSysdate()).
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
