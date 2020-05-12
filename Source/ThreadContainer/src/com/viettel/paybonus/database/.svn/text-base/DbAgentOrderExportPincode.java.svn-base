/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.PinCode;
import com.viettel.paybonus.obj.Serial;
import com.viettel.paybonus.obj.StockModel;
import com.viettel.threadfw.manager.AppManager;

import com.viettel.threadfw.database.DbProcessorAbstract;
import static com.viettel.threadfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
import com.viettel.vas.util.ConnectionPoolManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;

public class DbAgentOrderExportPincode extends DbProcessorAbstract {

    private String loggerLabel = DbAgentOrderExportPincode.class.getSimpleName() + ": ";
    private String dbNameCofig;
    private String sqlDeleteMo = "update agent_order_pincode set status = ?, result_export = ?, description_export = ?, export_time = sysdate where sale_trans_order_id = ?";

    public DbAgentOrderExportPincode() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
    }

    public DbAgentOrderExportPincode(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
        dbNameCofig = sessionName;
    }

    public void closeStatement(Statement st) {
        try {
            if (st != null) {
                st.close();
                st = null;
            }
        } catch (Exception ex) {
            st = null;
        }
    }

    public void logTimeDb(String strLog, long timeSt) {
        long timeEx = System.currentTimeMillis() - timeSt;

        if (timeEx >= AppManager.minTimeDb && AppManager.loggerDbMap != null) {
            br.setLength(0);
            br.append(loggerLabel).
                    append(AppManager.getTimeLevelDb(timeEx)).append(": ").
                    append(strLog).
                    append(": ").
                    append(timeEx).
                    append(" ms");

            logger.warn(br);
        } else {
            br.setLength(0);
            br.append(loggerLabel).
                    append(strLog).
                    append(": ").
                    append(timeEx).
                    append(" ms");

            logger.info(br);
        }
    }

    @Override
    public Record parse(ResultSet rs) {
        PinCode record = new PinCode();
        long timeSt = System.currentTimeMillis();
        try {
            record.setSaleTransOrderId(rs.getLong("sale_trans_order_id"));
            record.setReceiverId(rs.getLong("receiver_id"));
            record.setCreateStaffId(rs.getLong("create_staff_id"));
            record.setCreateTime(rs.getString("create_time"));
            record.setResultCode("0");
            record.setDescription("Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse MoRecord");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        String batchId = "";

        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                PinCode sd = (PinCode) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getStatus());
                ps.setString(2, sd.getResultCode());
                ps.setString(3, sd.getDescription());
                ps.setLong(4, sd.getSaleTransOrderId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("ERROR update deleteQueue batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        int[] res = new int[0];
        return res;
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        int[] res = new int[0];
        return res;
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
        StringBuilder sb = new StringBuilder();
        try {
//            The first delete queue timeout
            deleteQueueTimeout(ids);
//            Save history
            for (String sd : ids) {
                sb.append(":" + sd);
            }
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
        }
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int[] deleteQueueTimeout(List<String> listId) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        StringBuilder sf = new StringBuilder();
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(sqlDeleteMo);
            sf.setLength(0);
            for (String id : listId) {
                ps.setLong(1, 9L);
                ps.setString(2, "FW_99_Timeout");
                ps.setString(3, "FW_99_Timeout");
                ps.setLong(4, Long.valueOf(id));
                ps.addBatch();
                sf.append(id);
                sf.append(", ");
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueueTimeout AgentOrderExportOrder listId " + sf.toString(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueueTimeout AgentOrderExportOrder, listId " + sf.toString(), timeStart);
        }
    }

    public boolean checkStaffExportOrder(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select * from sm.staff where status = 1 and staff_code = ? and shop_id = 7282";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setString(1, staffCode.toUpperCase());
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = true;
                break;
            }
            logTimeDb("Time to checkStaffExportOrder, staffCode: " + staffCode + ", result: " + result, timeSt);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("ERROR checkStaffExportOrder, staffCode" + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public List<StockModel> getListStockModel(Long saleTransOrderId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        List<StockModel> listStockModel = new ArrayList<StockModel>();
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select stock_model_id, (select stock_model_code from sm.stock_model where stock_model_id = a.stock_model_id) as stock_model_code, quantity from sm.sale_trans_detail_order a where sale_trans_order_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransOrderId);
            rs = ps.executeQuery();
            while (rs.next()) {
                long stockModelId = rs.getLong("stock_model_id");
                String stockModelCode = rs.getString("stock_model_code");
                long quantity = rs.getLong("quantity");
                StockModel stockModel = new StockModel();
                stockModel.setStockModelId(stockModelId);
                stockModel.setStockModelCode(stockModelCode);
                stockModel.setQuantitySaling(quantity);
                stockModel.setType("9");
                stockModel.setStockTypeId(6L);
                stockModel.setStockModelName(stockModelCode);
                stockModel.setTableName("STOCK_CARD");
                ArrayList<Serial> listSerial = new ArrayList<Serial>();
                stockModel.setListSerial(listSerial);
                listStockModel.add(stockModel);
            }
            logger.info("End getListStockModel saleTransOrderId " + saleTransOrderId + ", total record: " + listStockModel.size() + ", time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getListStockModel: ").
                    append(sql).append("\n")
                    .append(" saleTransOrderId ")
                    .append(saleTransOrderId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return listStockModel;
        }
    }

    public List<Serial> getListSerialAvailableExport(Long stockModelId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        List<Serial> listSerial = new ArrayList<Serial>();
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select min (serial) as fromserial, max (serial) as toserial, max(serial) - min(serial) + 1 as quantity from\n"
                    + "(select serial, serial - row_number () over (order by to_number(serial)) rn from  sm.stock_card\n"
                    + "where 1 = 1 and owner_id = 7282 and stock_model_id = ? and status = 1)\n"
                    + "group by rn order by quantity";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, stockModelId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String fromSerial = rs.getString("fromserial");
                long quantity = rs.getLong("quantity");
                String toSerial = rs.getString("toserial");

                Serial serial = new Serial();
                serial.setFromSerial(fromSerial);
                serial.setToSerial(toSerial);
                serial.setQuantity(quantity);
                listSerial.add(serial);
            }
            logger.info("End getListSerialAvailableExport stockModelId " + stockModelId + ", total record: " + listSerial.size() + ", time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getListSerialAvailableExport: ").
                    append(sql).append("\n")
                    .append(" stockModelId ")
                    .append(stockModelId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return listSerial;
        }
    }

    public String buildRawData(Long saleTransOrderId, String token, List<StockModel> listStockModel) {
        StringBuilder rawData = new StringBuilder();

        rawData.append("<saleInput>");
        HashMap<String, Object> param = new HashMap<String, Object>();
        param.put("saleTransOrderId", saleTransOrderId);
        param.put("isCheckOwner", "true");
        param.put("exportOrder", "true");
        param.put("sellerType", "2");
        param.put("token", token);

        ArrayList<String> xmlStockModel = new ArrayList<String>();
        for (StockModel item : listStockModel) {
            if (item.getQuantitySaling() > 0) {
                HashMap<String, Object> stockModelParam = new HashMap<String, Object>();
                if (item.getCheckSerial() != null
                        && item.getCheckSerial().compareTo(1L) == 0) {
                    // Neu la mat hang co serial
                    stockModelParam.put("haveSerial", "true");
                    ArrayList<String> xmlSerial = new ArrayList<String>();
                    for (Serial serial : item.getListSerial()) {
                        StringBuilder serialStr = new StringBuilder();
                        serialStr.append("<fromSerial>");
                        serialStr.append(serial.getFromSerial());
                        serialStr.append("</fromSerial>");
                        serialStr.append("<toSerial>");
                        serialStr.append(serial.getToSerial());
                        serialStr.append("</toSerial>");
                        xmlSerial.add(serialStr.toString());
                    }
                    for (Serial serial : item.getListSerial()) {
                        stockModelParam.put("toSerial", serial.getToSerial());
                    }
                    stockModelParam.put("lstSerial", xmlSerial);

                } else {
                    stockModelParam.put("haveSerial", "false");
                }

                stockModelParam.put("quantity", item.getQuantitySaling() + "");
                stockModelParam.put("stockModelCode", item.getStockModelCode()
                        .toLowerCase());
                stockModelParam
                        .put("stockModelId", item.getStockModelId() + "");
                stockModelParam.put("telecomServiceId", "1");
                param.put("lstStockModel",
                        buildXMLFromHashmap(stockModelParam));
                xmlStockModel.add(buildXMLFromHashmap(stockModelParam));
            }
        }
        param.put("lstStockModel", xmlStockModel);
        rawData.append("<locale>en_US</locale>");
        rawData.append(buildXMLFromHashmap(param));
        rawData.append("</saleInput>");

        return rawData.toString();
    }

    private String buildXMLFromHashmap(HashMap<String, Object> param) {
        StringBuilder result = new StringBuilder();

        Set<String> keySet = param.keySet();
        if (keySet != null && !keySet.isEmpty()) {
            for (String key : keySet) {
                Object value = param.get(key);
                if (value != null && value instanceof ArrayList<?>) {
                    ArrayList<String> arrayValue = (ArrayList<String>) value;
                    for (Object object : arrayValue) {
                        result.append("<").append(key).append(">");
                        result.append(object);
                        result.append("</").append(key).append(">");
                    }
                } else if (value == null || value.toString().trim().isEmpty()) {
                    result.append("<").append(key).append("/>");

                } else {
                    result.append("<").append(key).append(">");
                    result.append(param.get(key));
                    result.append("</").append(key).append(">");
                }

            }
        }

        return result.toString();
    }

    public int sendSms(String msisdn, String message, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        try {
            connection = getConnection("cm_pre");
            sql = "insert into mt values(mt_seq.nextval,1,?,?,sysdate,null,?,'PROCESS')";
            ps = connection.prepareStatement(sql);
            ps.setString(1, msisdn);
            ps.setString(2, message);
            ps.setString(3, channel);
            result = ps.executeUpdate();
            logger.info("End sendSms isdn " + msisdn + " message " + message + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR sendSms: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(msisdn)
                    .append(" message ")
                    .append(message)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public void sendSmsByList(String[] lstIsdn, String message) {
        for (String isdn : lstIsdn) {
            sendSms(isdn, message, "86952");
        }
    }

    public int resetToken(String staffCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp2");
            String sql = "update bockd.token set create_time = sysdate, last_request = sysdate where status = 1 and upper(user_name) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            result = ps.executeUpdate();
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR resetToken.");
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getTokenValue(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String tokenValue = "";
        String sqlMo = "select * from bockd.token where upper(user_name) = upper(?) and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbapp2");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, staffCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                tokenValue = rs1.getString("token_value");
                break;
            }
            logTimeDb("Time to getTokenValue: " + staffCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTokenValue " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return tokenValue;
    }

    public boolean checkSaleTransOrder(Long saleTransOrderId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select * from sm.sale_trans_order where sale_trans_order_id = ? and sale_trans_id = 0";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setLong(1, saleTransOrderId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = true;
                break;
            }
            logTimeDb("Time to checkSaleTransOrder, saleTransOrderId: " + saleTransOrderId + ", result: " + result, timeSt);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("ERROR checkSaleTransOrder, saleTransOrderId" + saleTransOrderId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }
}
