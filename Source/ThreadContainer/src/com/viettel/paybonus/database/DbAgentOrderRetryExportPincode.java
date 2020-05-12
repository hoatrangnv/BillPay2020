/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.PinCode;
import com.viettel.threadfw.manager.AppManager;

import com.viettel.threadfw.database.DbProcessorAbstract;
import com.viettel.vas.util.ConnectionPoolManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.log4j.Logger;
import java.util.Date;

public class DbAgentOrderRetryExportPincode extends DbProcessorAbstract {

    private String loggerLabel = DbAgentOrderRetryExportPincode.class.getSimpleName() + ": ";
    private String sqlDeleteMo = "update agent_order_pincode set status = ?, result_export = ?, description_export = ?, export_time = sysdate where sale_trans_order_id = ?";

    public DbAgentOrderRetryExportPincode() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
    }

    public DbAgentOrderRetryExportPincode(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
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
            int[] result = ps.executeBatch();
            if (result.length > 0) {
                for (Record rc : listRecords) {
                    PinCode sd = (PinCode) rc;
                    if ("0".equals(sd.getResultCode())) {
                        resendEmail(sd.getSaleTransOrderId());
                    }
                }

            }
            return result;
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

    public boolean checkTransactionAutoExported(Long saleTransOrderId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select sale_trans_id from sm.sale_trans_order where sale_trans_date > trunc(sysdate-7) and sale_trans_order_id = ? and sale_trans_id > 0\n"
                + "union \n"
                + "select sale_trans_id from sm.agent_trans_order_his where sale_trans_date > trunc(sysdate-7) and sale_trans_order_id = ? and sale_trans_id > 0";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setLong(1, saleTransOrderId);
            psMo.setLong(2, saleTransOrderId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = true;
                break;
            }
            logTimeDb("Time to checkTransactionAutoExported, saleTransOrderId: " + saleTransOrderId + ", result: " + result, timeSt);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("ERROR checkTransactionAutoExported, saleTransOrderId" + saleTransOrderId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
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

    public int resendEmail(Long saleTransOrderId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "update sm.agent_order_pincode set status = 1, process_time = null, result_code = null, description = null where sale_trans_order_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransOrderId);
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
}
