/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.*;
import com.viettel.threadfw.database.DbProcessorAbstract;
import static com.viettel.threadfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.PoolStore;
import java.io.PrintWriter;
import java.io.StringWriter;
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

public class DbPinCodeGetter extends DbProcessorAbstract {

    private String loggerLabel = DbPinCodeGetter.class.getSimpleName() + ": ";
    private String dbNameCofig;
    private String sqlDeleteMo = "update agent_order_pincode set process_time = sysdate, result_code = ?, description = ?, node_name = ?, cluster_name = ? where sale_trans_order_id = ?";

    public DbPinCodeGetter() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
    }

    public DbPinCodeGetter(String sessionName, Logger logger) throws SQLException, Exception {
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
                ps.setString(1, sd.getResultCode());
                ps.setString(2, sd.getDescription());
                ps.setString(3, sd.getNodeName());
                ps.setString(4, sd.getClusterName());
                ps.setLong(5, sd.getSaleTransOrderId());
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

                ps.setString(1, "FW_99");
                ps.setString(2, "FW_Timeout");
                ps.setString(3, "FW_Timeout");
                ps.setString(4, "FW_Timeout");
                ps.setLong(5, Long.valueOf(id));

                ps.addBatch();
                sf.append(id);
                sf.append(", ");
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueueTimeout PinCodeGetter listId " + sf.toString(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueueTimeout PinCodeGetter, listId " + sf.toString(), timeStart);
        }
    }

    public long getSaleTransId(Long saleTransOrderId, String tableName) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        long saleTransId = 0;
        String sqlMo = "select * from " + tableName + " where sale_trans_order_id = ?";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setLong(1, saleTransOrderId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                saleTransId = rs1.getLong("sale_trans_id");
                break;
            }
            logTimeDb("Time to getSaleTransId, saleTransOrderId: " + saleTransOrderId + ", saleTransId: " + saleTransId, timeSt);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("ERROR getSaleTransId, saleTransOrderId" + saleTransOrderId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return saleTransId;
    }

    public List<PinCode> getListSerialPinCode(long saleTransId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        List<PinCode> listPinCode = new ArrayList<PinCode>();

        String sqlMo = "select b.stock_model_code, a.quantity, a.from_serial, a.to_serial from sm.sale_trans_serial a , sm.stock_model b where a.sale_trans_date > '01-jul-2019' and a.stock_model_id = b.stock_model_id\n"
                + "and a.sale_trans_detail_id in (select sale_trans_detail_id from sm.sale_trans_detail where sale_trans_date > '01-jul-2019' and sale_trans_id = ?) order by a.from_serial asc";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setLong(1, saleTransId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String stockModelCode = rs1.getString("stock_model_code");
                Long quantity = rs1.getLong("quantity");
                String fromSerial = rs1.getString("from_serial");
                String toSerial = rs1.getString("to_serial");

                PinCode pincode = new PinCode();
                pincode.setStockModelCode(stockModelCode);
                pincode.setQuantity(quantity);
                pincode.setFromSerial(fromSerial);
                pincode.setToSerial(toSerial);

                listPinCode.add(pincode);

            }
            logTimeDb("Time to getListSerialPinCode, saleTransId: " + saleTransId + ", total record: " + listPinCode.size(), timeSt);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("ERROR getListSerialPinCode, saleTransId" + saleTransId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return listPinCode;
    }

    public ArrayList<PinCode> getPinCodeBySerial(String fromSerial, String toSerial, String tableName) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String sqlMo = " select a.serial, a.pincode, a.file_id, b.header from "
                + tableName + " a, pincode_file_info b  where a.file_id = b.file_id and to_number(a.serial) between to_number(?) and to_number(?) order by a.serial";
        PreparedStatement psMo = null;
        ArrayList<PinCode> listResult = new ArrayList<PinCode>();
        try {
            connection = getConnection("dbpincode");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setString(1, fromSerial);
            psMo.setString(2, toSerial);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String serial = rs1.getString("serial");
                String pincode = rs1.getString("pincode");
                long fileId = rs1.getLong("file_id");
                String header = rs1.getString("header");

                PinCode pinCode = new PinCode();
                pinCode.setSerial(serial);
                pinCode.setPincode(pincode);
                pinCode.setFileId(fileId);
                pinCode.setHeader(header);
                listResult.add(pinCode);
            }
            logTimeDb("Time to getPinCodeBySerial fromSerial " + fromSerial + " toSerial "
                    + toSerial + " tableName " + tableName, timeSt);
        } catch (Throwable ex) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error("ERROR getPinCodeBySerial fromSerial " + fromSerial + " toSerial "
                    + toSerial + " tableName " + tableName + " detail " + sw.toString());
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return listResult;
        }
    }

    public String getEmailPassword(Long receiverId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        StringBuilder result = new StringBuilder();
        String sqlMo = "select email_address,password_lv1, password_lv2, password_lv3 from sm.agent_order_info where agent_code = (select staff_code from sm.staff "
                + "where status = 1 and staff_id = ?)";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setLong(1, receiverId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String email = rs1.getString("email_address");
                String passLv1 = rs1.getString("password_lv1");
                String passLv2 = rs1.getString("password_lv2");
                String passLv3 = rs1.getString("password_lv3");
                if (email != null && email.length() > 0 && passLv1 != null && passLv1.length() > 0
                        && passLv2 != null && passLv2.length() > 0 && passLv3 != null && passLv3.length() > 0) {
                    result.append(email).append("|").append(passLv1).append("|").append(passLv2).append("|").append(passLv3);
                }

                break;
            }
            logTimeDb("Time to getEmailPassword, receiverId: " + receiverId + ", result: " + result.toString(), timeSt);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("ERROR getEmailPassword, receiverId" + receiverId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result.toString();
    }

    public String getAgentInfo(Long receiverId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        StringBuilder result = new StringBuilder();
        String sqlMo = "select  agent_code, name from sm.agent_order_info where agent_code = (select staff_code from sm.staff "
                + "where status = 1 and staff_id = ?)";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setLong(1, receiverId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String agentCode = rs1.getString("agent_code");
                String name = rs1.getString("name");
                if (agentCode != null && agentCode.length() > 0 && name != null && name.length() > 0) {
                    result.append(agentCode).append("|").append(name);
                }

                break;
            }
            logTimeDb("Time to getAgentInfo, receiverId: " + receiverId + ", result: " + result.toString(), timeSt);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("ERROR getAgentInfo, receiverId" + receiverId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result.toString();
    }

    public String getAmountTaxOfOrder(long saleTransId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String amountTax = "0";
        String sqlMo = "select TRIM(TO_CHAR(amount_tax, '999,999,999,999,999')) amount_tax from sm.sale_trans where sale_trans_date > '01-aug-2019' and sale_trans_id = ?";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setLong(1, saleTransId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                amountTax = rs1.getString("amount_tax");
                break;
            }
            logTimeDb("Time to getAmountTaxOfOrder: saleTransId" + saleTransId + ", amountTax: " + amountTax, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getAmountTaxOfOrder,saleTransId " + saleTransId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return amountTax;
    }

    public Long getSequence(String sequenceName, String dbName) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Long sequenceValue = null;
        String sqlMo = "select " + sequenceName + ".nextval as sequence from dual";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbName);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                sequenceValue = rs1.getLong("sequence");
            }
            logTimeDb("Time to getSequence: " + sequenceName, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSequence " + sequenceName);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return sequenceValue;
    }

    public String getTelByStaffCode(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String tel = null;
        String sqlMo = "select cellphone from vsa_v3.users where lower(user_name) = lower(?) and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, staffCode.toLowerCase());
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                tel = rs1.getString("cellphone");
            }
            if (tel == null) {
                tel = "";
                logger.info("tel is null - staff_code: " + staffCode);
            }
            logTimeDb("Time to getTelByStaffCode: " + staffCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTelByStaffCode " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return tel;
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
}
