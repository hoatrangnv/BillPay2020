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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.util.Date;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbCheckActiveScratchCard extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbCheckActiveScratchCard.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbCheckActiveScratchCard() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbCheckActiveScratchCard(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
        dbNameCofig = sessionName;
        poolStore = new PoolStore(dbNameCofig, logger);
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
        VcRequest record = new VcRequest();
        long timeSt = System.currentTimeMillis();
        try {
            record.setRequestId(rs.getLong("request_id"));
            record.setStatus(rs.getLong("status"));
            record.setUserId(rs.getString("user_id"));
            record.setFromSerial(rs.getString("from_serial"));
            record.setToSerial(rs.getString("to_serial"));
            record.setShopId(rs.getLong("shop_id"));
            record.setStaffId(rs.getLong("staff_id"));
            record.setSaleTransOrderId(rs.getLong("sale_trans_order_id"));
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
        int[] res = new int[0];
        return res;
    }

    public String getListIsdnReceiveWarningOrder(String shopId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String channelCode = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from agent_receive_sms where shop_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, shopId);
            rs = ps.executeQuery();
            while (rs.next()) {
                channelCode = rs.getString("list_isdn");
            }
            logger.info("End getListIsdnReceiveWarningOrder shopId " + shopId + "result: " + channelCode + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getListIsdnReceiveWarningOrder: ").
                    append(sql).append("\n")
                    .append(" shopId ")
                    .append(shopId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return channelCode;
        }
    }

    public String getAmountTaxOfOrder(long saleTransOrderId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String amountTax = "0";
        String sqlMo = "select TRIM(TO_CHAR(amount_tax, '999,999,999,999,999')) amount_tax from sm.agent_trans_order_his where sale_trans_order_id = ?";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setLong(1, saleTransOrderId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                amountTax = rs1.getString("amount_tax");
                break;
            }
            logTimeDb("Time to getAmountTaxOfOrder: saleTransOrderId" + saleTransOrderId + ", amountTax: " + amountTax, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getAmountTaxOfOrder,saleTransOrderId " + saleTransOrderId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return amountTax;
    }

    public String getReceiverCode(long saleTransOrderId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String staffCode = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.staff where staff_id = (select staff_id from sm.staff \n"
                    + "where staff_id = (select receiver_id from sm.agent_trans_order_his where sale_trans_order_id = ? and rownum < 2))";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransOrderId);
            rs = ps.executeQuery();
            while (rs.next()) {
                staffCode = rs.getString("staff_code");
            }
            logger.info("End getReceiverCode staffCode " + staffCode + " saleTransOrderId " + saleTransOrderId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getAgentOwnerCode: ").
                    append(sql).append("\n")
                    .append(" saleTransOrderId ")
                    .append(saleTransOrderId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return staffCode;
        }
    }

    public String getAgentMakeOrder(long saleTransOrderId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String staffCode = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.staff where staff_id = (select staff_id from sm.staff \n"
                    + "where staff_id = (select create_staff_id from sm.agent_trans_order_his where sale_trans_order_id = ? and rownum < 2))";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransOrderId);
            rs = ps.executeQuery();
            while (rs.next()) {
                staffCode = rs.getString("staff_code");
            }
            logger.info("End getAgentMakeOrder staffCode " + staffCode + " saleTransOrderId " + saleTransOrderId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getAgentMakeOrder: ").
                    append(sql).append("\n")
                    .append(" saleTransOrderId ")
                    .append(saleTransOrderId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return staffCode;
        }
    }

    public String getNameOfAgentCode(String agentCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String name = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from agent_order_info where upper(agent_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, agentCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                name = rs.getString("name");
            }
            logger.info("End getNameOfAgentCode agentCode " + agentCode + " name " + name + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getAgentOwnerCode: ").
                    append(sql).append("\n")
                    .append(" agentCode ")
                    .append(agentCode);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return name;
        }
    }

    public String getPriceOfScratchCard(String serial) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String price = "0";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select TRIM(TO_CHAR(price, '999,999,999,999,999')) price from sm.price "
                    + "where stock_model_id = (select stock_model_id from sm.stock_card where serial = to_number(?)) \n"
                    + "and status = 1 and trunc(sysdate) > sta_date \n"
                    + "and (end_date is null or trunc(sysdate) < end_date) and type = 9 and price_policy = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            rs = ps.executeQuery();
            while (rs.next()) {
                price = rs.getString("price");
            }
            logger.info("End getPriceOfScratchCard serial " + serial + " price " + price + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getPriceOfScratchCard: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return price;
        }
    }

    public boolean checkSaleScratchCard(String serial) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.stock_card where serial = to_number(?) and status = 0";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkSaleScratchCard serial " + serial + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkSaleScratchCard: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkExistAgentOrderActiveDuration(String fromSerial, String toSerial, long saleTransOrderId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from agent_order_active_duration where from_serial = ? and to_serial = ? and sale_trans_order_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, fromSerial);
            ps.setString(2, toSerial);
            ps.setLong(3, saleTransOrderId);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkExistAgentOrderActiveDuration fromSerial: " + fromSerial + ", saleTransOrderId " + saleTransOrderId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkExistAgentOrderActiveDuration: ").
                    append(sql).append("\n")
                    .append(" saleTransOrderId ")
                    .append(saleTransOrderId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateStatusAgentOrderDuration(double duration, int status, String fromSerial, String toSerial, long saleTransOrderId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update agent_order_active_duration set duration = nvl(duration,0) + ?, status = ?, last_update_time = sysdate where from_serial = ? \n"
                    + "and to_serial = ? and sale_trans_order_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setDouble(1, duration);
            ps.setInt(2, status);
            ps.setString(3, fromSerial);
            ps.setString(4, toSerial);
            ps.setLong(5, saleTransOrderId);

            result = ps.executeUpdate();
            logger.info("End updateStatusAgentOrderDuration fromSerial " + fromSerial + " toSerial " + toSerial + ", status: " + status
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateStatusAgentOrderDuration: ").
                    append(sql).append("\n")
                    .append(" fromSerial ")
                    .append(fromSerial)
                    .append(" toSerial ")
                    .append(toSerial);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertAgentOrderDuration(int status, String fromSerial, String toSerial, long saleTransOrderId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO agent_order_active_duration (SALE_TRANS_ORDER_ID,FROM_SERIAL,TO_SERIAL,DURATION,RECEIVER_ID,STATUS,CREATE_TIME) \n"
                    + "VALUES(?,?,?,null,(select receiver_id from sm.agent_trans_order_his where sale_trans_order_id = ? and rownum < 2),?,sysdate)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransOrderId);
            ps.setString(2, fromSerial);
            ps.setString(3, toSerial);
            ps.setLong(4, saleTransOrderId);
            ps.setInt(5, status);
            result = ps.executeUpdate();
            logger.info("End insertAgentOrderDuration fromSerial " + fromSerial
                    + " toSerial " + toSerial
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionLog: ").
                    append(sql).append("\n")
                    .append(" fromSerial ")
                    .append(fromSerial)
                    .append(" toSerial ")
                    .append(toSerial);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public long countTotalRangeSerial(long saleTransOrderId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        long total = 0;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select count(1) as total from sm.agent_order_vc_request where sale_trans_order_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransOrderId);
            rs = ps.executeQuery();
            while (rs.next()) {
                total = rs.getLong("total");
            }
            logger.info("End countTotalRangeSerial saleTransOrderId " + saleTransOrderId + " total " + total + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR countTotalRangeSerial: ").
                    append(sql).append("\n")
                    .append(" saleTransOrderId ")
                    .append(saleTransOrderId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return total;
        }
    }

    public long countCurrentRangeSerialActive(long saleTransOrderId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        long total = 0;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select count(1) as total from agent_order_active_duration where sale_trans_order_id = ? and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransOrderId);
            rs = ps.executeQuery();
            while (rs.next()) {
                total = rs.getLong("total");
            }
            logger.info("End countCurrentRangeSerialActive saleTransOrderId " + saleTransOrderId + " total " + total + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR countCurrentRangeSerialActive: ").
                    append(sql).append("\n")
                    .append(" saleTransOrderId ")
                    .append(saleTransOrderId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return total;
        }
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
        String sqlMo = " select cellphone from vsa_v3.users where user_name = ? and status = 1";
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
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
        }
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int[] deleteQueueTimeout(List<String> listId) {
        int[] res = new int[0];
        return res;
    }

//    public int sendSms(String msisdn, String message, String channel) {
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder br = new StringBuilder();
//        String sql = "";
//        int result = 0;
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("dbapp2");
//            sql = "INSERT INTO mt (mt_id,msisdn,message,mo_his_id,retry_num,receive_time,channel) "
//                    + "VALUES(mt_SEQ.nextval,?,?,null,0,sysdate,?)";
//            ps = connection.prepareStatement(sql);
//            if (!msisdn.startsWith("258")) {
//                msisdn = "258" + msisdn;
//            }
//            ps.setString(1, msisdn);
//            ps.setString(2, message);
//            ps.setString(3, channel);
//            result = ps.executeUpdate();
//            logger.info("End sendSms isdn " + msisdn + " message " + message + " result " + result + " time "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).
//                    append("\nERROR sendSms: ").
//                    append(sql).append("\n")
//                    .append(" isdn ")
//                    .append(msisdn)
//                    .append(" message ")
//                    .append(message)
//                    .append(" result ")
//                    .append(result);
//            logger.error(br + ex.toString());
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            return result;
//        }
//    }
    public int sendSmsV2(String msisdn, String message, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into mt values(mt_seq.nextval,1,?,?,sysdate,null,?,'PROCESS')";
            ps = connection.prepareStatement(sql);
            ps.setString(1, msisdn);
            ps.setString(2, message);
            ps.setString(3, channel);
            result = ps.executeUpdate();
            logger.info("End sendSmsV2 isdn " + msisdn + " message " + message + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR sendSmsV2: ").
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
