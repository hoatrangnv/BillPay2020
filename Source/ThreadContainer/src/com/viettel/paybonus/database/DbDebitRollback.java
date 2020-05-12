/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.*;
import com.viettel.threadfw.database.DbProcessorAbstract;
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
public class DbDebitRollback extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbDebitRollback.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbDebitRollback() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbDebitRollback(String sessionName, Logger logger) throws SQLException, Exception {
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
        EwalletDebitLog record = new EwalletDebitLog();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("ewallet_debit_log_id"));
            record.setIsdn(rs.getString("isdn"));
            record.setMoney(rs.getLong("money"));
            record.setStaffCode(rs.getString("staff_code"));
            record.setOtp(rs.getString("otp"));
            record.setRequestId(rs.getString("request_id"));
            record.setTotalSaleTrans(rs.getInt("total_sale_trans"));
            record.setListSaleTrans(rs.getString("list_sale_trans"));
            record.setLogTime(rs.getTimestamp("log_time"));
            record.setClearDebitType(rs.getLong("clear_debit_type"));
            record.setResultCode("0");
            record.setDescription("Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse MoRecord");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    public int updateEwalletDebitLog(String staffCode, long eWalletDebitLogId, String orgRequetsId, String desc) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update sm.ewallet_debit_log set response = ?, error_code = '98'"
                    + " where ewallet_debit_log_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, desc);
            ps.setLong(2, eWalletDebitLogId);
            result = ps.executeUpdate();
            logger.info("End updateEwalletDebitLog staffCode " + staffCode + " eWalletDebitLogId " + eWalletDebitLogId
                    + " orgRequetsId " + orgRequetsId
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateEwalletDebitLog: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" orgRequetsId ")
                    .append(orgRequetsId)
                    .append(" eWalletDebitLogId ")
                    .append(eWalletDebitLogId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int saveEwalletDebigLog(EwalletDebitLog newLog, String newRequestId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "INSERT INTO sm.ewallet_debit_log (EWALLET_DEBIT_LOG_ID,ISDN,MONEY,OTP,REQUEST_ID,STAFF_CODE,"
                    + " FUNCTION_NAME,URL,REQUEST,RESPONSE,LOG_TIME,DURATION,ERROR_CODE,DESCRIPTION,TOTAL_SALE_TRANS,LIST_SALE_TRANS) "
                    + "VALUES(ewallet_debit_log_seq.nextval,?,?,?,?,?,?,?,?,?,sysdate,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, newLog.getIsdn());
            ps.setLong(2, newLog.getMoney());
            ps.setString(3, newLog.getOtp());
            ps.setString(4, newRequestId);
            ps.setString(5, newLog.getStaffCode());
            ps.setString(6, newLog.getFunctionName());
            ps.setString(7, newLog.getUrl());
            ps.setString(8, newLog.getRequest());
            ps.setString(9, newLog.getRespone());
            ps.setLong(10, newLog.getDuration());
            ps.setString(11, newLog.getErrorCode());
            ps.setString(12, newLog.getDescription());
            ps.setInt(13, newLog.getTotalSaleTrans());
            ps.setString(14, newLog.getListSaleTrans());
            result = ps.executeUpdate();
            logger.info("End saveEwalletDebigLog staffCode " + newLog.getStaffCode()
                    + " orgRequestId " + newLog.getRequestId()
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR saveEwalletDebigLog: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(newLog.getStaffCode())
                    .append(" orgRequestId ")
                    .append(newLog.getRequestId())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String checkTransactions(Long transId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String transactionCode = "";//01: Sale_Trans Transaction | 02: Deposit Transaction | 03: Payment Transaction
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            String sql = "select * from sale_trans where sale_trans_date > '17-jul-2018' and sale_trans_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, transId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long cout = rs.getLong("sale_trans_id");
                if (cout > 0) {
                    transactionCode = "01";
                    break;
                }
            }
//            Check deposit transaction.
            if (transactionCode.isEmpty()) {
                closeResultSet(rs);
                closeStatement(ps);
                sql = "select * from deposit where create_date > '17-jul-2018' and deposit_id = ?";
                ps = connection.prepareStatement(sql);
                ps.setLong(1, transId);
                rs = ps.executeQuery();
                while (rs.next()) {
                    Long cout = rs.getLong("deposit_id");
                    if (cout > 0) {
                        transactionCode = "02";
                        break;
                    }
                }
            }
//            Check payment transaction.
            if (transactionCode.isEmpty()) {
                closeResultSet(rs);
                closeStatement(ps);
                sql = "select * from sm.v_payment_contract where create_date > '17-jul-2018' and payment_id = ?";
                ps = connection.prepareStatement(sql);
                ps.setLong(1, transId);
                rs = ps.executeQuery();
                while (rs.next()) {
                    Long cout = rs.getLong("payment_id");
                    if (cout > 0) {
                        transactionCode = "03";
                        break;
                    }
                }
            }

            logger.info("End checkTransactions transId " + transId
                    + " transactionCode " + transactionCode + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkTransactions ---- transId ").append(transId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return transactionCode;
        }
    }

    public int updateSaleTransClearDebit(String staffCode, String requestId, List<Long> saleTransIds, String transType) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            if ("01".equals(transType)) {
//                        Sale_trans_type 
                connection = getConnection(dbNameCofig);
                sql = "update sale_trans set clear_debit_status = 1, clear_debit_time = sysdate, clear_debit_user = ?,"
                        + "clear_debit_request_id = ? where sale_trans_date > '17-jul-2018' and sale_trans_id in ";
            } else if ("02".equals(transType)) {
//                        Deposit trans_type
                connection = getConnection(dbNameCofig);
                sql = "update deposit set clear_debit_status = 1, clear_debit_time = sysdate, clear_debit_user = ?,"
                        + "clear_debit_request_id = ? where create_date > '17-jul-2018' and deposit_id in ";
            } else if ("03".equals(transType)) {
//                        Payment trans_type
                connection = getConnection("db_payment");
                sql = "update payment_contract set clear_debit_status = 1, clear_debit_time = sysdate, clear_debit_user = ?,"
                        + "clear_debit_request_id = ? where create_date > '17-jul-2018' and payment_id in ";
            } else {
                connection = getConnection(dbNameCofig);
                sql = "update sale_emola_float set clear_debit_status = 1, clear_debit_time = sysdate, clear_debit_user = ?,"
                        + "clear_debit_request_id = ? where sale_trans_id in ";
            }

            StringBuilder parameterBuilder = new StringBuilder();
            parameterBuilder.append(" (");
            for (int i = 0; i < saleTransIds.size(); i++) {
                parameterBuilder.append("?");
                if (saleTransIds.size() > i + 1) {
                    parameterBuilder.append(",");
                }
            }
            parameterBuilder.append(")");
            ps = connection.prepareStatement(sql + parameterBuilder);
            ps.setString(1, staffCode.toUpperCase());
            ps.setString(2, requestId);
            for (int i = 1; i <= saleTransIds.size(); i++) {
                ps.setLong(i + 2, saleTransIds.get(i - 1));
            }
            result = ps.executeUpdate();

            logger.info("End updateSaleTransClearDebit staffCode " + staffCode + "and requestId: "
                    + requestId + "lstSaleTransId: " + saleTransIds + " transType " + transType
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            result = -1;
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSaleTransClearDebit: ").
                    append(sql).append("\n")
                    .append(" requestId ")
                    .append(requestId)
                    .append(" lstSaleTransId ")
                    .append(saleTransIds)
                    .append("transType")
                    .append(transType)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
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
    public int[] deleteQueue(List<Record> listRecords) {
        int[] res = new int[0];
        return res;
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

    public int sendSms(String msisdn, String message, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "INSERT INTO mt (mt_id,msisdn,message,mo_his_id,retry_num,receive_time,channel) "
                    + "VALUES(mt_SEQ.nextval,?,?,null,0,sysdate,?)";
            ps = connection.prepareStatement(sql);
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            ps.setString(1, msisdn.trim());
            ps.setString(2, message.trim());
            ps.setString(3, channel.trim());
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
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
}
