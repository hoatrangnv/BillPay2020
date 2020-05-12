/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.*;
import com.viettel.threadfw.database.DbProcessorAbstract;
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
public class DbFixBroadband extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbFixBroadband.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbFixBroadband() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbFixBroadband(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
        dbNameCofig = sessionName;
        poolStore = new PoolStore(sessionName, logger);
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
        SubAdslLLPrepaid record = new SubAdslLLPrepaid();
        long timeSt = System.currentTimeMillis();
        try {
            record.setPrepaidType(rs.getLong("prepaid_type"));
            record.setId(rs.getLong("sub_adsl_ll_prepaid_id"));
            record.setSubAdslLlPrepaidId(rs.getLong("sub_adsl_ll_prepaid_id"));
            record.setSubId(rs.getLong("sub_id"));
            record.setContractId(rs.getLong("contract_id"));
            record.setAccount(rs.getString("account"));
            record.setNewProductCode(rs.getString("new_product_code"));
            record.setCreateTime(rs.getTimestamp("create_time"));
            record.setExpireTime(rs.getTimestamp("expire_time"));
            record.setCreateUser(rs.getString("create_user"));
            record.setCreateShop(rs.getString("create_shop"));
            record.setSaleTransId(rs.getLong("sale_trans_id"));
            record.setWarningCount(rs.getLong("warning_count"));
            record.setBlockTime(rs.getTimestamp("block_time"));
            record.setResultCode("0");
            record.setDescription("Start to process blocking sub_adsl_ll_prepaid");
        } catch (Exception ex) {
            logger.error("ERROR parse sub_adsl_ll_prepaid");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
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

    public int sendSms(String msisdn, String message, String channel, long moHisId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("db_payment"); //using this to send mcell, vodacom
            sql = "INSERT INTO mt (mt_id,msisdn,message,mo_his_id,retry_num,receive_time,channel) "
                    + "VALUES(mt_SEQ.nextval,?,?,?,0,sysdate,?)";
            ps = connection.prepareStatement(sql);
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            ps.setString(1, msisdn.trim());
            ps.setString(2, message.trim());
            ps.setLong(3, moHisId);
            ps.setString(4, channel.trim());
            result = ps.executeUpdate();
            logger.info("End sendSms isdn " + msisdn + " message " + message + " moHisId " + moHisId + " result " + result + " time "
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
                    .append(" moHisId ")
                    .append(moHisId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getPhoneOfContract(long contractId, String account) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String result = "";
        try {
            connection = ConnectionPoolManager.getConnection("cm_pos");
            ps = connection.prepareStatement("select tel_fax from contract where contract_id = ? and status = 2");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setLong(1, contractId);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getString("tel_fax");
                break;
            }
            logTimeDb("Time to getPhoneOfContract contractId " + contractId + " account " + account + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getPhoneOfContract defaul return empty, contractId " + contractId + " account " + account);
            logger.error(AppManager.logException(timeSt, ex));
            result = "";
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getRefOfContract(long contractId, String account) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String result = "";
        try {
            connection = ConnectionPoolManager.getConnection("cm_pos");
            ps = connection.prepareStatement("select reference_id from contract where contract_id = ? and status = 2");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setLong(1, contractId);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getString("reference_id");
                break;
            }
            logTimeDb("Time to getRefOfContract contractId " + contractId + " account " + account + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getRefOfContract defaul return empty, contractId " + contractId + " account " + account);
            logger.error(AppManager.logException(timeSt, ex));
            result = "";
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getCenter(String account) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String result = "";
        try {
            connection = ConnectionPoolManager.getConnection("cm_pos");
            ps = connection.prepareStatement("select cen_code from area where area_code = (select address_code from sub_adsl_ll where account = ? and status = 2)");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setString(1, account);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getString("cen_code");
                break;
            }
            logTimeDb("Time to getCenter account " + account + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getCenter defaul return empty, account " + account);
            logger.error(AppManager.logException(timeSt, ex));
            result = "";
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateWarning(long prepaidId, String account) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pos");
            sql = "update  sub_adsl_ll_prepaid set warning_count = warning_count + 1, LAST_WARNING_TIME = sysdate where sub_adsl_ll_prepaid_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, prepaidId);
            result = ps.executeUpdate();
            logger.info("End updateWarning prepaidId " + prepaidId + " account " + account + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateWarning: ").
                    append(sql).append("\n")
                    .append(" prepaidId ")
                    .append(prepaidId)
                    .append(" account ")
                    .append(account)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateBlock(long prepaidId, String account) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pos");
            sql = "update  sub_adsl_ll_prepaid set block_time = sysdate where sub_adsl_ll_prepaid_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, prepaidId);
            result = ps.executeUpdate();
            logger.info("End updateBlock prepaidId " + prepaidId + " account " + account + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBlock: ").
                    append(sql).append("\n")
                    .append(" prepaidId ")
                    .append(prepaidId)
                    .append(" account ")
                    .append(account)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateSubAdslLl(long prepaidId, String account) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pos");
            sql = "update sub_adsl_ll set act_status = '100' where account = ? and status = 2";
            ps = connection.prepareStatement(sql);
            ps.setString(1, account);
            result = ps.executeUpdate();
            logger.info("End updateSubAdslLl prepaidId " + prepaidId + " account " + account + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSubAdslLl: ").
                    append(sql).append("\n")
                    .append(" prepaidId ")
                    .append(prepaidId)
                    .append(" account ")
                    .append(account)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertActionAudit(long prepaidId, String account, String des, long subId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pos");
            sql = "INSERT INTO action_audit (ACTION_AUDIT_ID,ISSUE_DATETIME,ACTION_CODE,REASON_ID,SHOP_CODE,USER_NAME,"
                    + " PK_TYPE,PK_ID,IP,DESCRIPTION) "
                    + " VALUES(action_audit_seq.nextval,sysdate,'06',299089,'ITBILLING','ITBILLING', "
                    + "'3',?,'127.0.0.1',?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, subId);
            ps.setString(2, des);
            result = ps.executeUpdate();
            logger.info("End insertActionAudit prepaidId " + prepaidId + " account " + account
                    + " subId " + subId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionAudit: ").
                    append(sql).append("\n")
                    .append(" prepaidId ")
                    .append(prepaidId)
                    .append(" account ")
                    .append(account)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateBlockedTimeFtthMobilePckg(String account) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pos");
            sql = "update cm_pos.sub_mb_ftth set blocked_time = sysdate where account  = ? and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, account);
            result = ps.executeUpdate();
            logger.info("End updateBlockedTimeFtthMobilePckg account " + account + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBlockedTimeFtthMobilePckg: ").
                    append(sql).append("\n")
                    .append(" account ")
                    .append(account)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateContract(long contractId, String referenceId) {
        int result = 0;
        long timeSt = System.currentTimeMillis();
        PreparedStatement ps = null;
        String sqlGetUser = "update cm_pos.contract set reference_id = ? "
                + " where contract_id = ?";
        Connection connection = null;
        try {
            connection = getConnection("cm_pos");
            ps = connection.prepareStatement(sqlGetUser);
            ps.setString(1, referenceId);
            ps.setLong(2, contractId);
            result = ps.executeUpdate();
            logTimeDb("Time to updateContract contractId " + contractId
                    + " referenceId " + referenceId + " result " + result, timeSt);
        } catch (Exception ex) {
            logger.error("Error updateContract contractId " + contractId + " referenceId " + referenceId, ex);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
}
