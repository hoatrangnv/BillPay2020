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
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import java.util.Date;
import java.util.ResourceBundle;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbCmpreProcessor extends DbProcessorAbstract {

    private String loggerLabel = DbCmpreProcessor.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbCmpreProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbCmPre");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbCmpreProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        Bonus record = new Bonus();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("sub_profile_id"));
            record.setActionAuditId(rs.getLong("action_audit_id"));
            record.setPkId(rs.getLong("sub_id"));
            record.setUserName(rs.getString("create_staff"));
            record.setActionProfileId(rs.getLong("sub_profile_id"));
            record.setReceiverDate(rs.getTimestamp("check_time"));
            record.setTimeCheck(rs.getTimestamp("check_time"));
            record.setShopCode(rs.getString("create_shop"));
            record.setActionCode(rs.getString("action_code"));
            record.setReasonId(rs.getLong("reason_id"));
            record.setIssueDateTime(rs.getTimestamp("create_time"));
            record.setCheckInfo(rs.getString("check_status"));
            record.setCheckComment(rs.getString("check_commend"));
            record.setStaffCheck(rs.getString("assigned_user"));
            record.setBonusStatus(rs.getString("BONUS_STATUS"));
            record.setIsdnCustomer(rs.getString("isdn"));
            record.setBts2G(rs.getString("bts_2g"));
            record.setBlockOcsHlr(rs.getLong("block_ocs_hlr"));
            record.setCustId(rs.getLong("cust_id"));
            record.setModifyType(rs.getString("modify_type"));
            record.setRequestEmola(rs.getString("request_emola"));
            record.setDaySentSms(rs.getLong("day_sent_sms"));
            record.setResultCode("0");
            record.setDescription("Start Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse SubProfileInfo");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        int[] res = new int[0];
        return res;
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
        return new int[0];
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }  

    public int sendSms(String msisdn, String message, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbSentSMS");
            sql = "INSERT INTO mt (mt_id,msisdn,message,mo_his_id,retry_num,receive_time,channel) "
                    + "VALUES(mt_SEQ.nextval,?,?,null,0,sysdate,?)";
            ps = connection.prepareStatement(sql);
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
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

//LamNT update Assign Staff check
    public long updateAssign(String assignStaff, Integer subProfileId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update sub_profile_info set assigned_user = ?, modify_type=3 where sub_profile_id = ? ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, assignStaff);
            ps.setInt(2, subProfileId);
            res = ps.executeUpdate();
            logger.info("End updateAssign where ID " + subProfileId + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateAssign ID " + subProfileId);
            logger.error(br + ex.toString());
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }
//End LamNT

//LamNT get user online
    public List<String> getUserOnline() {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        List<String> users = new ArrayList<String>();
        String user = null;
        String sql = " select distinct(spl.user_name) as name from cm_pre.sub_profile_log spl,cm_pre.sub_profile_role spr "
                + " where lower(spl.user_name)=lower(spr.user_name) and  spl.logout_time is null and spr.role_id = 0 ";
        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            ps = connection.prepareStatement(sql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                user = rs1.getString("name");
                users.add(user);
            }
        } catch (Exception ex) {
            logger.error("ERROR getUserOnline ");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(ps);
            closeConnection(connection);
        }
        return users;
    }
//End LamNT

//LamNT get config
    public List<Config> getConfig() {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs1 = null;
        List<Config> lst = new ArrayList<Config>();
        String sqlMo = "SELECT CONFIG_ID,VALUES_ID,VALUES_NAME FROM CONFIG_SMS_PROPERTY ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Config conf = new Config();
                conf.setConfigId(rs1.getLong("CONFIG_ID"));
                conf.setValuesId(rs1.getString("VALUES_ID"));
                conf.setValuesName(rs1.getString("VALUES_NAME"));
                lst.add(conf);
                continue;
            }
        } catch (Exception ex) {
            logger.error("ERROR getConfig ");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        return lst;
    }
//End LamNT
    
    public boolean checkAlreadyWarningInDay(String isdn, String msg) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbSentSMS");
            sql = "select msisdn from mt_his where sent_time > trunc(sysdate) and msisdn = ? and message = ?";
            ps = connection.prepareStatement(sql);
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            ps.setString(1, isdn.trim());
            ps.setString(2, msg.trim());
            rs = ps.executeQuery();
            while (rs.next()) {
                String id = rs.getString("msisdn");
                if (id != null && id.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkAlreadyWarningInDay isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkAlreadyWarningInDay: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" message ")
                    .append(msg)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkAlreadyWarningInDayQueue(String isdn, String msg) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbSentSMS");
            sql = "select msisdn from mt where msisdn = ? and message = ?";
            ps = connection.prepareStatement(sql);
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            ps.setString(1, isdn.trim());
            ps.setString(2, msg.trim());
            rs = ps.executeQuery();
            while (rs.next()) {
                String id = rs.getString("msisdn");
                if (id != null && id.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkAlreadyWarningInDayQueue isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkAlreadyWarningInDayQueue: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" message ")
                    .append(msg)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }
    
//LamNT update Assign Staff check
    public long updateSentSms(Integer daySentSms, Integer subProfileId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update sub_profile_info set day_sent_sms = ? where sub_profile_id = ? ";
            ps = connection.prepareStatement(sql);
            logger.info("Start updateSentSms isdn " + subProfileId);
            ps.setInt(1, daySentSms);
            ps.setInt(2, subProfileId);
            res = ps.executeUpdate();
            logger.info("End updateSentSms where ID " + subProfileId + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSentSms ID " + subProfileId);
            logger.error(br + ex.toString());
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }
//End LamNT

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        return new int[0];
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        return new int[0];
    }
    
    public int[] deleteQueueTimeout(List<String> listId) {
        return new int[0];
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
        StringBuilder sb = new StringBuilder();
        try {
//            The first delete queue timeout
            deleteQueueTimeout(ids);
//            Save history
            for (String sd : ids) {
                sb.append(": ").append(sd);
            }
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString() + " " + ex.toString());
        }
    }
}
