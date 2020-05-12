/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.BounesMonthly;
import com.viettel.paybonus.obj.EwalletLog;
import com.viettel.paybonus.obj.FirstMobilePhone;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.threadfw.database.DbProcessorAbstract;
import static com.viettel.threadfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.DataResources;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbBounesMonthly extends DbProcessorAbstract {

    private String loggerLabel = DbBounesMonthly.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbBounesMonthly() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbBounesMonthly(String sessionName, Logger logger) throws SQLException, Exception {
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
        BounesMonthly record = new BounesMonthly();
        long timeSt = System.currentTimeMillis();
        try {
            record.setActionAuditId(rs.getLong("action_audit_id"));
            record.setIsdn(rs.getString("isdn"));
            record.setCreateTime(rs.getTimestamp("create_time"));
            record.setCreateStaff(rs.getString("CREATE_STAFF"));
            record.setCheckInfo(rs.getString("check_info"));
            record.seteMolaIsdn(rs.getString("EMOLA_ISDN"));
            record.setCountProcess(rs.getLong("count_process"));
            record.setImei(rs.getString("imei"));
            record.setResultCode("0");
            record.setDescription(rs.getString("description"));
            record.setDuration(rs.getLong("duration"));
            record.setBonusType(rs.getInt("bonus_Type"));
        } catch (Exception ex) {
            logger.error("ERROR parse BonusMonthly");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        return new int[0];
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        return new int[0];
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        return new int[0];
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
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

    public int inserTrparpu300His(String isdn, Date createTime) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = " INSERT INTO rp_arpu_300_his (ISDN,create_time) values (?,?) ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setDate(2, new java.sql.Date(createTime.getTime()));
            result = ps.executeUpdate();
            logger.info("End inserTrparpu300 isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR inserTrparpu300: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
    
    public long updateBounesMonthly(BounesMonthly bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "UPDATE bounes_monthly SET count_process = ? ,LAST_TIME = to_date(to_char(sysdate,'yyyyMMdd') || '235959','yyyyMMddhh24miss'),result_code = ?,description = ?,duration = ?  WHERE action_audit_id = ? ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getCountProcess() - 1);
            ps.setString(2, bn.getResultCode());
            ps.setString(3, bn.getDescription());
            ps.setLong(4, bn.getDuration());
            ps.setLong(5, bn.getActionAuditId());
            res = ps.executeUpdate();
            logger.info("End updateBounesMonthly id " + bn.getActionAuditId() + " isdn " + bn.getIsdn()
                    + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBounesMonthly id " + bn.getActionAuditId() + " isdn " + bn.getIsdn());
            logger.error(AppManager.logException(startTime, ex));
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }
    
    

    public long updateCheckInfo1BounesMonthly(BounesMonthly bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "UPDATE bounes_monthly SET count_process = ? ,LAST_TIME = to_date(to_char(sysdate,'yyyyMMdd') || '235959','yyyyMMddhh24miss'),result_code = ?,description = ?,duration = ?,check_info = '1'  WHERE action_audit_id = ? ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, 30);
            ps.setString(2, bn.getResultCode());
            ps.setString(3, bn.getDescription());
            ps.setLong(4, bn.getDuration());
            ps.setLong(5, bn.getActionAuditId());
            res = ps.executeUpdate();
            logger.info("End updateCheckInfoBounesMonthly id " + bn.getActionAuditId() + " isdn " + bn.getIsdn()
                    + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateCheckInfoBounesMonthly id " + bn.getActionAuditId() + " isdn " + bn.getIsdn());
            logger.error(AppManager.logException(startTime, ex));
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public long updateCheckInfo0BounesMonthly(BounesMonthly bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "UPDATE bounes_monthly SET count_process = ? ,LAST_TIME = to_date(to_char(sysdate,'yyyyMMdd') || '235959','yyyyMMddhh24miss'),result_code = ?,description = ?,duration = ?,check_info = '0'  WHERE action_audit_id = ? ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, 30);
            ps.setString(2, bn.getResultCode());
            ps.setString(3, bn.getDescription());
            ps.setLong(4, bn.getDuration());
            ps.setLong(5, bn.getActionAuditId());
            res = ps.executeUpdate();
            logger.info("End updateCheckInfoBounesMonthly id " + bn.getActionAuditId() + " isdn " + bn.getIsdn()
                    + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateCheckInfoBounesMonthly id " + bn.getActionAuditId() + " isdn " + bn.getIsdn());
            logger.error(AppManager.logException(startTime, ex));
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public int insertBounesMonthlyHis(BounesMonthly bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "INSERT INTO Bounes_Monthly_his (action_audit_id,isdn,create_time,emola_isdn,count_process,last_time,imei,result_code,description,duration) VALUES (?,?,sysdate,?,?,sysdate,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getActionAuditId());
            ps.setString(2, bn.getIsdn());
            ps.setString(3, bn.geteMolaIsdn());
            ps.setLong(4, bn.getCountProcess());
            ps.setString(5, bn.getImei());
            ps.setString(6, bn.getResultCode());
            ps.setString(7, bn.getDescription());
            ps.setLong(8, bn.getDuration());
            result = ps.executeUpdate();
            logger.info("End insertBounesMonthlyHis action_audit_id " + bn.getActionAuditId() + " isdn " + bn.getIsdn() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertBounesMonthlyHis: ").
                    append(sql).append("\n")
                    .append(" id ")
                    .append(" isdn ")
                    .append(bn.getIsdn())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public long updateBounesMonthlyByDays(BounesMonthly bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "UPDATE bounes_monthly SET count_process = ? ,LAST_TIME = sysdate  WHERE action_audit_id = ? ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, 2);
            ps.setLong(2, bn.getActionAuditId());
            res = ps.executeUpdate();
            logger.info("End updateBounesMonthly id " + bn.getActionAuditId() + " isdn " + bn.getIsdn()
                    + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBounesMonthly id " + bn.getActionAuditId() + " isdn " + bn.getIsdn());
            logger.error(AppManager.logException(startTime, ex));
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public long updateBounesMonthlyByDays(BounesMonthly bn, int days) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "UPDATE bounes_monthly SET count_process = ? ,LAST_TIME = sysdate  WHERE action_audit_id = ? ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, days);
            ps.setLong(2, bn.getActionAuditId());
            res = ps.executeUpdate();
            logger.info("End updateBounesMonthly id " + bn.getActionAuditId() + " isdn " + bn.getIsdn()
                    + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBounesMonthly id " + bn.getActionAuditId() + " isdn " + bn.getIsdn());
            logger.error(AppManager.logException(startTime, ex));
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public long updateBounesMonthlyOnlyTwoDays(BounesMonthly bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "UPDATE bounes_monthly SET count_process = ? ,LAST_TIME = sysdate  WHERE action_audit_id = ? ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, 2);
            ps.setLong(2, bn.getActionAuditId());
            res = ps.executeUpdate();
            logger.info("End updateBounesMonthly id " + bn.getActionAuditId() + " isdn " + bn.getIsdn()
                    + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBounesMonthly id " + bn.getActionAuditId() + " isdn " + bn.getIsdn());
            logger.error(AppManager.logException(startTime, ex));
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean checkAlreadyProcessRecord(long idRecord) {
        ParamList paramList = new ParamList();
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("action_audit_id", idRecord, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("action_audit_id", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "bonus_mobile_shop_his");
            while (rs.next()) {
                long id = rs.getLong("action_audit_id");
                if (id > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkAlreadyProcessRecord idRecord " + idRecord + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAlreadyProcessRecord defaul return false" + idRecord);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        }
        return result;
    }

    public boolean checkAlreadyBounesMonthly(long idRecord) {
        ParamList paramList = new ParamList();
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("action_audit_id", idRecord, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("count_process", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "bounes_monthly");
            while (rs.next()) {
                long id = rs.getLong("count_process");
                if (id > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkAlreadyProcessRecord idRecord " + idRecord + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAlreadyProcessRecord defaul return false" + idRecord);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        }
        return result;
    }

    public int[] deleteQueueTimeout(List<String> listId) {
        return new int[0];
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

    public int insertEwalletLog(EwalletLog log) {

        ParamList paramList = new ParamList();
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("ACTION_AUDIT_ID", Long.valueOf(log.getAtionAuditId()), Param.DataType.LONG, Param.IN));
            paramList.add(new Param("STAFF_CODE", log.getStaffCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("CHANNEL_TYPE_ID", log.getChannelTypeId(), Param.DataType.INT, Param.IN));
            paramList.add(new Param("MOBILE", log.getMobile(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("TRANS_ID", log.getTransId(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("ACTION_CODE", log.getActionCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("AMOUNT", log.getAmount(), Param.DataType.LONG, Param.IN));
            paramList.add(new Param("FUNCTION_NAME", log.getFunctionName(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("URL", log.getUrl(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("USERNAME", log.getUserName(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("REQUEST", log.getRequest(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("RESPONSE", log.getRespone(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("DURATION", log.getDuration(), Param.DataType.LONG, Param.IN));
            paramList.add(new Param("ERROR_CODE", log.getErrorCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("DESCRIPTION", log.getDescription(), Param.DataType.STRING, Param.IN));
            PoolStore.PoolResult prs = poolStore.insertTable(paramList, "EWALLET_LOG");
            logTimeDb("Time to insertEwalletLog isdn " + log.getMobile(), timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getMobile());
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public String checkNewHandset(BounesMonthly bn, String isdn, String to_imei) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        String res = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            connection = getConnection("mdm");
            ps = connection.prepareStatement("select msisdn from hlr_devices_change where datetime >= trunc(sysdate-180) and (from_imei = ? or to_imei = ?)  and msisdn = ?  ");
            ps.setString(1, to_imei);
            ps.setString(2, to_imei);
            ps.setString(3, isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                String rsimei = rs.getString("msisdn");
                res = rsimei;
            }
        } catch (Exception ex) {
            logger.error("ERROR checkNewHandset id " + bn.getID() + " isdn " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkNewHandset, id " + bn.getID() + " isdn " + bn.getIsdn(), timeStart);
            return res;
        }

    }

    public String checkCacheHandset(BounesMonthly bn, String isdn, String to_imei) {
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        String res = null;
        long timeStart = System.currentTimeMillis();
        try {
            connection = getConnection("mdm");
            ps = connection.prepareStatement(" select msisdn from mdm.hlr_subscriber_cache where msisdn = ? and imei = ?   ");
            if(!isdn.startsWith("258")){
                isdn = "258" + isdn;
            }
            ps.setString(1, isdn);
            ps.setString(2, to_imei);
            rs = ps.executeQuery();
            if (rs.next()) {
                String rsimei = rs.getString("msisdn");
                res = rsimei;
            }
        } catch (Exception ex) {
            logger.error("ERROR checkNewHandset id " + bn.getID() + " isdn " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkNewHandset, id " + bn.getID() + " isdn " + bn.getIsdn(), timeStart);
            return res;
        }
    }

    public boolean checkBonusCusToday(String isdn) {
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        boolean isCheck = false;
        long timeStart = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement(" select * from Bounes_Monthly_his where isdn = ? and last_time >=  TRUNC( sysdate )    ");
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                isCheck = true;
            }
        } catch (Exception ex) {
            logger.error("ERROR checkBonusCusToday  isdn " + isdn, ex);
            logger.error(AppManager.logException(timeStart, ex));
            isCheck = false;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkBonusCusToday, isdn " + isdn, timeStart);
            return isCheck;
        }
    }
   

    public int deleteBounesMonthly(BounesMonthly bn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        int res = 0;
        try {
            connection = connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement("delete Bounes_Monthly where action_audit_id = ?");
            ps.setLong(1, bn.getActionAuditId());
            res = ps.executeUpdate();
        } catch (SQLException ex) {
            logger.error("ERROR deleteBounesMonthly getActionAuditId " + bn.getActionAuditId() + " isdn " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeStart, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteBounesMonthly, getActionAuditId " + bn.getActionAuditId() + " isdn " + bn.getIsdn() + " result " + res, timeStart);
            return res;
        }
    }

    public String[] checkTopupPartner1(String isdn, String backDay) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        String[] res = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            connection = getConnection("recarg_aki");
            ps = connection.prepareStatement("SELECT   client, start_date, money "
                    + "  FROM   topup_log\n"
                    + " WHERE       start_date >= TRUNC (SYSDATE - ?)\n"
                    + "         AND start_date < TRUNC (SYSDATE - ?)\n"
                    + "         AND isdn = ?\n"
                    + "         AND result_code = '0' AND money >= 20");
            ps.setString(1, backDay);
            ps.setString(2, String.valueOf(Integer.valueOf(backDay) - 1));
            ps.setString(3, isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                String client = rs.getString("client");
                String amount = rs.getString("money");
                String refilDate = sdf.format(rs.getDate("start_date"));
                res = new String[]{client, amount, refilDate};
            }
        } catch (Exception ex) {
            logger.error("ERROR checkTopupPartner1 id " + " isdn " + isdn + " backDay " + backDay, ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkTopupPartner1, id " + " isdn " + isdn, timeStart);
            return res;
        }
    }

    public String[] checkTopupPartner2(String isdn, String backDay) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        String[] res = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            connection = getConnection("UTTM");
            ps = connection.prepareStatement("SELECT   client, start_date, money "
                    + "  FROM   topup_log\n"
                    + " WHERE       start_date >= TRUNC (SYSDATE - ?)\n"
                    + "         AND start_date < TRUNC (SYSDATE - ?)\n"
                    + "         AND isdn = ?\n"
                    + "         AND result_code = '0' AND money >= 20");
            ps.setString(1, backDay);
            ps.setString(2, String.valueOf(Integer.valueOf(backDay) - 1));
            ps.setString(3, "258" + isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                String client = rs.getString("client");
                String amount = rs.getString("money");
                String refilDate = sdf.format(rs.getDate("start_date"));
                res = new String[]{client, amount, refilDate};
            }
        } catch (Exception ex) {
            logger.error("ERROR checkTopupPartner2 id " + " isdn " + isdn + " backDay " + backDay, ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkTopupPartner2, id " + " isdn " + isdn, timeStart);
            return res;
        }
    }

//    public String[] checkScraftCardToDay(String isdn) {
//        long timeStart = System.currentTimeMillis();
//        PreparedStatement ps = null;
//        Connection connection = null;
//        ResultSet rs = null;
//        String[] res = null;
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//        try {
//            connection = getConnection("cm_pre");
//            ps = connection.prepareStatement("SELECT   seri_number, refill_amount, refill_date\n"
//                    + "  FROM   cm_pre.mc_scratch_history\n"
//                    + " WHERE       refill_date >= TRUNC (SYSDATE)\n"
//                    + "         AND refill_isdn = ? AND refill_amount >=20 ");
//            ps.setString(1, isdn);
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                String serial = rs.getString("seri_number");
//                String amount = rs.getString("refill_amount");
//                String refilDate = sdf.format(rs.getDate("refill_date"));
//                res = new String[]{serial, amount, refilDate};
//            }
//        } catch (Exception ex) {
//            logger.error("ERROR checkScraftCardToDay id " + " isdn " + isdn, ex);
//            logger.error(AppManager.logException(timeStart, ex));
//            res = null;
//        } finally {
//            closeResultSet(rs);
//            closeStatement(ps);
//            closeConnection(connection);
//            logTimeDb("Time to checkScraftCardToDay, id " + " isdn " + isdn, timeStart);
//            return res;
//        }
//    }
    
// Lamnt check scraft card today
    public int checkScraftCardMonth(String isdn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        int res = 0;
        try {
            connection = getConnection("cm_pre");
            ps = connection.prepareStatement("SELECT   sum(refill_amount) totalMoney "
                    + " FROM cm_pre.mc_scratch_history "
                    + " WHERE  refill_date >= TRUNC (SYSDATE -30) AND refill_isdn = ? ");
           ps.setString(1, isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                res = rs.getInt("totalMoney");
            }
        } catch (Exception ex) {
            logger.error("ERROR checkScraftCardToDay id " + " isdn " + isdn, ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = 0;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkScraftCardToDay, id " + " isdn " + isdn, timeStart);
            return res;
        }
    }
    
//    public String[] checkTopupToday(FirstMobilePhone bn) {
//        long timeStart = System.currentTimeMillis();
//        PreparedStatement ps = null;
//        Connection connection = null;
//        ResultSet rs = null;
//        String[] res = null;
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//        try {
//            connection = getConnection("appBccsGw");
//            ps = connection.prepareStatement("SELECT   client, start_date, money "
//                    + "  FROM   topup_log\n"
//                    + " WHERE       start_date >= TRUNC (SYSDATE)\n"
//                    + "         AND isdn = ?\n"
//                    + "         AND result_code = '0' AND money >= 20 ");
//            ps.setString(1, bn.getIsdn());
//            rs = ps.executeQuery();
//            if (rs.next()) {
//                String client = rs.getString("client");
//                String amount = rs.getString("money");
//                String refilDate = sdf.format(rs.getDate("start_date"));
//                res = new String[]{client, amount, refilDate};
//            }
//        } catch (Exception ex) {
//            logger.error("ERROR checkTopupToday id " + bn.getId() + " isdn " + bn.getIsdn(), ex);
//            logger.error(AppManager.logException(timeStart, ex));
//            res = null;
//        } finally {
//            closeResultSet(rs);
//            closeStatement(ps);
//            closeConnection(connection);
//            logTimeDb("Time to checkTopupToday, id " + bn.getId() + " isdn " + bn.getIsdn(), timeStart);
//            return res;
//        }
//    }
// Lamnt check the cao dien tu
    public int checkTopupMonth(String isdn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        int res = 0;
        try {
            connection = getConnection("appBccsGw");
            ps = connection.prepareStatement("SELECT   sum(money) totalMoney "
                    + "  FROM   topup_log WHERE  start_date >= TRUNC (SYSDATE -30) AND isdn = ? AND result_code = '0' ");
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                res = rs.getInt("totalMoney");
            }
        } catch (Exception ex) {
            logger.error("ERROR checkTopupToday id " + " isdn " + isdn, ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = 0;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkTopupToday, id " + " isdn " + isdn + " res " + res, timeStart);
            return res;
        }
    }

//    public String[] checkTopupTodayPartner1(String isdn) {
//        long timeStart = System.currentTimeMillis();
//        PreparedStatement ps = null;
//        Connection connection = null;
//        ResultSet rs = null;
//        String[] res = null;
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//        try {
//            connection = getConnection("recarg_aki");
//            ps = connection.prepareStatement("SELECT   client, start_date, money "
//                    + "  FROM   topup_log\n"
//                    + " WHERE       start_date >= TRUNC (SYSDATE)\n"
//                    + "         AND isdn = ?\n"
//                    + "         AND result_code = '0' AND money >= 20 ");
//            ps.setString(1, isdn);
//            rs = ps.executeQuery();
//            if (rs.next()) {
//                String client = rs.getString("client");
//                String amount = rs.getString("money");
//                String refilDate = sdf.format(rs.getDate("start_date"));
//                res = new String[]{client, amount, refilDate};
//            }
//        } catch (Exception ex) {
//            logger.error("ERROR checkTopupTodayPartner1 id " + " isdn " + isdn, ex);
//            logger.error(AppManager.logException(timeStart, ex));
//            res = null;
//        } finally {
//            closeResultSet(rs);
//            closeStatement(ps);
//            closeConnection(connection);
//            logTimeDb("Time to checkTopupTodayPartner1, id " + " isdn " + isdn, timeStart);
//            return res;
//        }
//    }

// Lamnt check topup card from partner1
    public int checkRecargAki(String isdn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        int res = 0;
        try {
            connection = getConnection("recarg_aki");
            ps = connection.prepareStatement("SELECT sum(money) totalMoney "
                    + " FROM   topup_log "
                    + " WHERE       start_date >= TRUNC (SYSDATE -30) AND isdn = ?  AND result_code = '0' ");
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                res = rs.getInt("totalMoney");
            }
        } catch (Exception ex) {
            logger.error("ERROR checkTopupTodayPartner1 id " + " isdn " + isdn, ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = 0;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkTopupTodayPartner1, id " + " isdn " + isdn, timeStart);
            return res;
        }
    }

//    public String[] checkTopupTodayPartner2(String isdn) {
//        long timeStart = System.currentTimeMillis();
//        PreparedStatement ps = null;
//        Connection connection = null;
//        ResultSet rs = null;
//        String[] res = null;
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//        try {
//            connection = getConnection("UTTM");
//            ps = connection.prepareStatement("SELECT   client, start_date, money "
//                    + "  FROM   topup_log\n"
//                    + " WHERE       start_date >= TRUNC (SYSDATE)\n"
//                    + "         AND isdn = ?\n"
//                    + "         AND result_code = '0' AND money >= 20 ");
//            ps.setString(1, "258" + isdn);
//            rs = ps.executeQuery();
//            if (rs.next()) {
//                String client = rs.getString("client");
//                String amount = rs.getString("money");
//                String refilDate = sdf.format(rs.getDate("start_date"));
//                res = new String[]{client, amount, refilDate};
//            }
//        } catch (Exception ex) {
//            logger.error("ERROR checkTopupTodayPartner2 id " + " isdn " + isdn, ex);
//            logger.error(AppManager.logException(timeStart, ex));
//            res = null;
//        } finally {
//            closeResultSet(rs);
//            closeStatement(ps);
//            closeConnection(connection);
//            logTimeDb("Time to checkTopupTodayPartner2, id " + " isdn " + isdn, timeStart);
//            return res;
//        }
//    }

// Lamnt check topup card from partner2
    public int checkUTTM(String isdn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        int res = 0;
        try {
            connection = getConnection("UTTM");
            ps = connection.prepareStatement("SELECT sum(money) totalMoney "
                    + " FROM   topup_log "
                    + " WHERE       start_date >= TRUNC (SYSDATE -30) AND isdn = ? AND result_code = '0' ");
            ps.setString(1, "258" + isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                res = rs.getInt("totalMoney");
            }
        } catch (Exception ex) {
            logger.error("ERROR checkTopupTodayPartner2 id " + " isdn " + isdn, ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = 0;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkTopupTodayPartner2, id " + " isdn " + isdn, timeStart);
            return res;
        }
    }
    
    public boolean checkUssd(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        String sqlGetCell = "select count(1) checkNumber from rp_arpu_300 where isdn=? ";
        try {
            connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlGetCell);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                long check = rs.getLong("checkNumber");
                if(check >= 1){
                    result = true;
                }
            }
        } catch (Exception ex) {
            logger.error("ERROR getCell staffcode ");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }
    
    public int inserTrparpu300(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = " INSERT INTO rp_arpu_300 (ISDN,create_time) values (?,sysdate) ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            result = ps.executeUpdate();
            logger.info("End inserTrparpu300 isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR inserTrparpu300: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
    
    public int deleteTrparu300ByIsdn(String isdn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        int res = 0;
        try {
            connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement("delete rp_arpu_300 where  isdn=? ");
            ps.setString(1, isdn);
            res = ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("ERROR deleteNewSubscriberPhone isdn " + isdn, ex);
            logger.error(AppManager.logException(timeStart, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteNewSubscriberPhone, isdn " + isdn + " result " + res, timeStart);
            return res;
        }
    }
}
