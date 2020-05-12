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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbPayBonusSecond extends DbProcessorAbstract {

    private String loggerLabel = DbPayBonusSecond.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbPayBonusSecond() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbPayBonusSecond(String sessionName, Logger logger) throws SQLException, Exception {
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
        BonusSecond record = new BonusSecond();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("action_audit_id"));
            record.setActionAuditId(rs.getLong("action_audit_id"));
            record.setIsdn(rs.getString("isdn"));
            record.setCreateTime(rs.getTimestamp("create_time"));
            record.setCreateStaff(rs.getString("CREATE_STAFF"));
            record.setCheckInfo(rs.getString("check_info"));
            record.seteMolaIsdn(rs.getString("EMOLA_ISDN"));
            record.setItemFeeId(rs.getLong("item_fee_id"));
            record.setFirstAmount(rs.getLong("first_amount"));
            record.setSecondAmount(rs.getLong("second_amount"));
            record.setCountProcess(rs.getLong("count_process"));
            record.setLastProcess(rs.getTimestamp("last_process"));
            record.setChannelTypeId(rs.getInt("CHANNEL_TYPE_ID"));
            record.setActionCode(rs.getString("ACTION_CODE"));
            record.setResultCode("0");
            record.setDescription("Start Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse SubProfileInfo");
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

    public int[] updateBonusSecond(List<BonusSecond> lstBn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int[] res = new int[0];
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "UPDATE bonus_second SET count_process = ?, last_process = sysdate WHERE action_audit_id = ? ";
            ps = connection.prepareStatement(sql);
            for (BonusSecond bn : lstBn) {
                ps.setLong(1, bn.getCountProcess());
                ps.setLong(2, bn.getActionAuditId());
                ps.addBatch();
            }
            res = ps.executeBatch();
            logger.info("End updatePayBonusSecond "
                    + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updatePayBonusSecond id ");
            logger.error(AppManager.logException(startTime, ex));
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
            paramList.add(new Param("date_process", "trunc(sysdate - 7)", Param.OperatorType.GREATER_EQUAL, Param.DataType.CONST, Param.IN));
            paramList.add(new Param("result_code", "0", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("action_audit_id", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "profile.bonus_second_his");
            int count = 0;
            while (rs.next()) {
                long id = rs.getLong("action_audit_id");
                if (id > 0) {
                    count++;
                    if (count >= 2) {
                        result = true;
                        break;
                    }
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

    public boolean checkAlreadyPayFirstTime(long idRecord) {
        ParamList paramList = new ParamList();
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("action_audit_id", idRecord, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("date_process", "trunc(sysdate - 7)", Param.OperatorType.GREATER_EQUAL, Param.DataType.CONST, Param.IN));
            paramList.add(new Param("result_code", "0", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("action_audit_id", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "profile.bonus_second_his");
            while (rs.next()) {
                long id = rs.getLong("action_audit_id");
                if (id > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkAlreadyPayFirstTime idRecord " + idRecord + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAlreadyPayFirstTime defaul return false" + idRecord);
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
            connection = getConnection("dbapp2");
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
            paramList.add(new Param("ACTION_AUDIT_ID", Long.valueOf("999" + log.getAtionAuditId()), Param.DataType.LONG, Param.IN));
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
            paramList.add(new Param("BONUS_TYPE", 2L, Param.DataType.LONG, Param.IN));
            PoolStore.PoolResult prs = poolStore.insertTable(paramList, "EWALLET_LOG");
            logTimeDb("Time to insertEwalletLog isdn " + log.getMobile(), timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getMobile());
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public int insertBonusSecondHis(BonusSecond bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "INSERT INTO bonus_second_his (ACTION_AUDIT_ID,ISDN,CREATE_STAFF,CHECK_INFO,EMOLA_ISDN,ITEM_FEE_ID,"
                    + "FIRST_AMOUNT,SECOND_AMOUNT, second_pay_card, result_code, "
                    + "ewallet_error_code, description, count_process, node_name, cluster_name, duration, channel_type_id, action_code,"
                    + "SECOND_PAY_TIME, create_time, PAY_TYPE) \n"
                    + " VALUES(?,?,?,?,?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getActionAuditId());
            ps.setString(2, bn.getIsdn());
            ps.setString(3, bn.getCreateStaff());
            ps.setString(4, bn.getCheckInfo());
            ps.setString(5, bn.geteMolaIsdn());
            ps.setLong(6, bn.getItemFeeId());
            ps.setLong(7, bn.getFirstAmount());
            ps.setLong(8, bn.getSecondAmount());
            ps.setString(9, bn.getSecondPayCard());
            ps.setString(10, bn.getResultCode());
            ps.setString(11, bn.geteWalletErrCode());
            ps.setString(12, bn.getDescription());
            ps.setLong(13, bn.getCountProcess());
            ps.setString(14, bn.getNodeName());
            ps.setString(15, bn.getClusterName());
            ps.setLong(16, bn.getDuration());
            ps.setInt(17, bn.getChannelTypeId());
            ps.setString(18, bn.getActionCode());
            if (bn.getSecondPayTime() == null) {
                ps.setTimestamp(19, null);
            } else {
                ps.setTimestamp(19, new Timestamp(bn.getSecondPayTime().getTime()));
            }
            ps.setTimestamp(20, new Timestamp(bn.getCreateTime().getTime()));
            ps.setInt(21, bn.getPayType());
            result = ps.executeUpdate();
            logger.info("End insertBonusSecondHis id " + bn.getId() + " isdn " + bn.getIsdn() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertBonusSecondHis: ").
                    append(sql).append("\n")
                    .append(" id ")
                    .append(bn.getId())
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

    public int deleteBonusSecond(BonusSecond bn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        int res = 0;
        try {
            connection = connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement("delete bonus_second where action_audit_id = ?");
            ps.setLong(1, bn.getId());
            res = ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("ERROR deleteBonusSecond id " + bn.getId() + " isdn " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeStart, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteBonusSecond, id " + bn.getId() + " isdn " + bn.getIsdn() + " result " + res, timeStart);
            return res;
        }
    }

    public String[] checkScraftCard(BonusSecond bn, String backDay) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        String[] res = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            connection = getConnection("cm_pre");
            ps = connection.prepareStatement("SELECT   seri_number, refill_amount, refill_date\n"
                    + "  FROM   cm_pre.mc_scratch_history\n"
                    + " WHERE       refill_date >= TRUNC (SYSDATE - ?)\n"
                    + "         AND refill_date < TRUNC (SYSDATE - ?)\n"
                    + "         AND refill_isdn = ?");
            ps.setString(1, backDay);
            ps.setString(2, String.valueOf(Integer.valueOf(backDay) - 1));
            ps.setString(3, bn.getIsdn());
            rs = ps.executeQuery();
            if (rs.next()) {
                String serial = rs.getString("seri_number");
                String amount = rs.getString("refill_amount");
                String refilDate = sdf.format(rs.getDate("refill_date"));
                res = new String[]{serial, amount, refilDate};
            }
        } catch (Exception ex) {
            logger.error("ERROR checkScraftCard id " + bn.getId() + " isdn " + bn.getIsdn() + " backDay " + backDay, ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkScraftCard, id " + bn.getId() + " isdn " + bn.getIsdn(), timeStart);
            return res;
        }
    }

    public String[] checkTopup(BonusSecond bn, String backDay) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        String[] res = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            connection = getConnection("appBccsGw");
            ps = connection.prepareStatement("SELECT   client, start_date, money "
                    + "  FROM   topup_log\n"
                    + " WHERE       start_date >= TRUNC (SYSDATE - ?)\n"
                    + "         AND start_date < TRUNC (SYSDATE - ?)\n"
                    + "         AND isdn = ?\n"
                    + "         AND result_code = '0' and money >= 10");
            ps.setString(1, backDay);
            ps.setString(2, String.valueOf(Integer.valueOf(backDay) - 1));
            ps.setString(3, bn.getIsdn());
            rs = ps.executeQuery();
            if (rs.next()) {
                String client = rs.getString("client");
                String amount = rs.getString("money");
                String refilDate = sdf.format(rs.getDate("start_date"));
                res = new String[]{client, amount, refilDate};
            }
        } catch (Exception ex) {
            logger.error("ERROR checkTopup id " + bn.getId() + " isdn " + bn.getIsdn() + " backDay " + backDay, ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkTopup, id " + bn.getId() + " isdn " + bn.getIsdn(), timeStart);
            return res;
        }
    }

    public String[] checkTopupUttm(BonusSecond bn, String backDay) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        String[] res = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            connection = getConnection("uttm");
            ps = connection.prepareStatement("SELECT   client, start_date, money "
                    + "  FROM   topup_log\n"
                    + " WHERE       start_date >= TRUNC (SYSDATE - ?)\n"
                    + "         AND start_date < TRUNC (SYSDATE - ?)\n"
                    + "         AND isdn = ?\n"
                    + "         AND result_code = '0' and money >= 10");
            ps.setString(1, backDay);
            ps.setString(2, String.valueOf(Integer.valueOf(backDay) - 1));
            ps.setString(3, bn.getIsdn());
            rs = ps.executeQuery();
            if (rs.next()) {
                String client = rs.getString("client");
                String amount = rs.getString("money");
                String refilDate = sdf.format(rs.getDate("start_date"));
                res = new String[]{client, amount, refilDate};
            }
        } catch (Exception ex) {
            logger.error("ERROR checkTopupUttm id " + bn.getId() + " isdn " + bn.getIsdn() + " backDay " + backDay, ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkTopupUttm, id " + bn.getId() + " isdn " + bn.getIsdn(), timeStart);
            return res;
        }
    }

    public String[] checkTopupRecargAki(BonusSecond bn, String backDay) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        String[] res = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            connection = getConnection("recargAki");
            ps = connection.prepareStatement("SELECT   client, start_date, money "
                    + "  FROM   topup_log\n"
                    + " WHERE       start_date >= TRUNC (SYSDATE - ?)\n"
                    + "         AND start_date < TRUNC (SYSDATE - ?)\n"
                    + "         AND isdn = ?\n"
                    + "         AND result_code = '0' and money >= 10");
            ps.setString(1, backDay);
            ps.setString(2, String.valueOf(Integer.valueOf(backDay) - 1));
            ps.setString(3, bn.getIsdn());
            rs = ps.executeQuery();
            if (rs.next()) {
                String client = rs.getString("client");
                String amount = rs.getString("money");
                String refilDate = sdf.format(rs.getDate("start_date"));
                res = new String[]{client, amount, refilDate};
            }
        } catch (Exception ex) {
            logger.error("ERROR checkTopupRecargAki id " + bn.getId() + " isdn " + bn.getIsdn() + " backDay " + backDay, ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkTopupRecargAki, id " + bn.getId() + " isdn " + bn.getIsdn(), timeStart);
            return res;
        }
    }

    public String[] checkScraftCardToDay(BonusSecond bn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        String[] res = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            connection = getConnection("cm_pre");
            ps = connection.prepareStatement("SELECT   seri_number, refill_amount, refill_date\n"
                    + "  FROM   cm_pre.mc_scratch_history\n"
                    + " WHERE       refill_date >= TRUNC (SYSDATE)\n"
                    + "         AND refill_isdn = ? and refill_amount >=10  order by refill_date asc");
            ps.setString(1, bn.getIsdn());
            rs = ps.executeQuery();
            if (rs.next()) {
                String serial = rs.getString("seri_number");
                String amount = rs.getString("refill_amount");
                String refilDate = sdf.format(rs.getDate("refill_date"));
                res = new String[]{serial, amount, refilDate};
            }
        } catch (Exception ex) {
            logger.error("ERROR checkScraftCardToDay id " + bn.getId() + " isdn " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkScraftCardToDay, id " + bn.getId() + " isdn " + bn.getIsdn(), timeStart);
            return res;
        }
    }

    public String[] checkTopupToday(BonusSecond bn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        String[] res = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            connection = getConnection("appBccsGw");
            ps = connection.prepareStatement("SELECT   client, start_date, money "
                    + "  FROM   topup_log\n"
                    + " WHERE       start_date >= TRUNC (SYSDATE)\n"
                    + "         AND (isdn = ? or substr(isdn,4) = ?)\n"
                    + "         AND result_code = '0' and money >= 10 order by start_date asc");
            String isdn;
            if (bn.getIsdn().startsWith("258")) {
                isdn = bn.getIsdn().substring(3);
            } else {
                isdn = bn.getIsdn();
            }
            ps.setString(1, isdn);
            ps.setString(2, isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                String client = rs.getString("client");
                String amount = rs.getString("money");
                String refilDate = sdf.format(rs.getDate("start_date"));
                res = new String[]{client, amount, refilDate};
            }
        } catch (Exception ex) {
            logger.error("ERROR checkTopupToday id " + bn.getId() + " isdn " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkTopupToday, id " + bn.getId() + " isdn " + bn.getIsdn(), timeStart);
            return res;
        }
    }

    public String[] checkTopupUttmToday(BonusSecond bn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        String[] res = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            connection = getConnection("uttm");
            ps = connection.prepareStatement("SELECT   client, start_date, money "
                    + "  FROM   topup_log\n"
                    + " WHERE       start_date >= TRUNC (SYSDATE)\n"
                    + "         AND (isdn = ? or substr(isdn,4) = ?)\n"
                    + "         AND result_code = '0' and money >= 10 order by start_date asc");
            String isdn;
            if (bn.getIsdn().startsWith("258")) {
                isdn = bn.getIsdn().substring(3);
            } else {
                isdn = bn.getIsdn();
            }
            ps.setString(1, isdn);
            ps.setString(2, isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                String client = rs.getString("client");
                String amount = rs.getString("money");
                String refilDate = sdf.format(rs.getDate("start_date"));
                res = new String[]{client, amount, refilDate};
            }
        } catch (Exception ex) {
            logger.error("ERROR checkTopupUttmToday id " + bn.getId() + " isdn " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkTopupUttmToday, id " + bn.getId() + " isdn " + bn.getIsdn(), timeStart);
            return res;
        }
    }

    public String[] checkTopupRecargAkiToday(BonusSecond bn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        String[] res = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            connection = getConnection("recargAki");
            ps = connection.prepareStatement("SELECT   client, start_date, money "
                    + "  FROM   topup_log\n"
                    + " WHERE       start_date >= TRUNC (SYSDATE)\n"
                    + "         AND (isdn = ? or substr(isdn,4) = ?)\n"
                    + "         AND result_code = '0' and money >= 10 order by start_date asc");
            String isdn;
            if (bn.getIsdn().startsWith("258")) {
                isdn = bn.getIsdn().substring(3);
            } else {
                isdn = bn.getIsdn();
            }
            ps.setString(1, isdn);
            ps.setString(2, isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                String client = rs.getString("client");
                String amount = rs.getString("money");
                String refilDate = sdf.format(rs.getDate("start_date"));
                res = new String[]{client, amount, refilDate};
            }
        } catch (Exception ex) {
            logger.error("ERROR checkTopupRecargAkiToday id " + bn.getId() + " isdn " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            res = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to checkTopupRecargAkiToday, id " + bn.getId() + " isdn " + bn.getIsdn(), timeStart);
            return res;
        }
    }

    public long updateBonusProcesSecond(BonusSecond bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "UPDATE bonus_process SET second_pay_amount = ?, second_pay_time = sysdate, second_pay_card = ? "
                    + " WHERE date_process > trunc(sysdate-60) and action_audit_id = ? ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getSecondAmount());
            ps.setString(2, bn.getSecondPayCard());
            ps.setLong(3, bn.getActionAuditId());
            res = ps.executeUpdate();
            logger.info("End updateBonusProces id " + bn.getActionAuditId() + " isdn " + bn.getIsdn() + " res " + res
                    + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBonusProces id " + bn.getActionAuditId() + " isdn " + bn.getIsdn());
            logger.error(AppManager.logException(startTime, ex));
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public long updateBonusProcesFirst(BonusSecond bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "UPDATE bonus_process SET result_code='0', description = 'pay bonus fist time success', "
                    + "ewallet_error_code = '01', amount = ?, "
                    + "total_current_value = total_current_value + ?, total_current_times = total_current_times + 1 "
                    + " WHERE date_process > trunc(sysdate-60) and action_audit_id = ? ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getSecondAmount());
            ps.setString(2, bn.getSecondPayCard());
            ps.setLong(3, bn.getActionAuditId());
            res = ps.executeUpdate();
            logger.info("End updateBonusProces id " + bn.getActionAuditId() + " isdn " + bn.getIsdn() + " res " + res
                    + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBonusProces id " + bn.getActionAuditId() + " isdn " + bn.getIsdn());
            logger.error(AppManager.logException(startTime, ex));
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public int updateSubMbWhenActive(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update cm_pre.sub_mb set act_status = '00', sta_datetime = sysdate where isdn = ? and status = '2'";
            ps = connection.prepareStatement(sql);
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            ps.setString(1, isdn);
            result = ps.executeUpdate();
            logger.info("End updateSubMbWhenActive isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSubMbWhenActive: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateSubMbBackIdle(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update cm_pre.sub_mb set act_status = '03', sta_datetime = null where isdn = ? and status = '2'";
            ps = connection.prepareStatement(sql);
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            ps.setString(1, isdn);
            result = ps.executeUpdate();
            logger.info("End updateSubMbBackIdle isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSubMbBackIdle: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getSerialByIsdnCustomer(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String serial = null;
        String sqlMo = " select serial from cm_pre.sub_mb where status = 2 and isdn = ? ";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                serial = rs1.getString("serial");
            }
            logTimeDb("Time to getSerialByIsdnCustomer: " + isdn, timeSt);
            if (serial == null) {
                serial = "";
            }
        } catch (Exception ex) {
            logger.error("ERROR getSerialByIsdnCustomer " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            serial = "";
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return serial;
    }

    public boolean checkUserCodeOfBranch(String staffCode, Long shopId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        long transId = 0;
        try {
            connection = getConnection("dbsm");
            sql = "select * from staff where lower(staff_code) = lower(?) and status = 1 and shop_Id in (select shop_Id from Shop where shop_Path like '%" + shopId + "%')";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                transId = rs.getLong("staff_id");
                if (transId > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkUserCodeOfBranch staffCode " + staffCode
                    + " result " + result + " transId " + transId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkUserCodeOfBranch: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
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
}
