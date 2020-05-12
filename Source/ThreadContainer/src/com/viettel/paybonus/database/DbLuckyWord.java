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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbLuckyWord extends DbProcessorAbstract {

    private String loggerLabel = DbLuckyWord.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public DbLuckyWord() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbluckyword";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbLuckyWord(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
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
        LuckyWordSub record = new LuckyWordSub();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("luckyword_sub_id"));
            record.setLuckyWordSubId(rs.getLong("luckyword_sub_id"));
            record.setMsisdn(rs.getString("msisdn"));
            record.setResultCode("0");
            record.setDescription("Processing");
            record.setMoney("");
        } catch (Exception ex) {
            logger.error("ERROR parse MoRecord");
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
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                LuckyWordSub sd = (LuckyWordSub) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("luckyword_charge_id", "luckyword_charge_seq.nextval", Param.DataType.CONST, Param.IN));
                paramList.add(new Param("luckyword_sub_id", sd.getLuckyWordSubId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("msisdn", sd.getMsisdn(), Param.DataType.STRING, Param.IN));
                if (sd.getMoney() == null) {
                    paramList.add(new Param("money", "", Param.DataType.STRING, Param.IN));
                } else {
                    paramList.add(new Param("money", sd.getMoney(), Param.DataType.STRING, Param.IN));
                }
                paramList.add(new Param("CLUSTER_NAME", sd.getClusterName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("NODE_NAME", sd.getNodeName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("ERROR_CODE", sd.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("DESCRIPTION", sd.getDescription(), Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "luckyword_charge");
            logTimeDb("Time to insertQueueHis luckyword_charge, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis luckyword_charge batchid " + batchId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        }
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        return new int[0];
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
        StringBuilder sb = new StringBuilder();
        try {
//            Save history
            for (String sd : ids) {
                sb.append(":" + sd);
            }
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString() + " detail " + ex.toString());
        }
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean checkAlreadyProcessRecord(String msisdn, long luckyWordSubId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select msisdn from luckyword_charge where luckyword_sub_id = ? and charge_time >= trunc(sysdate) and error_code = '0'";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, luckyWordSubId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String soluong = rs1.getString("msisdn");
                if (soluong != null && soluong.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkAlreadyProcessRecord msisdn " + msisdn + " luckyWordSubId " + luckyWordSubId + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAlreadyProcessRecord msisdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public boolean checkPreSub(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select isdn from cm_pre.sub_mb where isdn = ? and status = 2";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String soluong = rs1.getString("isdn");
                if (soluong != null && soluong.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkMovitelSub msisdn " + isdn + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkMovitelSub msisdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public boolean checkPosSub(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select isdn from cm_pos.sub_mb where isdn = ? and status = 2";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pos");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String soluong = rs1.getString("isdn");
                if (soluong != null && soluong.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkMovitelSub msisdn " + isdn + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkMovitelSub msisdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
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
        try {
            connection = getConnection("dbluckyword");
            sql = "INSERT INTO mt (mt_id,msisdn,message,mo_his_id,retry_num,receive_time,channel) "
                    + "VALUES(mt_SEQ.nextval,?,?,null,0,sysdate,?)";
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

    public int saveDataPlay(long subId, String msisdn, long moHisId, String letter, String word, String winner) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbluckyword");
            sql = "INSERT INTO luckyword_play (luckyword_play_id,luckyword_sub_id,msisdn,mo_his_id_play,letter, word, play_time, winner) "
                    + "VALUES(luckyword_play_seq.nextval,?,?,?,?,?,sysdate, ?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, subId);
            ps.setString(2, msisdn.trim());
            ps.setLong(3, moHisId);
            ps.setString(4, letter.trim().toUpperCase());
            ps.setString(5, word.trim().toUpperCase());
            ps.setString(6, winner.trim());
            result = ps.executeUpdate();
            logger.info("End saveDataPlay msisdn " + msisdn + " letter " + letter + " word " + word
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
            return result;
        } catch (Exception ex) {
            logger.error("ERROR saveDataPlay msisdn " + msisdn, ex);
            logger.error(AppManager.logException(startTime, ex));
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int saveCharge(long subId, String msisdn, String money, String errorCode, String desc, String nodeName, String threadName) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbluckyword");
            sql = "INSERT INTO luckyword_charge (LUCKYWORD_CHARGE_ID,LUCKYWORD_SUB_ID,MSISDN,MONEY,ERROR_CODE,DESCRIPTION,cluster_name,node_name) "
                    + "VALUES(luckyword_charge_seq.nextval,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, subId);
            ps.setString(2, msisdn.trim());
            ps.setString(3, money.trim());
            ps.setString(4, errorCode);
            ps.setString(5, desc);
            ps.setString(6, nodeName);
            ps.setString(7, threadName);
            result = ps.executeUpdate();
            logger.info("End saveCharge msisdn " + msisdn + " LUCKYWORD_SUB_ID " + subId + " money " + money
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
            return result;
        } catch (Exception ex) {
            logger.error("ERROR saveCharge msisdn " + msisdn + " LUCKYWORD_SUB_ID " + subId, ex);
            logger.error(AppManager.logException(startTime, ex));
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int countWinner(String msisdn) {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs1 = null;
        int result = -1;
        String sqlMo = "select count(*) soluong from luckyword_play where winner = '1' and last_reset is null ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbluckyword");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = Integer.valueOf(rs1.getString("soluong"));
                break;
            }
            logTimeDb("Time to countWinner msisdn " + msisdn + " result " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR countWinner msisdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = -1;
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    //LinhNBV start modified on September 09 2017: Add method to get winner in day
    public int countWinnerInDay(String msisdn) {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs1 = null;
        int result = -1;
        String sqlMo = "select count(*) soluong from luckyword_play where winner = '1' and last_reset is null and play_time >= trunc(sysdate) ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbluckyword");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = Integer.valueOf(rs1.getString("soluong"));
                break;
            }
            logTimeDb("Time to countWinnerInDay msisdn " + msisdn + " result " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR countWinnerInDay msisdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = -1;
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }
    //Get winword from table config, not using file config.properties
    public String getCurrentLuckyWinWord() {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs = null;
        String result = null;
        String sqlMo = "select param_value as win_word from config where module = 'lwWinWord'";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbluckyword");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs = psMo.executeQuery();
            while (rs.next()) {
                result = rs.getString("win_word");
                break;
            }
            logTimeDb("Time to getCurrentLuckyWinWord result " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getCurrentLuckyWinWord result " + result);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    //Get total winner in week
    public int getMaxWinner() {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs1 = null;
        int result = -1;
        String sqlMo = "select param_value as max_winner from config where module = 'lwMaxWinner'";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbluckyword");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = Integer.valueOf(rs1.getString("max_winner"));
                break;
            }
            logTimeDb("Time to getMaxWinner result " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getMaxWinner result " + result);
            logger.error(AppManager.logException(timeSt, ex));
            result = -1;
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public int updateCurrentLuckyWinWord() {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbluckyword");
            sql = "update config set param_value = default_value where module = 'lwWinWord'";
            ps = connection.prepareStatement(sql);
            result = ps.executeUpdate();
            logger.info("End updateCurrentLuckyWinWord result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
            return result;
        } catch (Exception ex) {
            logger.error("ERROR updateCurrentLuckyWinWord result " + result, ex);
            logger.error(AppManager.logException(startTime, ex));
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    //LinhNBV end.
    public String viewWord(String msisdn) {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs1 = null;
        String result = "";
        String sqlMo = "select * from (select word from luckyword_play where msisdn = ? and old_week is null and winner <> '1' "
                + " order by play_time desc) where ROWNUM <=1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbluckyword");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, msisdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = rs1.getString("word");
                break;
            }
            logTimeDb("Time to viewWord msisdn " + msisdn + " result " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR viewWord msisdn " + msisdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = "";
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public int clearDataWhenWin(String msisdn, long moHisId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbluckyword");
            sql = "update luckyword_play set old_week = -2 where msisdn = ? and old_week is null";
            ps = connection.prepareStatement(sql);
            ps.setString(1, msisdn.trim());
            result = ps.executeUpdate();
            logger.info("End clearDataWhenWin isdn " + msisdn + " moHisId " + moHisId
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
            return result;
        } catch (Exception ex) {
            logger.error("ERROR clearDataWhenWin msisdn " + msisdn, ex);
            logger.error(AppManager.logException(startTime, ex));
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int resetOldWeek() {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbluckyword");
            sql = "update luckyword_play set old_week = 1, last_reset = sysdate where (old_week is null or old_week = '-2') and play_time < trunc(sysdate)";
            ps = connection.prepareStatement(sql);
            result = ps.executeUpdate();
            logger.info("End resetOldWeek result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
            return result;
        } catch (Exception ex) {
            logger.error("ERROR resetOldWeek ", ex);
            logger.error(AppManager.logException(startTime, ex));
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
    //LinhNBV start modified on August 27 2017: Add methods to get total sub, total player...

    public int getTotalSub(String command) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        int totalSub = 0;
        String sqlMo = " select count(distinct(msisdn)) as total_sub from mo_his where command = ? and err_code = '0' ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbluckyword");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, command);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                totalSub = rs1.getInt("total_sub");
            }
            logTimeDb("Time to getTotalSub with command " + command + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTotalSub with command " + command);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return totalSub;
    }

    public int getTotalPlayer(String command) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        int totalSub = 0;
        String sqlMo = " select count(msisdn) as total_player from mo_his where command = ? and err_code = '0' ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbluckyword");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, command);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                totalSub = rs1.getInt("total_player");
            }
            logTimeDb("Time to getTotalPlayer with command " + command + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTotalPlayer with command " + command);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return totalSub;
    }

    public int getTotalSubOff() {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        int totalSub = 0;
        String sqlMo = " select count(distinct(msisdn)) as total_sub_off from mo_his where command = 'OFF' and err_code = '0' ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbluckyword");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                totalSub = rs1.getInt("total_sub_off");
            }
            logTimeDb("Time to getTotalSubOff with command OFF" + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTotalSubOff with command OFF");
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return totalSub;
    }
    //LinhNBV end.
}
