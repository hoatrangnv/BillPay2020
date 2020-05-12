/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.FirstMobilePhone;
import com.viettel.paybonus.obj.StaffInfo;
import com.viettel.paybonus.obj.Trparu300;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.threadfw.database.DbProcessorAbstract;
import static com.viettel.threadfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.PoolStore;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbTrparu300 extends DbProcessorAbstract {

    private String loggerLabel = DbTrparu300.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbTrparu300() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbapp1";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbTrparu300(String sessionName, Logger logger) throws SQLException, Exception {
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
        Trparu300 record = new Trparu300();
        long timeSt = System.currentTimeMillis();
        try {
            record.setIsdn(rs.getString("isdn"));
            record.setCreateTime(rs.getTimestamp("create_time"));
        } catch (Exception ex) {
            logger.error("ERROR parse NewSubscriberPhone");
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

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int[] deleteQueueTimeout(List<String> listId) {
        return new int[0];
    }

    public int deleteTrparu300(Trparu300 bn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        int res = 0;
        try {
            connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement("delete rp_arpu_300 where  create_time <= sysdate -90 ");
            res = ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("ERROR deleteNewSubscriberPhone id " + bn.getID() + " isdn " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeStart, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteNewSubscriberPhone, id " + bn.getID() + " isdn " + bn.getIsdn() + " result " + res, timeStart);
            return res;
        }
    }
    
    public int deleteTrparu300His(Trparu300 bn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        int res = 0;
        try {
            connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement("delete rp_arpu_300_His where  create_time <= sysdate -90 ");
            res = ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("ERROR deleteTrparu300His id " + bn.getID() + " isdn " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeStart, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteTrparu300His, id " + bn.getID() + " isdn " + bn.getIsdn() + " result " + res, timeStart);
            return res;
        }
    }
    
    public String getCell(String staffCode, String cellId, String lacId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement psGetCell = null;
        String cell = "";
        String sqlGetCell = "select cell from cell where ci = TO_NUMBER (util.convert_cell_id (?)) "
                + " and lac = TO_NUMBER (util.convert_lac (?)) and is_delete = 0 and ROWNUM < 2";
        try {
            connection = ConnectionPoolManager.getConnection("dbtracecell");
            psGetCell = connection.prepareStatement(sqlGetCell);
            if (QUERY_TIMEOUT > 0) {
                psGetCell.setQueryTimeout(QUERY_TIMEOUT);
            }
            psGetCell.setString(1, cellId);
            psGetCell.setString(2, lacId);
            rs = psGetCell.executeQuery();
            while (rs.next()) {
                String cellValue = rs.getString("cell");
                if (cellValue != null && cellValue.length() > 0) {
                    cell = cellValue;
                    break;
                }
            }
            logTimeDb("Time to getCell staffcode " + staffCode + " cellId " + cellId
                    + " lacId" + lacId + " result: " + cell, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getCell staffcode " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(psGetCell);
            closeConnection(connection);
        }
        return cell;
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
    
    public int deleteTrparu300HisByIsdn(String isdn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        int res = 0;
        try {
            connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement("delete rp_arpu_300_his where  isdn=? ");
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
    
}
