/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.KitVas;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.threadfw.database.DbProcessorAbstract;
import static com.viettel.threadfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
public class DbElite extends DbProcessorAbstract {

    private String loggerLabel = DbElite.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDelete = "Delete kit_vas_bonus where isdn=? ";
    private String sqlInsert = "INSERT INTO kit_vas_bonus(KIT_VAS_BONUS_ID,ISDN,PRODUCT_CODE,GRADE_OCS,CREATE_TIME) VALUES(kit_vas_bonus_seq.nextval,?,?,?,sysdate) ";

    public DbElite() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "cm_pre";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbElite(String sessionName, Logger logger) throws SQLException, Exception {
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
        KitVas record = new KitVas();
        long timeSt = System.currentTimeMillis();
        try {
            record.setKitVasId(rs.getLong("kit_vas_id"));
            record.setIsdn(rs.getString("isdn"));
            record.setSerial(rs.getString("serial"));
            record.setCreateUser(rs.getString("create_user"));
            record.setStaDateTime(rs.getDate("create_date"));
            record.setProductCode(rs.getString("product_code"));
            record.setResultCode("0");
        } catch (Exception ex) {
            logger.error("ERROR parse KitVas");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        if (listRecords.isEmpty()) {
            return new int[0];
        }
        
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                KitVas kv = (KitVas) rc;
                batchId = kv.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("KIT_VAS_BONUS_ID", kv.getKitVasId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("ISDN", kv.getIsdn(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("PRODUCT_CODE", kv.getProductCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("GRADE_OCS", kv.getGradeOcs(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("CREATE_TIME", kv.getStaDateTime(), Param.DataType.DATE, Param.IN));
                paramList.add(new Param("ERROR_CODE", kv.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("DESCRIPTION", kv.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("PROCESS_TIME", "sysdate", Param.DataType.CONST, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "KIT_VAS_BONUS_HIS");
            logTimeDb("Time to deleteQueue KIT_VAS_BONUS_HIS, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue KIT_VAS_BONUS_HIS batchid " + batchId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        }
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

    public int insertKitVasBonus(KitVas kv) {
        Connection connection = null;
        PreparedStatement ps = null;
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlInsert);
            ps.setString(1, kv.getIsdn());
            ps.setString(2, kv.getProductCode());
            ps.setLong(3, kv.getGradeOcs());
            result = ps.executeUpdate();
            if (result == 1L) {
                this.logger.info("Insert kit_vas_bonus successfully with isdn: " + kv.getIsdn());
                return result;
            }
        } catch (Exception ex) {
            this.logger.error("Insert kit_vas_bonus fail");
            this.logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }

    public int deleteKitVasBonus(KitVas kv) {
        Connection connection = null;
        PreparedStatement ps = null;
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlDelete);
            ps.setString(1, kv.getIsdn());
            result = ps.executeUpdate();
            if (result == 1L) {
                this.logger.info("Delete kit_vas_bonus successfully with isdn: " + kv.getIsdn());
                return result;
            }
        } catch (Exception ex) {
            this.logger.error("Delete kit_vas_bonus fail");
            this.logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }

    public boolean checkRenew(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement psGetCell = null;
        boolean result = false;
        String sqlGetCell = "select * from report.prepaid_vas_elite "
                + " where sta_datetime > trunc(ADD_MONTHS(sysdate, -1), 'MON') and sta_datetime < trunc(sysdate , 'MON')"
                + " and calling_number = ?  ";
        try {
            connection = ConnectionPoolManager.getConnection("report");
            psGetCell = connection.prepareStatement(sqlGetCell);
            if (QUERY_TIMEOUT > 0) {
                psGetCell.setQueryTimeout(QUERY_TIMEOUT);
            }
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3, 12);
            }
            psGetCell.setString(1, isdn);
            rs = psGetCell.executeQuery();
            if (rs.next()) {
                result = true;
            }
            logTimeDb("Time to checkRenew staffcode isdn " + isdn + "  result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkRenew isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(psGetCell);
            closeConnection(connection);
        }
        return result;
    }
    
    public boolean checkByMore(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement psGetCell = null;
        boolean result = false;
        String sqlGetCell = "select * from vas_elite.sub_renew_elite_his "
                + " where process_time > trunc(ADD_MONTHS(sysdate, -1), 'MON') and input_type = 'PROCESS' and isdn = ?  ";
        try {
            connection = ConnectionPoolManager.getConnection("vasElite");
            psGetCell = connection.prepareStatement(sqlGetCell);
            if (QUERY_TIMEOUT > 0) {
                psGetCell.setQueryTimeout(QUERY_TIMEOUT);
            }
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            psGetCell.setString(1, isdn);
            rs = psGetCell.executeQuery();
            if (rs.next()) {
                result = true;
            }
            logTimeDb("Time to checkByMore isdn " + isdn + "  result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkByMore isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(psGetCell);
            closeConnection(connection);
        }
        return result;
    }

    public boolean checkKitVasBonus(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        String sqlGetCell = "select * from cm_pre.kit_vas_bonus where isdn = ? ";
        try {
            connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlGetCell);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3, 12);
            }
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            if (rs.next()) {
                result = true;
            }
        } catch (Exception ex) {
            logger.error("ERROR checkKitVasBonus ");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }
}
