/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.*;
import com.viettel.threadfw.database.DbProcessorAbstract;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbKitVipImport extends DbProcessorAbstract {

    private String loggerLabel = DbKitVipImport.class.getSimpleName() + ": ";

    public DbKitVipImport() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
    }

    public DbKitVipImport(Logger logger) throws SQLException, Exception {
        this.logger = logger;
    }

    @Override
    public Record parse(ResultSet rs) {
        KitWarning record = new KitWarning();
        long timeSt = System.currentTimeMillis();
        try {
            record.setActionAuditId(rs.getLong("IMPORT_COUNT"));
            record.setIsdn(rs.getString("ISDN"));
            record.setEffectDate(rs.getString("EFFECT_DATE"));
            record.setProductCode(rs.getString("PRODUCT_CODE"));
        } catch (Exception ex) {
            logger.error("ERROR parse KitWarning");
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
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            KitWarning sd = (KitWarning) listRecords.get(0);//Insert 1 time.
            batchId = sd.getBatchId();
            ParamList paramList = new ParamList();
            paramList.add(new Param("PROCESS_DATE", "sysdate", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("IMPORT_DATE", "trunc(sysdate)", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("IMPORT_COUNT", sd.getActionAuditId(), Param.DataType.LONG, Param.IN));
            listParam.add(paramList);
            PoolStore poolStore = new PoolStore("dbPreCall", logger);
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "ELITE_IMPORT_DAILY");
            logTimeDb("Time to insertQueueHis ELITE_IMPORT_DAILY, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis ELITE_IMPORT_DAILY batchid " + batchId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        }
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

    public boolean checkAlreadyImported(String isdn, String productCode, String effectDate) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        ResultSet rs = null;
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbElite");
            sql = "select * from kit_vip_warning where isdn = ? and upper(product_code) = upper(?) and effect_date = to_date(?,'yyyyMMddhh24miss')";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setString(2, productCode);
            ps.setString(3, effectDate);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkAlreadyImported,isdn:" + isdn + ", productCode: " + productCode + ", result  " + result + ", time: " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkAlreadyImported: ").append(ex.getMessage());
            logger.error(br + ex.toString());
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            closeResultSet(rs);
        }
        return result;
    }

    public boolean checkAlreadyProcessed(Long importCount) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        ResultSet rs = null;
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbPreCall");
            sql = "select * from elite_import_daily where import_date = trunc(sysdate) and import_count = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, importCount);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkAlreayProcessed, result  " + result + ", time: " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkAlreayProcessed: ").append(ex.getMessage());
            logger.error(br + ex.toString());
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            closeResultSet(rs);
        }
        return result;
    }

    public int[] insertKitVipWarning(List<KitWarning> listInput) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int[] result = new int[0];
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbElite");
            sql = "INSERT INTO kit_vip_warning (isdn,product_code,effect_date) "
                    + "VALUES(?,?,to_date(?,'yyyyMMddhh24miss'))";
            ps = connection.prepareStatement(sql);
            for (KitWarning obj : listInput) {
                ps.setString(1, obj.getIsdn());
                ps.setString(2, obj.getProductCode());
                ps.setString(3, obj.getEffectDate());
                ps.addBatch();
            }
            result = ps.executeBatch();
            logger.info("End insertKitVipWarning list size " + listInput.size() + " result size " + result.length + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertKitVipWarning");
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
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
}
