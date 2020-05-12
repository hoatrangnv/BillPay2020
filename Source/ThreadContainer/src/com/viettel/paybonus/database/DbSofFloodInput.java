/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.SofFloodInput;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.threadfw.database.DbProcessorAbstract;
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
public class DbSofFloodInput extends DbProcessorAbstract {

    private String loggerLabel = DbSofFloodInput.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;
    private String dbNameReportCDR="dbReportCDR";
    private String sqlDeleteMo = "delete Sof_Flood_Input where id = ?";

    public DbSofFloodInput() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbElite";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbSofFloodInput(String sessionName, Logger logger) throws SQLException, Exception {
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
        SofFloodInput record = new SofFloodInput();
        long timeSt = System.currentTimeMillis();
        try {
            record.setMoId(rs.getLong("ID"));
            record.setMsisdn(rs.getString("MSISDN"));
            record.setRegistorTime(rs.getTimestamp("registor_time"));
            record.setCreateTime(rs.getTimestamp("create_time"));
            record.setType(rs.getLong("TYPE"));
            record.setResultCode("0");
        } catch (Exception ex) {
            logger.error("ERROR parse SofFloodInput");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        String batchId = "";
        try {
            connection = ConnectionPoolManager.getConnection(dbNameReportCDR);
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                SofFloodInput sd = (SofFloodInput) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getMoId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue Sof_Flood_Input batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue Sof_Flood_Input, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        if (listRecords.isEmpty()) {
            return new int[0];
        }

        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                SofFloodInput kv = (SofFloodInput) rc;
                batchId = kv.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("HIS_ID", kv.getMoId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("MSISDN", kv.getMsisdn(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("RECEIVE_TIME", kv.getRegistorTime(), Param.DataType.TIMESTAMP, Param.IN)); //tannh sua lai add thoi gian cong CS
                paramList.add(new Param("TYPE", kv.getType(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("PROCESS_TIME", "sysdate", Param.DataType.CONST, Param.IN));
                paramList.add(new Param("ERR_CODE", kv.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("ERR_OCS", null, Param.DataType.STRING, Param.IN));
                paramList.add(new Param("NODE_NAME", kv.getNodeName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("CLUSTER_NAME", kv.getClusterName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("FEE_ACTION", null, Param.DataType.LONG, Param.IN));
                paramList.add(new Param("PROMOTION_VALUE", null, Param.DataType.LONG, Param.IN));
                paramList.add(new Param("DESCRIPTION", kv.getDesctiption(), Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "Sof_Flood_His");
            logTimeDb("Time to deleteQueue Sof_Flood_His , batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue Sof_Flood_His  batchid " + batchId, ex);
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


    public int sendSms(String msisdn, String message, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbElite");
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
}
