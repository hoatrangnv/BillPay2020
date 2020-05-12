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
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
import java.sql.PreparedStatement;
import java.util.Date;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbOpenFlagRegisterInfo extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbOpenFlagRegisterInfo.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDeleteMo = "delete log_register_info where log_register_info_id = ?";

    public DbOpenFlagRegisterInfo() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbEmola";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbOpenFlagRegisterInfo(String sessionName, Logger logger) throws SQLException, Exception {
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
        LogRegisterInfo record = new LogRegisterInfo();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("CustomerID"));
            record.setLogRegisterInfoId(rs.getLong(LogRegisterInfo.LOG_REGISTER_INFO_ID));
            record.setStaffCode(rs.getString(LogRegisterInfo.STAFF_CODE));
            record.setIsdn(rs.getString(LogRegisterInfo.ISDN));
            record.setCreateTime(rs.getTimestamp(LogRegisterInfo.CREATE_TIME));
            record.setResultCode("0");
            record.setDescription("Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse MoRecord");
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
            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbBocNameConfig");
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                LogRegisterInfo sd = (LogRegisterInfo) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getLogRegisterInfoId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue LOG_REGISTER_INFO batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue LOG_REGISTER_INFO, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                LogRegisterInfo sd = (LogRegisterInfo) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param(LogRegisterInfo.LOG_REGISTER_INFO_ID, sd.getLogRegisterInfoId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(LogRegisterInfo.STAFF_CODE, sd.getStaffCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(LogRegisterInfo.ISDN, sd.getIsdn(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(LogRegisterInfo.CREATE_TIME, sd.getCreateTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param(LogRegisterInfo.PROCESS_TIME, "sysdate", Param.DataType.CONST, Param.IN));
                paramList.add(new Param(LogRegisterInfo.RESULT_CODE, sd.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(LogRegisterInfo.DESCRIPTION, sd.getDescription(), Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "LOG_REGISTER_INFO_HIS");
            logTimeDb("Time to insertQueueHis LOG_REGISTER_INFO_HIS, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis LOG_REGISTER_INFO_HIS batchid " + batchId, ex);
            try {
                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "LOG_REGISTER_INFO_HIS");
                logTimeDb("Time to retry insertQueueHis LOG_REGISTER_INFO_HIS, batchid " + batchId + " total result: " + res.length, timeSt);
                return res;
            } catch (Exception ex1) {
                logger.error("ERROR retry insertQueueHis LOG_REGISTER_INFO_HIS batchid " + batchId, ex1);
                logger.error(AppManager.logException(timeSt, ex));
                return null;
            }
        }
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        /*
         * insert into MT(MT_ID,MO_HIS_ID,MSISDN,MESSAGE,RECEIVE_TIME,RETRY_NUM,CHANNEL) "
         + "values(MT_SEQ.NEXTVAL, ?, ?, ?, sysdate, 0, ?)
         */
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        int[] res = new int[0];
        long timeSt = System.currentTimeMillis();
        try {
            String countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
            String sms_code = ResourceBundle.getBundle("configPayBonus").getString("sms_code");
            for (Record rc : listRecords) {
                LogRegisterInfo sd = (LogRegisterInfo) rc;
                if (sd.getMessage() == null || sd.getIsdn() == null || sd.getMessage().trim().length() <= 0
                        || sd.getIsdn().trim().length() <= 0) {
                    continue;
                }
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("MO_HIS_ID", sd.getLogRegisterInfoId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("MT_ID", "MT_SEQ.NEXTVAL", Param.DataType.CONST, Param.IN));
                paramList.add(new Param("MSISDN", countryCode + sd.getIsdn(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("MESSAGE", sd.getMessage(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("CHANNEL", sms_code, Param.DataType.CONST, Param.IN));
                listParam.add(paramList);
            }
            if (listParam.size() > 0) {
                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "MT");
                logTimeDb("Time to insertQueueOutput MT, batchid " + batchId + " total result: " + res.length, timeSt);
            } else {
                logTimeDb("List Record to insert Queue Output is empty, batchid " + batchId, timeSt);
            }
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueOutput batchid " + batchId, ex);
            try {
                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "MT");
                logTimeDb("Time to retry insertQueueOutput MT, batchid " + batchId + " total result: " + res.length, timeSt);
                return res;
            } catch (Exception ex1) {
                logger.error("ERROR retry insertQueueOutput MT, batchid " + batchId, ex1);
                logger.error(AppManager.logException(timeSt, ex));
                return null;
            }
        }
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        StringBuilder sb = new StringBuilder();
        long timeSt = System.currentTimeMillis();
        try {
            for (String sd : ids) {
                sb.append(":" + sd);
                ParamList paramList = new ParamList();
                paramList.add(new Param(LogRegisterInfo.LOG_REGISTER_INFO_ID, Long.valueOf(sd), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(LogRegisterInfo.RESULT_CODE, "FW_99", Param.DataType.STRING, Param.IN));
                paramList.add(new Param(LogRegisterInfo.DESCRIPTION, "FW_Timeout", Param.DataType.STRING, Param.IN));
                paramList.add(new Param(LogRegisterInfo.PROCESS_TIME, "sysdate", Param.DataType.CONST, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "LOG_REGISTER_INFO_HIS");
            logTimeDb("Time to processTimeoutRecord, insert LOG_REGISTER_INFO_HIS, total result: " + res.length, timeSt);
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
            try {
                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "LOG_REGISTER_INFO_HIS");
                logTimeDb("Time to retry processTimeoutRecord, insert LOG_REGISTER_INFO_HIS, total result: " + res.length, timeSt);
            } catch (Exception ex1) {
                logger.error("ERROR retry processTimeoutRecord ", ex1);
                logger.error(AppManager.logException(timeSt, ex));
            }
        }
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
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
            if(isdn.startsWith("258")){
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
}
