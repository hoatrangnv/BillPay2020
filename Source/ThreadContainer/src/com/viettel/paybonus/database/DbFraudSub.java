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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
public class DbFraudSub extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbFraudSub.class.getSimpleName() + ": ";
    private String dbNameCofig;
    private PoolStore poolStore;
    private String sqlDeleteMo = "delete fraud_sub_input where fraud_sub_input_id = ?";

    public DbFraudSub() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "db_payment";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbFraudSub(String sessionName, Logger logger) throws SQLException, Exception {
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
        FraudSubInput record = new FraudSubInput();
        long timeSt = System.currentTimeMillis();
        try {
            record.setFraud_sub_input_id(Long.valueOf(rs.getLong("FRAUD_SUB_INPUT_ID")));
            record.setIsdn(rs.getString("ISDN"));
            record.setCommand(rs.getString("COMMAND"));
            record.setInput_time(rs.getDate("INPUT_TIME"));
            record.setFileName(rs.getString("FILE_NAME"));
            record.setActionType(rs.getString("ACTION_TYPE"));
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
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                FraudSubInput sd = (FraudSubInput) rc;
                if ("BLOCK".equals(sd.getActionType())) {
                    batchId = sd.getBatchId();
                    ps.setLong(1, sd.getFraud_sub_input_id());
                    ps.addBatch();
                }
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue fraud_sub_input batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue fraud_sub_input, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParamBlock = new ArrayList();
        List<ParamList> listParamUnBlock = new ArrayList();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                FraudSubInput sd = (FraudSubInput) rc;
                if ("BLOCK".equals(sd.getActionType())) {
                    batchId = sd.getBatchId();
                    ParamList paramList = new ParamList();
                    paramList.add(new Param("HIS_ID", sd.getFraud_sub_input_id(), Param.DataType.LONG, 0));
                    paramList.add(new Param("COMMAND", sd.getCommand(), Param.DataType.STRING, 0));
                    paramList.add(new Param("ISDN", sd.getIsdn(), Param.DataType.STRING, 0));
                    paramList.add(new Param("INPUT_TIME", sd.getInput_time(), Param.DataType.DATE, 0));
                    paramList.add(new Param("ACTION_TIME", "sysdate", Param.DataType.CONST, 0));
                    paramList.add(new Param("ERR_CODE", sd.getResultCode(), Param.DataType.STRING, 0));
                    paramList.add(new Param("ERR_OCS", sd.getOcsResponseCode(), Param.DataType.STRING, 0));
                    paramList.add(new Param("NODE_NAME", sd.getNodeName(), Param.DataType.STRING, 0));
                    paramList.add(new Param("CLUSTER_NAME", sd.getClusterName(), Param.DataType.STRING, 0));
                    paramList.add(new Param("DESCRIPTION", sd.getDescription(), Param.DataType.STRING, 0));
                    paramList.add(new Param("FILE_NAME", sd.getFileName(), Param.DataType.STRING, 0));

                    listParamBlock.add(paramList);
                } else {
                    batchId = sd.getBatchId();
                    ParamList paramList = new ParamList();
                    paramList.add(new Param("HIS_ID", sd.getFraud_sub_input_id(), Param.DataType.LONG, 0));
                    paramList.add(new Param("COMMAND", sd.getCommand(), Param.DataType.STRING, 0));
                    paramList.add(new Param("ISDN", sd.getIsdn(), Param.DataType.STRING, 0));
                    paramList.add(new Param("INPUT_TIME", sd.getInput_time(), Param.DataType.DATE, 0));
                    paramList.add(new Param("ACTION_TIME", "sysdate", Param.DataType.CONST, 0));
                    paramList.add(new Param("ERR_CODE", sd.getResultCode(), Param.DataType.STRING, 0));
                    paramList.add(new Param("ERR_OCS", sd.getOcsResponseCode(), Param.DataType.STRING, 0));
                    paramList.add(new Param("NODE_NAME", sd.getNodeName(), Param.DataType.STRING, 0));
                    paramList.add(new Param("CLUSTER_NAME", sd.getClusterName(), Param.DataType.STRING, 0));
                    paramList.add(new Param("DESCRIPTION", sd.getDescription(), Param.DataType.STRING, 0));
                    paramList.add(new Param("FILE_NAME", sd.getFileName(), Param.DataType.STRING, 0));

                    listParamUnBlock.add(paramList);
                }
            }
            int[] res = new int[0];
            if (!listParamBlock.isEmpty()) {
                res = this.poolStore.insertTable((ParamList[]) listParamBlock.toArray(new ParamList[listParamBlock.size()]), "fraud_sub_input_his");
                logTimeDb("Time to insertQueueHis fraud_sub_input_his, batchid " + batchId + " total result: " + res.length, timeSt);
            } else if (!listParamUnBlock.isEmpty()) {
                res = this.poolStore.insertTable((ParamList[]) listParamUnBlock.toArray(new ParamList[listParamUnBlock.size()]), "fraud_sub_input_unlock_his");
                logTimeDb("Time to insertQueueHis fraud_sub_input_unlock_his, batchid " + batchId + " total result: " + res.length, timeSt);
            }

            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis FRAUD_SUB_INPUT_HIS batchid " + batchId, ex);
            logger.error(AppManager.logException(timeSt, ex));
        }
        return null;
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

    public int updateActStatusSubMB(String subId, String actStatus) {
        long timeSt = System.currentTimeMillis();
        int rs1 = 0;
        Connection connection = null;
        String sqlMo = "update sub_mb set act_status = ? where sub_id = ? ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, actStatus);
            psMo.setString(2, subId);
            rs1 = psMo.executeUpdate();
            logTimeDb("Time to updateActStatusSubMB subId " + subId + " act status " + actStatus + " result: " + rs1, timeSt);
            return rs1;
        } catch (Exception ex) {
            logger.error("ERROR updateActStatusSubMB subId " + subId);
            logger.error(AppManager.logException(timeSt, ex));

            return rs1;
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
    }

    public int insertActionAudit(String subId, String desc) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        try {
            connection = getConnection("cm_pre");
            sql = " INSERT INTO cm_pre.action_audit(ACTION_AUDIT_ID,ISSUE_DATETIME,ACTION_CODE,REASON_ID,SHOP_CODE,USER_NAME,PK_TYPE,PK_ID,IP,DESCRIPTION,VALID)  VALUES (cm_pre.seq_action_audit.nextval,sysdate,'67',970,'MV','SYSTEM AUTO','3',?,'',?,null) ";

            ps = connection.prepareStatement(sql);
            ps.setString(1, subId);
            ps.setString(2, desc);
            return ps.executeUpdate();
        } catch (Exception ex) {
            br.setLength(0);
            this.logger.error(br + ex.toString());
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
        }
    }

    public String getPreVASCode(String subId) {
        Connection connection = null;
        String str = "0";
        String sql = "SELECT DISTINCT rel_product_code FROM cm_pre.sub_rel_product WHERE status =1 AND sub_id = ? ";
        PreparedStatement psMo = null;
        ResultSet rs = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sql);
            psMo.setString(1, subId);
            rs = psMo.executeQuery();
            while (rs.next()) {
                str += "-" + rs.getString("rel_product_code").trim();
            }
            return str;
        } catch (Exception e) {
            this.logger.error("ERROR AT getPreVASCode(): param:" + subId + ", ", e);
            return "";
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
    }

    public String getSubIdByIsdn(String isdn) {
        Connection connection = null;
        String result = null;
        String sql = " select sub_id from cm_pre.sub_mb where isdn =? and status =2 ";
        PreparedStatement psMo = null;
        ResultSet rs = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sql);
            psMo.setString(1, isdn);
            rs = psMo.executeQuery();
            while (rs.next()) {
                result = rs.getString("sub_id");
            }
            logger.error("getSubIdByIsdn  " + isdn + " result , " + result);
            return result;
        } catch (Exception e) {
            this.logger.error("ERROR AT getSubIdByIsdn param " + isdn + ", ", e);
            return null;
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
    }

    public boolean checkStatusPhoneByIsdn(String isdn, boolean isActive) {
        Connection connection = null;
        boolean result = false;
        String sql = " select sub_id from cm_pre.sub_mb where isdn =? and status =2 ";
        if (isActive) {
            sql += " and act_status = '00' ";
        } else {
            sql += " and act_status <> '00' ";
        }
        PreparedStatement psMo = null;
        ResultSet rs = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sql);
            psMo.setString(1, isdn);
            rs = psMo.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.error("checkStatusPhoneByIsdn  " + isdn + " result , " + result);
            return result;
        } catch (Exception e) {
            this.logger.error("ERROR AT checkStatusPhoneByIsdn param " + isdn + ", ", e);
            return result;
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
    }

    public boolean checkGenerrateSMS(String isdn, int times, int preriod) {
        Connection connection = null;
        boolean result = false;
        String sql = " select count(1) as totalSms  from CDR_MSC_MT_SMS where start_time > trunc(sysdate) - ? and  from_phone_number = ? ";
        PreparedStatement psMo = null;
        ResultSet rs = null;
        int totalSms = 0;
        try {
            connection = ConnectionPoolManager.getConnection("htds");
            psMo = connection.prepareStatement(sql);
            psMo.setInt(1, preriod);
            psMo.setString(2, isdn);
            rs = psMo.executeQuery();
            while (rs.next()) {
                totalSms = rs.getInt("totalSms");
                break;
            }
            if (totalSms >= times) {
                result = true;
            }
            logger.error("checkStatusPhoneByIsdn  " + isdn + " result , " + result + " totalSms " + totalSms);
            return result;
        } catch (Exception e) {
            this.logger.error("ERROR AT checkStatusPhoneByIsdn param " + isdn + ", ", e);
            return result;
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
    }

    public boolean checkGenerrateData(String isdn, int minKD, int preriod) {
        Connection connection = null;
        boolean result = false;
        String sql = " select sum(nvl(total_up_down,0)) as kb_data  from PREPAID_GPRS_10092010 where sta_datetime > trunc(sysdate) - ? and  calling_number = ? ";
        PreparedStatement psMo = null;
        ResultSet rs = null;
        int totalKB = 0;
        try {
            isdn = (isdn.startsWith("258") ? isdn : "258" + isdn);
            connection = ConnectionPoolManager.getConnection("pre_call");
            psMo = connection.prepareStatement(sql);
            psMo.setInt(1, preriod);
            psMo.setString(2, isdn);
            rs = psMo.executeQuery();
            while (rs.next()) {
                totalKB = rs.getInt("kb_data");
                break;
            }
            if (totalKB >= minKD) {
                result = true;
            }
            logger.error("checkGenerrateData  " + isdn + " result , " + result + " totalSms " + totalKB);
            return result;
        } catch (Exception e) {
            this.logger.error("ERROR AT checkGenerrateData param " + isdn + ", ", e);
            return result;
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
    }

    public int updateLastProcessInfo(long recordId, String isdn, int status) {
        Connection connection = null;
        int result = 0;
        String sql = " update fraud_sub_input_his set last_unlock_time= sysdate ,last_unlock_status =? where his_id=? and isdn=? ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sql);
            psMo.setInt(1, status);
            psMo.setLong(2, recordId);
            psMo.setString(3, isdn);
            result = psMo.executeUpdate();
            logger.error("checkGenerrateData  " + isdn + " result , " + result);
            return result;
        } catch (Exception e) {
            this.logger.error("ERROR AT checkGenerrateData param " + isdn + ", ", e);
            return -1;
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
    }
}
