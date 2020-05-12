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
import com.viettel.vas.util.obj.DataResources;
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
import java.util.ResourceBundle;
import oracle.sql.TIMESTAMP;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbFirstRegister extends DbProcessorAbstract {

    private String loggerLabel = DbFirstRegister.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbFirstRegister() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbFirstRegister(String sessionName, Logger logger) throws SQLException, Exception {
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
        FirstRegister record = new FirstRegister();
        long timeSt = System.currentTimeMillis();
        try {
            record.setIsdn(rs.getString("isdn"));
            record.setCreateDate(rs.getTimestamp("create_date"));
            record.setPackageCode(rs.getString("package_code"));
            record.setEwalletErrCode("Error");
        } catch (Exception ex) {
            logger.error("ERROR parse SubProfileInfo");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
//        return new int[0];
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        String batchId = "";
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement("delete first_register_capital where isdn = ? ");
            for (Record rc : listRecords) {
                FirstRegister fr = (FirstRegister) rc;
                batchId = fr.getBatchId();
                ps.setString(1, fr.getIsdn());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue announcement batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue announcement, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
//        return new int[0];
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                FirstRegister fr = (FirstRegister) rc;
                batchId = fr.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("isdn", fr.getIsdn(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("create_time", fr.getCreateDate(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param("package_code", fr.getPackageCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("pay_bonus", fr.getPayBonus(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("result_code", fr.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("description", fr.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("process_time", fr.getProcessTime(), Param.DataType.DATE, Param.IN));
                paramList.add(new Param("ewallet_err_code", fr.getEwalletErrCode(), Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "first_register_capital_his");
            logTimeDb("Time to insertQueueHis announcement_his, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis announcement_his batchid " + batchId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        }
    }

    public int deleteFirstResister(String isdn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        int res = 0;
        try {
            connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement("delete first_register_capital where isdn = ? ");
            ps.setString(1, isdn);
            res = ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("ERROR deleteFirstResister with isdn: " + isdn, ex);
            logger.error(AppManager.logException(timeStart, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteFirstResister, with isdn: " + isdn, timeStart);
            return res;
        }
    }

    public int insertFirstResisterHis(FirstRegister bn) {
        ParamList paramList = new ParamList();
        try {
            paramList.add(new Param("isdn", bn.getIsdn(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("create_time", bn.getCreateDate(), Param.DataType.TIMESTAMP, Param.IN));
            paramList.add(new Param("package_code", bn.getPackageCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("pay_bonus", bn.getPayBonus(), Param.DataType.LONG, Param.IN));
            paramList.add(new Param("result_code", bn.getResultCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("description", bn.getDescription(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("process_time", "sysdate", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("ewallet_err_code", bn.getEwalletErrCode(), Param.DataType.STRING, Param.IN));
            long timeSt = System.currentTimeMillis();
            PoolStore.PoolResult prs = poolStore.insertTable(paramList, "first_register_capital_his");
            logTimeDb("Time to insertFirstResisterHis", timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertFirstResisterHis", ex);
        }
        return -1;
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

    public boolean checkAlreadyProcessRecord(String isdn) {
        ParamList paramList = new ParamList();
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("isdn", isdn, Param.DataType.STRING, Param.IN));
            DataResources rs = poolStore.selectTable(paramList, "first_register_capital_his");
            while (rs.next()) {
                String id = rs.getString("isdn");
                if (id != null && id.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkAlreadyProcessRecord isdn " + isdn + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAlreadyProcessRecord defaul return false" + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        }
        return result;
    }

    public StaffInfo getStaffInfo(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        StaffInfo result = null;
        String sqlMo = "select * from sm.staff where staff_code in (select upper(create_staff) from cm_pre.sub_profile_info a "
                + " where a.create_time > trunc(sysdate - 30) and a.isdn = ? and a.check_status = '1' "
                + " and exists (select 1 from cm_pre.sub_mb where isdn = a.isdn and status = 2))";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            if (rs1.next()) {
                result = new StaffInfo();
                result.setChannelTypeId(rs1.getInt("channel_type_id"));
                result.setAreaManageId(rs1.getLong("area_manage_id"));
                result.setIsdnWallet(rs1.getString("isdn_wallet"));
                result.setStaffCode(rs1.getString("staff_code"));
                result.setStaffOnerId(rs1.getLong("staff_owner_id"));
            }
            logTimeDb("Time to getStaffInfo for isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getStaffInfo isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public StaffInfo getStaffMaster(long staffId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        StaffInfo result = null;
        String sqlMo = "select * from sm.staff where staff_id = ? ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, staffId);
            rs1 = psMo.executeQuery();
            if (rs1.next()) {
                result = new StaffInfo();
                result.setStaffCode(rs1.getString("staff_code"));
                result.setIsdnWallet(rs1.getString("isdn_wallet"));
            }
            logTimeDb("Time to getStaffMaster for staffId " + staffId, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getStaffMaster staffId " + staffId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public int checkWaitingOverTime(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        int result = -1;
        String sqlMo = "select sysdate-create_time as overtime from cm_pre.sub_profile_info "
                + " where isdn=? and create_time >= trunc(sysdate -30)";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            if (rs1.next()) {
                result = rs1.getInt("overtime");
            }
            logTimeDb("Time to getStaffInfo for isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getStaffInfo isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = -1;
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
            paramList.add(new Param("BONUS_TYPE", 6L, Param.DataType.LONG, Param.IN));
            PoolStore.PoolResult prs = poolStore.insertTable(paramList, "EWALLET_LOG");
            logTimeDb("Time to insertEwalletLog isdn " + log.getMobile(), timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getMobile());
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }
}
