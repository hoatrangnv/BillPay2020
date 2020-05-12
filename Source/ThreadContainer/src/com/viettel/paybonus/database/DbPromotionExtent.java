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
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 *
 * Thong tin phien ban
 *
 * @author itbl_jony
 * @version 1.0
 * @since 09-01-2019
 */
public class DbPromotionExtent extends DbProcessorAbstract {

    private String loggerLabel = DbPromotionExtent.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlUpdate = "Update branch_promotion_sub set expire_time = sysdate + 30 where isdn=? ";

    public DbPromotionExtent() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        poolStore = new PoolStore("dbvas", logger);
    }

    public DbPromotionExtent(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
        dbNameCofig = "dbvas";
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
        BranchPromotionSub record = new BranchPromotionSub();
        long timeSt = System.currentTimeMillis();
        try {
            record.setActionAuditId(rs.getLong("ACTION_AUDIT_ID"));
            record.setIsdn(rs.getString("ISDN"));
            record.setCreateUser(rs.getString("CREATE_USER"));
            record.setCreateTime(rs.getTimestamp("CREATE_TIME"));
            record.setProductCode(rs.getString("PRODUCT_CODE"));
            record.setExprieTime(rs.getTimestamp("EXPIRE_TIME"));

            record.setResultCode("0");
            record.setDescription("Start Processing");
            record.setLastExprieTime(rs.getTimestamp("EXPIRE_TIME"));
        } catch (Exception ex) {
            logger.error("ERROR parse SubProfileInfo");
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
                BranchPromotionSub gps = (BranchPromotionSub) rc;
                batchId = gps.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("ACTION_AUDIT_ID", gps.getActionAuditId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("ISDN", gps.getIsdn(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("CREATE_USER", gps.getCreateUser(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("CREATE_TIME", gps.getCreateTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param("PRODUCT_CODE", gps.getProductCode(), Param.DataType.STRING, Param.IN));

                paramList.add(new Param("MONEY_FEE", gps.getMoneyFee(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("LAST_EXPIRE_TIME", gps.getLastExprieTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param("NEW_EXPIRE_TIME", gps.getNewExprieTime(), Param.DataType.TIMESTAMP, Param.IN));

                paramList.add(new Param("PROCESS_TIME", "sysdate", Param.DataType.CONST, Param.IN));
                paramList.add(new Param("RESULT_CODE", gps.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("DESCRIPTION", gps.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("CLUSTER_NAME", gps.getClusterName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("NODE_NAME", gps.getNodeName(), Param.DataType.STRING, Param.IN));

                listParam.add(paramList);
                if (gps.getMoneyFee() > 0) {
                    updateBranchPromotionSub(gps);
                }
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "BRANCH_PROMOTION_EXTENT_HIS");
            logTimeDb("Time to insertQueueHis BRANCH_PROMOTION_EXTENT_HIS, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis BRANCH_PROMOTION_EXTENT_HIS batchid " + batchId, ex);
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

    public List<BranchPromotionConfig> getConfig() {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs1 = null;
        List<BranchPromotionConfig> lst = new ArrayList<BranchPromotionConfig>();
        String sqlMo = "SELECT * FROM BRANCH_PROMOTION_CONFIG ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbvas");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                BranchPromotionConfig conf = new BranchPromotionConfig();
                conf.setBranchPromotionConfigId(rs1.getLong("BRANCH_PROMOTION_CONFIG_ID"));
                conf.setPromotionCode(rs1.getString("PROMOTION_CODE"));
                conf.setMoneyFee(rs1.getLong("MONEY_FEE"));
                conf.setCallSmsOnNetPlan(rs1.getString("CALL_SMS_ONNET_PLAN"));
                conf.setCallOutNetValue(rs1.getString("CALL_OUTNET_VALUE"));
                conf.setSmsOutNetValue(rs1.getString("SMS_OUTNET_VALUE"));
                conf.setInterCallPlan(rs1.getString("INTER_CALL_PLAN"));
                conf.setDataValue(rs1.getString("DATA_VALUE"));
                conf.setDataId(rs1.getString("DATA_ID"));
                conf.setExtendMode(rs1.getString("EXTEND_MODE"));
                conf.setMessageRenew(rs1.getString("MESSAGE_RENEW"));
                lst.add(conf);
                continue;
            }
        } catch (Exception ex) {
            logger.error("ERROR get config ");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        return lst;
    }

    public BranchPromotionConfig getConfigByProduct(String promotionCode) {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs1 = null;
        BranchPromotionConfig bpc = new BranchPromotionConfig();
        String sqlMo = "SELECT * FROM BRANCH_PROMOTION_CONFIG WHERE UPPER(PROMOTION_CODE) = UPPER(?) ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbvas");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, promotionCode);
            rs1 = psMo.executeQuery();
            if (rs1.next()) {
                bpc.setBranchPromotionConfigId(rs1.getLong("BRANCH_PROMOTION_CONFIG_ID"));
                bpc.setPromotionCode(rs1.getString("PROMOTION_CODE"));
                bpc.setMoneyFee(rs1.getLong("MONEY_FEE"));
                bpc.setCallSmsOnNetPlan(rs1.getString("CALL_SMS_ONNET_PLAN"));
                bpc.setCallOutNetValue(rs1.getString("CALL_OUTNET_VALUE"));
                bpc.setSmsOutNetValue(rs1.getString("SMS_OUTNET_VALUE"));
                bpc.setInterCallPlan(rs1.getString("INTER_CALL_PLAN"));
                bpc.setDataValue(rs1.getString("DATA_VALUE"));
                bpc.setDataId(rs1.getString("DATA_ID"));
                bpc.setExtendMode(rs1.getString("EXTEND_MODE"));
                bpc.setMessageRenew(rs1.getString("MESSAGE_RENEW"));
            }
        } catch (Exception ex) {
            logger.error("ERROR get config ");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        return bpc;
    }

    public int updateBranchPromotionSub(BranchPromotionSub kv) {
        Connection connection = null;
        PreparedStatement ps = null;
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbvas");
            ps = connection.prepareStatement(sqlUpdate);
            ps.setString(1, kv.getIsdn());
            result = ps.executeUpdate();
            if (result == 1L) {
                this.logger.info("Update branch_promotion_sub successfully with isdn: " + kv.getIsdn());
                return result;
            }
        } catch (Exception ex) {
            this.logger.error("Update branch_promotion_sub fail");
            this.logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }

    public boolean checkZone(String cellCode, String promotionCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbvas");
            String sql = "select * from branch_promotion_bts where upper(bts_code) like upper(?) and promotion_code like ?";
            ps = connection.prepareStatement(sql);
            cellCode = cellCode.substring(0, cellCode.length() - 1);
            ps.setString(1, "%" + cellCode + "%");
            ps.setString(2, "%" + promotionCode + "%");
            rs = ps.executeQuery();
            while (rs.next()) {
                String tmpBts = rs.getString("bts_code");
                if (tmpBts != null && tmpBts.length() > 0) {
                    result = true;
                }
                break;
            }
            logger.info("End checkZone: result" + result + "cellCode: " + cellCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkZone result").append(result).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public BranchPromotionBts getBtsOnline(String btsCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        BranchPromotionBts result = null;
        String sqlMo = "select * from branch_promotion_bts where UPPER(bts_code) like upper(?)";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbvas");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, "%" + btsCode + "%");
            rs1 = psMo.executeQuery();
            if (rs1.next()) {
                result = new BranchPromotionBts();
                result.setBtsCode(rs1.getString("bts_code"));
                result.setPromotionCode(rs1.getString("promotion_code"));
                result.setBtsType(rs1.getString("bts_type"));
            }
            logTimeDb("Time to getBtsOnline for btsCode " + btsCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getBtsOnline btsCode " + btsCode);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
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
            connection = getConnection("dbSentSMS");
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

    public boolean checkAlreadyWarningInDay(String isdn, String msg) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbSentSMS");
            sql = "select msisdn from mt_his where sent_time > trunc(sysdate) and msisdn = ? and message = ?";
            ps = connection.prepareStatement(sql);
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            ps.setString(1, isdn.trim());
            ps.setString(2, msg.trim());
            rs = ps.executeQuery();
            while (rs.next()) {
                String id = rs.getString("msisdn");
                if (id != null && id.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkAlreadyWarningInDay isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkAlreadyWarningInDay: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" message ")
                    .append(msg)
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

    public boolean checkAlreadyWarningInDayQueue(String isdn, String msg) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbSentSMS");
            sql = "select msisdn from mt where msisdn = ? and message = ?";
            ps = connection.prepareStatement(sql);
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            ps.setString(1, isdn.trim());
            ps.setString(2, msg.trim());
            rs = ps.executeQuery();
            while (rs.next()) {
                String id = rs.getString("msisdn");
                if (id != null && id.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkAlreadyWarningInDayQueue isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkAlreadyWarningInDayQueue: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" message ")
                    .append(msg)
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

    public boolean checkStaffInBranch(String staffCode, String shopPath) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select * from sm.shop where "
                + " shop_id =(select shop_id from sm.staff  "
                + " where staff_code = ? and status =1  )  "
                + " and (shop_path like '%" + shopPath + "%')";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, staffCode);
            rs1 = psMo.executeQuery();
            if (rs1.next()) {
                result = true;
            }
            logTimeDb("Time to checkStaffInBranchGAZ for staff_code " + staffCode + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkStaffInBranchGAZ staff_code " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public String getCell(String staffCode, String cellId, String lacId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement psGetCell = null;
        String cell = "";
        String sqlGetCell = "select cell from cell where ci = TO_NUMBER (util.convert_cell_id (?)) "
                + " and lac = TO_NUMBER (util.convert_lac (?)) and is_delete = 0 and ROWNUM < 2";
//        String sqlGetCell = "select cell from cell where ci = ? and lac = ? and is_delete = 0 and ROWNUM < 2";
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
}
