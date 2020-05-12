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
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
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
public class DbPromotionScaner extends DbProcessorAbstract {

    private String loggerLabel = DbPromotionScaner.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbPromotionScaner() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbPromotionScaner(String sessionName, Logger logger) throws SQLException, Exception {
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
        BranchPromotionSub record = new BranchPromotionSub();
        long timeSt = System.currentTimeMillis();
        try {
            record.setActionAuditId(rs.getLong("mo_id"));
            record.setIsdn(rs.getString("msisdn"));
            record.setCreateTime(rs.getTimestamp("create_time"));
            record.setCreateUser(rs.getString("CREATE_STAFF"));
            record.setProductCode(rs.getString("promotion_code"));
            record.setCountProcess(rs.getLong("count_process"));
            record.setLastProcess(rs.getTimestamp("last_process"));
            record.setActionType(rs.getInt("action_type") + "");
            record.setResultCode("0");
            record.setDescription("Start Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse bonus_connect_kit");
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

    public int[] updatePromotionQueue(List<BranchPromotionSub> lstBn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int[] res = new int[0];
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbvas");
            sql = "UPDATE branch_promotion_queue SET count_process = ?, last_process = sysdate WHERE mo_id = ? ";
            ps = connection.prepareStatement(sql);
            for (BranchPromotionSub bn : lstBn) {
                ps.setLong(1, bn.getCountProcess());
                ps.setLong(2, bn.getActionAuditId());
                ps.addBatch();
            }
            res = ps.executeBatch();
            logger.info("End updatePromotionQueue "
                    + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updatePromotionQueue id ");
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
            connection = getConnection("dbvas");
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

    public int insertMoHis(BranchPromotionSub bn) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        long timeSt = System.currentTimeMillis();
        try {
            ParamList paramList = new ParamList();
            paramList.add(new Param("MO_HIS_ID", bn.getActionAuditId(), Param.DataType.LONG, Param.IN));
            paramList.add(new Param("MSISDN", bn.getIsdn(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("CHANNEL", "155", Param.DataType.STRING, Param.IN));
            paramList.add(new Param("COMMAND", bn.getProductCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("PARAM", bn.getCreateUser(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("RECEIVE_TIME", bn.getCreateTime(), Param.DataType.TIMESTAMP, Param.IN));
            paramList.add(new Param("ACTION_TYPE", Integer.valueOf(bn.getActionType()), Param.DataType.INT, Param.IN));
            paramList.add(new Param("PROCESS_TIME", "sysdate", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("ERR_CODE", bn.getResultCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("NODE_NAME", bn.getNodeName(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("CLUSTER_NAME", bn.getClusterName(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("CHANNEL_TYPE", "Scaner", Param.DataType.STRING, Param.IN));
            paramList.add(new Param("DESCRIPTION", bn.getDescription(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("fee", bn.getMoneyFee(), Param.DataType.LONG, Param.IN));
            listParam.add(paramList);
            if (poolStore == null) {
                poolStore = new PoolStore("dbvas", logger);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "mo_his");
            logTimeDb("Time to insertMoHis, " + bn.getIsdn() + " result: " + res[0], timeSt);
            return res[0];
        } catch (Exception ex) {
            logger.error("ERROR insertMoHis " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeSt, ex));
            return 0;
        }
    }

    public int deletePromotionQueue(BranchPromotionSub bn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        int res = 0;
        try {
            connection = connection = getConnection("dbvas");
            ps = connection.prepareStatement("delete branch_promotion_queue where mo_id = ?");
            ps.setLong(1, bn.getActionAuditId());
            res = ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("ERROR deletePromotionQueue id " + bn.getActionAuditId() + " isdn " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeStart, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deletePromotionQueue, id " + bn.getActionAuditId() + " isdn " + bn.getIsdn() + " result " + res, timeStart);
            return res;
        }
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
                conf.setMessage(rs1.getString("MESSAGE"));
                conf.setCommision(rs1.getString("commision"));
                lst.add(conf);
                continue;
            }
        } catch (Exception ex) {
            logger.error("ERROR get config ");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return lst;
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

    public int insertBranchPromotionSub(Long actionAuditId, String isdn, String createUser, String productCode, String expireTime) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbvas");
            sql = "insert into branch_promotion_sub (action_audit_id, isdn, create_user, create_time, product_code, expire_time)\n"
                    + "values (?,?,?,sysdate,?,to_date(?,'yyyyMMddHH24miss'))";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            ps.setString(2, isdn);
            ps.setString(3, createUser);
            ps.setString(4, productCode);
            ps.setString(5, expireTime);
            result = ps.executeUpdate();
            logger.info("End insertBranchPromotionSub mo_id " + actionAuditId + " isdn: " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertBranchPromotionSub: ").
                    append(sql).append("\n")
                    .append(" mo_id ")
                    .append(actionAuditId)
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

    public String getIsdnWallet(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String isdnWallet = "";
        try {
            connection = getConnection("dbsm");
            ps = connection.prepareStatement(
                    //LinhNBV start modified on April 12 2018: Add condition to check profile of channel correct or not
                    "select isdn_wallet from sm.staff where status = 1 and lower(staff_code) = lower(?)");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setString(1, staffCode.toUpperCase());
            rs = ps.executeQuery();
            while (rs.next()) {
                isdnWallet = rs.getString("isdn_wallet");
                break;
            }
            logTimeDb("Time to getIsdnWallet isdn_wallet: " + isdnWallet, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getIsdnWallet default return false");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return isdnWallet;
        }
    }

    public int insertEwalletLog(EwalletLog log) {

        ParamList paramList = new ParamList();
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("ACTION_AUDIT_ID", log.getAtionAuditId(), Param.DataType.LONG, Param.IN));
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
            paramList.add(new Param("BONUS_TYPE", 4L, Param.DataType.LONG, Param.IN));
            PoolStore mPoolStore = new PoolStore("dbProfile", logger);
            PoolStore.PoolResult prs = mPoolStore.insertTable(paramList, "EWALLET_LOG");
            logTimeDb("Time to insertEwalletLog isdn " + log.getMobile(), timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getMobile());
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public String getTelByStaffCode(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String tel = null;
        String sqlMo = " select cellphone from vsa_v3.users where lower(user_name) = lower(?) ";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setString(1, staffCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                tel = rs1.getString("cellphone");
            }
            if (tel == null) {
                tel = "";
                logger.info("tel is null - staff_code: " + staffCode);
            }
        } catch (Exception ex) {
            logger.error("ERROR getTelByStaffCode " + staffCode);
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return tel;
    }

    public String getOwnerStaffOfChannel(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String result = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select staff_code from staff where status = 1 and staff_id = (select staff_owner_id from staff where upper(staff_code) = upper(?))";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getString("staff_code");
                break;
            }
            logger.info("End getOwnerStaffOfChannel: staffCode" + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getOwnerStaffOfChannel ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            result = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkMovitelStaff(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        try {
            connection = getConnection("dbsm");
            ps = connection.prepareStatement(
                    //LinhNBV start modified on April 12 2018: Add condition to check profile of channel correct or not
                    "select staff_code from sm.staff where status = 1 and channel_type_id = 14 and lower(staff_code) = lower(?)");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setString(1, staffCode.toUpperCase());
            rs = ps.executeQuery();
            while (rs.next()) {
                String channel = rs.getString("staff_code");
                if (channel != null && channel.trim().length() > 0) {
                    result = true;
                }
                break;
            }
            logTimeDb("Time to checkMovitelStaff " + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkMovitelStaff defaul return false");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertSubRelProduct(String isdn, String vasCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into Sub_Rel_Product (sub_rel_product_id, sub_id, sta_datetime, reg_date, is_connected, main_product_code, rel_product_code, status)\n"
                    + " values (seq_sub_rel_product.nextval, (select sub_id from sub_mb where isdn = ? and status = 2), sysdate, sysdate, 1,\n"
                    + " (select product_code from sub_mb where isdn = ? and status = 2), ?,1)";
            ps = connection.prepareStatement(sql);
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            ps.setString(1, isdn);
            ps.setString(2, isdn);
            ps.setString(3, vasCode);
            result = ps.executeUpdate();
            logger.info("End insertSubRelProduct isdn " + isdn + " vasCode: " + vasCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSubRelProduct: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" vasCode ")
                    .append(vasCode)
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
