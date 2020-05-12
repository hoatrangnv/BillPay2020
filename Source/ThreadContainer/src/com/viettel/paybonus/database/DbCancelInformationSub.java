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
import com.viettel.vas.util.obj.DataResources;
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

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbCancelInformationSub extends DbProcessorAbstract {

    private String loggerLabel = DbCancelInformationSub.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDeleteMo = "update cm_pre.sub_profile_info set bonus_status = 1 where sub_profile_id = ?";

    public DbCancelInformationSub() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbCancelInformationSub(String sessionName, Logger logger) throws SQLException, Exception {
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

    public int clearCustIdWhenNotActive(String msisdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update cm_pre.sub_mb set cust_id = null, sub_name = null, birth_date = null, gender = null, province = null, district = null, precinct = null, address = null where isdn = ? and status = 2";
            ps = connection.prepareStatement(sql);
            ps.setString(1, msisdn.trim());
            result = ps.executeUpdate();
            logger.info("End clearCustIdWhenNotActive isdn " + msisdn
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
            return result;
        } catch (Exception ex) {
            logger.error("ERROR clearCustIdWhenNotActive msisdn " + msisdn, ex);
            logger.error(AppManager.logException(startTime, ex));
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateCustIdSubProfileInfo(String isdn, String custId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update cm_pre.sub_profile_info set delete_cust_id = ? where isdn = ? and cust_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, custId);
            ps.setString(2, isdn);
            ps.setString(3, custId);
            result = ps.executeUpdate();
            logger.info("End insertDeleteCustIdSubProfileInfo custId " + custId
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
            return result;
        } catch (Exception ex) {
            logger.error("ERROR insertDeleteCustIdSubProfileInfo custId " + custId, ex);
            logger.error(AppManager.logException(startTime, ex));
            return result;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public long getActionAuditIdSeq() {
        Connection connection = null;
        ResultSet rs = null;
        long actionAuditId = 0;
        String sqlMo = "select seq_action_audit.nextval action_audit_id from dual";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs = psMo.executeQuery();
            while (rs.next()) {
                actionAuditId = rs.getLong("action_audit_id");
                break;
            }
        } catch (Exception ex) {
            logger.error("ERROR getActionAuditIdSeq value: " + actionAuditId);
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        return actionAuditId;
    }

    public int insertActionAudit(Bonus bonus, long actionAuditId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "INSERT INTO ACTION_AUDIT (ACTION_AUDIT_ID,ISSUE_DATETIME,ACTION_CODE,REASON_ID,SHOP_CODE,USER_NAME,PK_TYPE,PK_ID,IP,DESCRIPTION,VALID) \n"
                    + "VALUES(?,sysdate,?,?,?,?,'3',?,NULL,'Delete information of subscriber',NULL)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            ps.setString(2, "619");
            ps.setLong(3, 3387);
            ps.setString(4, "SYSTEM");
            ps.setString(5, "SYSTEM");
            ps.setString(6, bonus.getSubId());
            result = ps.executeUpdate();
            logger.info("End insertActionAudit isdn " + bonus.getIsdnCustomer() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionAudit: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(bonus.getIsdnCustomer())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int deleteRecordActionAudit(long actionAuditId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "DELETE FROM ACTION_AUDIT WHERE ACTION_AUDIT_ID = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            result = ps.executeUpdate();
            logger.info("End deleteRecordActionAudit actionAuditId " + actionAuditId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR deleteRecordActionAudit: ").
                    append(sql).append("\n")
                    .append(" actionAuditId ")
                    .append(actionAuditId);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertActionDetail(Bonus bonus, long actionAuditId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "INSERT INTO ACTION_DETAIL (ACTION_AUDIT_ID,ISSUE_DATETIME,COL_NAME,OLD_VALUE,NEW_VALUE,ACTION_DETAIL_ID,TABLE_NAME,ROW_ID) \n"
                    + "VALUES(?,sysdate,?,?,?,seq_action_detail.nextval,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            ps.setString(2, "CUST_ID");
            ps.setLong(3, bonus.getPkId());
            ps.setString(4, "");
            ps.setString(5, "SUB_MB");
            ps.setString(6, bonus.getSubId());
            result = ps.executeUpdate();
            logger.info("End insertActionDetail isdn " + bonus.getIsdnCustomer() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionDetail: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(bonus.getIsdnCustomer())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    @Override
    public Record parse(ResultSet rs) {
        Bonus record = new Bonus();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("sub_profile_id"));
            record.setActionAuditId(rs.getLong("action_audit_id"));
            record.setSubId(String.valueOf(rs.getLong("sub_id")));
            record.setUserName(rs.getString("create_staff"));
            record.setActionProfileId(rs.getLong("sub_profile_id"));
            record.setReceiverDate(rs.getTimestamp("check_time"));
            record.setTimeCheck(rs.getTimestamp("check_time"));
            record.setShopCode(rs.getString("create_shop"));
            record.setActionCode(rs.getString("action_code"));
            record.setReasonId(rs.getLong("reason_id"));
            record.setIssueDateTime(rs.getTimestamp("create_time"));
            record.setCheckInfo(rs.getString("check_status"));
            record.setCheckComment(rs.getString("check_commend"));
            record.setStaffCheck(rs.getString("assigned_user"));
            record.setBonusStatus(rs.getString("BONUS_STATUS"));
            record.setIsdnCustomer(rs.getString("isdn"));
            //LinhNBV start modified on April 13 2018: Set custId = pkId
            record.setPkId(rs.getLong("cust_id"));
            //end.
            record.setResultCode("0");
            record.setDescription("Start Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse SubProfileInfo");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
//        long timeStart = System.currentTimeMillis();
//        PreparedStatement ps = null;
//        Connection connection = null;
//        String batchId = "";
//        try {
//            connection = ConnectionPoolManager.getConnection(dbNameCofig);
//            ps = connection.prepareStatement(sqlDeleteMo);
//            for (Record rc : listRecords) {
//                Bonus sd = (Bonus) rc;
//                batchId = sd.getBatchId();
//                ps.setLong(1, sd.getId());
//                ps.addBatch();
//            }
//            return ps.executeBatch();
//        } catch (Exception ex) {
//            logger.error("ERROR updateQueue SUB_PROFILE_INFO batchid " + batchId, ex);
//            logger.error(AppManager.logException(timeStart, ex));
//            return null;
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            logTimeDb("Time to updateQueue SUB_PROFILE_INFO, batchid " + batchId, timeStart);
//        }
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
//        List<ParamList> listParam = new ArrayList<ParamList>();
//        String batchId = "";
//        long timeSt = System.currentTimeMillis();
//        try {
//            for (Record rc : listRecords) {
//                Bonus sd = (Bonus) rc;
//                batchId = sd.getBatchId();
//                ParamList paramList = new ParamList();
//                paramList.add(new Param(Bonus.PK_TYPE, sd.getPkType(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(Bonus.PK_ID, sd.getId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param(Bonus.ACTION_CODE, sd.getActionCode(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(Bonus.REASON_ID, sd.getReasonId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param(Bonus.ACTION_AUDIT_ID, sd.getActionAuditId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param(Bonus.ISDN, sd.getIsdnCustomer(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(Bonus.ACTIVE_STATUS, sd.getActiveStatus(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(Bonus.PRODUCT_CODE, sd.getProductCode(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(Bonus.STAFF_CODE, sd.getStaffCode(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(Bonus.CHANNEL_TYPE_ID, sd.getChannelTypeId(), Param.DataType.INT, Param.IN));
//                paramList.add(new Param(Bonus.ITEM_FEE_ID, sd.getItemFeeId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param(Bonus.AMOUNT, sd.getAmount(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param(Bonus.RESULT_CODE, sd.getResultCode(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(Bonus.DESCRIPTION, sd.getDescription(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(Bonus.DATE_ACTION, sd.getIssueDateTime(), Param.DataType.TIMESTAMP, Param.IN));
//                paramList.add(new Param(Bonus.DURATION, sd.getDuration(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param(Bonus.EWALLET_ERROR_CODE, sd.geteWalletErrCode(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(Bonus.ID_BATCH, sd.getIdBatch(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(Bonus.TOTAL_CURRENT_VALUE, sd.getTotalCurrentValue(), Param.DataType.INT, Param.IN));
//                paramList.add(new Param(Bonus.TOTAL_CURRENT_TIMES, sd.getTotalCurrentAddTimes(), Param.DataType.INT, Param.IN));
//                paramList.add(new Param(Bonus.ACTION_PROFILE_ID, sd.getActionProfileId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param(Bonus.AGENT_ID, sd.getAgentId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param(Bonus.AGENT_ISDN, sd.getAgentIsdn(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(Bonus.ACCOUNT_ID, sd.getAccountId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param("check_staff", sd.getStaffCheck(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("check_info", sd.getCheckInfo(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("check_time", sd.getTimeCheck(), Param.DataType.TIMESTAMP, Param.IN));
//                listParam.add(paramList);
//            }
//            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "CHECK_ACTIVE_PROCESS");
//            logTimeDb("Time to insertQueueHis CHECK_ACTIVE_PROCESS, batchid " + batchId + " total result: " + res.length, timeSt);
//            return res;
//        } catch (Exception ex) {
//            logger.error("ERROR insertQueueHis CHECK_ACTIVE_PROCESS batchid " + batchId, ex);
//            logger.error(AppManager.logException(timeSt, ex));
//            return null;
//        }
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        /*
         * insert into MT(MT_ID,MO_HIS_ID,MSISDN,MESSAGE,RECEIVE_TIME,RETRY_NUM,CHANNEL) "
         + "values(MT_SEQ.NEXTVAL, ?, ?, ?, sysdate, 0, ?)
         */
//        List<ParamList> listParam = new ArrayList<ParamList>();
//        String batchId = "";
//        int[] res = new int[0];
//        long timeSt = System.currentTimeMillis();
//        try {
//            String countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
//            String sms_code = ResourceBundle.getBundle("configPayBonus").getString("sms_code");
//            for (Record rc : listRecords) {
//                Bonus sd = (Bonus) rc;
//                if (sd.getMessage() == null || sd.getIsdn() == null || sd.getMessage().trim().length() <= 0
//                        || sd.getIsdn().trim().length() <= 0) {
//                    continue;
//                }
//                batchId = sd.getBatchId();
//                ParamList paramList = new ParamList();
//                paramList.add(new Param("MO_HIS_ID", sd.getActionAuditId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param("MT_ID", "MT_SEQ.NEXTVAL", Param.DataType.CONST, Param.IN));
//                paramList.add(new Param("MSISDN", countryCode + sd.getIsdn(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("MESSAGE", sd.getMessage(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("CHANNEL", sms_code, Param.DataType.CONST, Param.IN));
//                listParam.add(paramList);
//            }
//            if (listParam.size() > 0) {
//                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "MT");
//                logTimeDb("Time to insertQueueOutput MT, batchid " + batchId + " total result: " + res.length, timeSt);
//            } else {
//                logTimeDb("List Record to insert Queue Output is empty, batchid " + batchId, timeSt);
//            }
//            return res;
//        } catch (Exception ex) {
//            logger.error("ERROR insertQueueOutput batchid " + batchId, ex);
//            logger.error(AppManager.logException(timeSt, ex));
//            return null;
//        }
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
//        List<ParamList> listParam = new ArrayList<ParamList>();
//        StringBuilder sb = new StringBuilder();
//        long timeSt = System.currentTimeMillis();
//        try {
//            //            The first delete queue timeout
//            deleteQueueTimeout(ids);
//            for (String sd : ids) {
//                sb.append(":" + sd);
//                ParamList paramList = new ParamList();
//                paramList.add(new Param(Bonus.ACTION_AUDIT_ID, Long.valueOf(sd), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param(Bonus.RESULT_CODE, "FW_99", Param.DataType.STRING, Param.IN));
//                paramList.add(new Param(Bonus.DESCRIPTION, "FW_Timeout", Param.DataType.STRING, Param.IN));
//                listParam.add(paramList);
//            }
//            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "BONUS_PROCESS");
//            logTimeDb("Time to processTimeoutRecord, insert BONUS_PROCESS, total result: " + res.length, timeSt);
//            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
//        } catch (Exception ex) {
//            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
//            try {
//                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "BONUS_PROCESS");
//                logTimeDb("Time to retry processTimeoutRecord, insert BONUS_PROCESS, total result: " + res.length, timeSt);
//            } catch (Exception ex1) {
//                logger.error("ERROR retry processTimeoutRecord ", ex1);
//                logger.error(AppManager.logException(timeSt, ex));
//            }
//        }
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public SubInfo getSubInfoBySubId(long subId, String ewalletIsdn) {
        /**
         * SELECT product_code, isdn, sta_datetime, status, act_status FROM
         * cm_pre.sub_mb WHERE sub_id = p_sub_id and status = 02;
         */
        ParamList paramList = new ParamList();
        SubInfo subInfo = null;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("sub_id", subId, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("STATUS", "2", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("product_code", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("isdn", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("act_status", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("sta_datetime", null, Param.DataType.DATE, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "cm_pre.sub_mb");
            subInfo = new SubInfo();
            while (rs.next()) {
                String productCode = rs.getString("product_code");
                String isdn = rs.getString("isdn");
                String actStatus = rs.getString("act_status");
                Date activeDate = rs.getDate("sta_datetime");
                if (productCode != null && productCode.trim().length() > 0) {
                    subInfo.setIsdnSub(isdn);
                    subInfo.setProductCode(productCode);
                    subInfo.setActStatus(actStatus);
                    subInfo.setActiveDate(activeDate);
                    break;
                }
            }
            logTimeDb("Time to getSubInfoBySubId for subid " + subId + " productCode "
                    + subInfo.getProductCode() + " isdn " + ewalletIsdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSubInfoBySubId subId " + subId + " isdn " + ewalletIsdn);
            logger.error(AppManager.logException(timeSt, ex));
        }
        return subInfo;
    }

    public boolean checkSubByCustId(long custId, String isdn) {
        /**
         * SELECT name FROM cm_pre.customer WHERE cust_id = p_cust_id;
         */
        ParamList paramList = new ParamList();
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("cust_id", custId, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("name", null, Param.DataType.STRING, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "cm_pre.customer");
            while (rs.next()) {
                String name = rs.getString("name");
                if (name != null && name.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkSubByCustId for cusid " + custId + " isdn " + isdn + " result " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkSubByCustId cusid " + custId + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        }
        return result;
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean checkAlreadyProcessRecord(long idRecord) {
        /**
         * SELECT action_audit_id FROM BONUS_PROCESS WHERE action_audit_id =
         * idRecord
         */
        ParamList paramList = new ParamList();
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("action_audit_id", idRecord, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("result_code", "0", Param.DataType.STRING, Param.IN));
            paramList.add(new Param("action_audit_id", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "profile.BONUS_PROCESS");
            while (rs.next()) {
                long id = rs.getLong("action_audit_id");
                if (id > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkAlreadyProcessRecord idRecord " + idRecord + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAlreadyProcessRecord defaul return false" + idRecord);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        }
        return result;
    }

    public boolean checkActionReason(String actionCode, long reasonId, long actionAuditId) {
        /**
         * SELECT * FROM action_reason WHERE action_code = ? AND reason_id = ?
         * AND prepaid = 1 AND req_bonus = 1 idRecord
         */
        ParamList paramList = new ParamList();
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("action_code", actionCode, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("reason_Id", reasonId, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("prepaid", "1", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("req_bonus", "1", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("action_reason_id", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "profile.action_reason");
            while (rs.next()) {
                long id = rs.getLong("action_reason_id");
                if (id > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkActionReason actionId " + actionAuditId + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkActionReason defaul return false" + actionAuditId);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        }
        return result;
    }

    public String getActionCode(long pkId, long actionId) {
        /**
         * select action_code from action where action_id = ? and status = 1;
         */
        ParamList paramList = new ParamList();
        String ationCode = "";
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("action_id", actionId, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("status", "1", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("action_code", null, Param.DataType.STRING, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "profile.action");
            while (rs.next()) {
                ationCode = rs.getString("action_code");
            }
            logTimeDb("Time to getActionCode for actionauditid " + pkId + " action_code " + ationCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getActionCode for actionauditid " + pkId + " action_code " + ationCode);
            logger.error(AppManager.logException(timeSt, ex));
        }
        return ationCode;
    }

    public long getActionId(long pkId, String actionCode) {
        /**
         * select action_id from action where action_code = ? and status = 1;
         */
        ParamList paramList = new ParamList();
        long ationId = 0;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("action_id", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("status", "1", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("action_code", actionCode, Param.DataType.STRING, Param.IN));
            DataResources rs = poolStore.selectTable(paramList, "profile.action");
            while (rs.next()) {
                ationId = rs.getLong("action_id");
            }
            logTimeDb("Time to getActionId for actionauditid " + pkId + " actionCode " + actionCode + " ationId " + ationId, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getActionId for actionauditid " + pkId + " actionCode " + actionCode);
            logger.error(AppManager.logException(timeSt, ex));
        }
        return ationId;
    }

    public AccountAgent getAccountAgentByUser(String staffCode) {
        /**
         * select * from account_agent where upper(owner_code) = ? and status =
         * 1;
         */
        ParamList paramList = new ParamList();
        AccountAgent agent = null;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("owner_code", staffCode, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("STATUS", 1, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("owner_code", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("account_id", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("AGENT_ID", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("isdn", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("account_type", null, Param.DataType.INT, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "sm.account_agent");
            agent = new AccountAgent();
            while (rs.next()) {
                String code = rs.getString("owner_code");
                long accountId = rs.getLong("account_id");
                long agentid = rs.getLong("AGENT_ID");
                String isdn = rs.getString("isdn");
                int channelTypeId = rs.getInt("account_type");
                if (isdn != null && isdn.trim().length() > 0) {
                    agent.setStaffCode(code);
                    agent.setAccountId(accountId);
                    agent.setAgentId(agentid);
                    agent.setIsdn(isdn);
                    agent.setChannelTypeId(channelTypeId);
                    break;
                }
            }
            logTimeDb("Time to getAccountAgentByUser staffCode " + staffCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getAccountAgentByUser: " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        }
        return agent;
    }

    public int[] deleteQueueTimeout(List<String> listId) {
//        long timeStart = System.currentTimeMillis();
//        PreparedStatement ps = null;
//        Connection connection = null;
//        StringBuilder sf = new StringBuilder();
//        try {
//            connection = ConnectionPoolManager.getConnection(dbNameCofig);
//            ps = connection.prepareStatement(sqlDeleteMo);
//            sf.setLength(0);
//            for (String id : listId) {
//                ps.setLong(1, Long.valueOf(id));
//                ps.addBatch();
//                sf.append(id);
//                sf.append(", ");
//            }
//            return ps.executeBatch();
//        } catch (Exception ex) {
//            logger.error("ERROR deleteQueueTimeout SUB_PROFILE_INFO listId " + sf.toString(), ex);
//            logger.error(AppManager.logException(timeStart, ex));
//            return null;
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            logTimeDb("Time to deleteQueueTimeout SUB_PROFILE_INFO, listId " + sf.toString(), timeStart);
//        }
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int sendSms(String msisdn, String message, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp2");
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
