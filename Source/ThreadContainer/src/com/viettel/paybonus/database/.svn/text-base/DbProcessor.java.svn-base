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
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.DataResources;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlCheckProfile = "SELECT count(*) as soluong, action_profile_id "
            + "FROM  profile.action_profile_record  WHERE action_profile_id = ( "
            + "SELECT Action_Profile_ID FROM profile.Action_Profile "
            + "WHERE action_audit_id = ? and record_status = 1 and "
            + "reason_id = 2926 ) group by action_profile_id";
    private String callChangeCommAccount = "{call sm.change_comm_account(?,?,?,?,?,?,?,?,?,?,?)}";
    private String sqlDeleteMo = "delete bonus_input where action_audit_id = ?";
    private PreparedStatement psCheckProfile;

    public DbProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbProcessor(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
        poolStore = new PoolStore(sessionName, logger);
    }

    public HashMap<String, String> getListConfig(String moduleName) {
        /**
         * select PARAM_NAME, PARAM_VALUE, DEFAULT_VALUE from CONFIG where
         * upper(module)=?
         */
        ParamList paramList = new ParamList();
        HashMap<String, String> listConfig = null;
        try {
            paramList.add(new Param("PARAM_NAME", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("PARAM_VALUE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("DEFAULT_VALUE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("upper(module)", moduleName, Param.DataType.STRING, Param.IN));
            long timeSt = System.currentTimeMillis();
            DataResources rs = poolStore.selectTable(paramList, "CONFIG");
            logTimeDb("Time to select CONFIG", timeSt);

            String paramValue = "";
            listConfig = new HashMap<String, String>();
            while (rs.next()) {
                paramValue = rs.getString("PARAM_VALUE");
                if (paramValue == null || paramValue.trim().length() == 0) {
                    paramValue = rs.getString("DEFAULT_VALUE");
                }
                listConfig.put(rs.getString("PARAM_NAME").trim().toLowerCase(), paramValue.trim());
            }
        } catch (Exception ex) {
            logger.error("ERROR select CONFIG: " + moduleName, ex);

            try {
                long timeSt = System.currentTimeMillis();
                DataResources rs = poolStore.selectTable(paramList, "CONFIG");
                logTimeDb("Time to select CONFIG", timeSt);

                String paramValue = "";
                listConfig = new HashMap<String, String>();
                while (rs.next()) {
                    paramValue = rs.getString("PARAM_VALUE");
                    if (paramValue == null || paramValue.trim().length() == 0) {
                        paramValue = rs.getString("DEFAULT_VALUE");
                    }
                    listConfig.put(rs.getString("PARAM_NAME").trim().toLowerCase(), paramValue.trim());
                }
            } catch (Exception ex1) {
                logger.error("ERROR retry select CONFIG: " + moduleName, ex1);
            }
        }
        return listConfig;
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
        Bonus record = new Bonus();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong(Bonus.ACTION_AUDIT_ID));
            record.setActionAuditId(rs.getLong(Bonus.ACTION_AUDIT_ID));
//            record.setPkId(rs.getLong(Bonus.PK_ID)); //only use with PayBonusInActionAudit
            record.setPkId(rs.getLong(Bonus.SUB_ID));
//            record.setPkType(rs.getString(Bonus.PK_TYPE)); //only use with PayBonusInActionAudit
//            record.setUserName(rs.getString(Bonus.USER_NAME)); //only use with PayBonusInActionAudit
            record.setUserName(rs.getString(Bonus.STAFF_CODE));
            record.setActionProfileId(rs.getLong(Bonus.ACTION_PROFILE_ID));
            record.setReceiverDate(rs.getTimestamp(Bonus.RECEIVE_DATE));
            record.setShopCode(rs.getString(Bonus.SHOP_CODE));
            record.setActionId(rs.getLong(Bonus.ACTION_ID));
            record.setReasonId(rs.getLong(Bonus.REASON_ID));
            record.setIssueDateTime(rs.getTimestamp(Bonus.ISSUE_DATETIME));
            record.setCheckInfo(rs.getString(Bonus.CHECK_INFO));
            record.setIsdnCustomer(rs.getString(Bonus.ISDN_ACCOUNT));
            record.setStaffCheck(rs.getString("staff_checked"));
            record.setTimeCheck(rs.getTimestamp("checked_datetime"));
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
            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                Bonus sd = (Bonus) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getActionAuditId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue BONUS_INPUT batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue BONUS_INPUT, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                Bonus sd = (Bonus) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param(Bonus.PK_TYPE, sd.getPkType(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.PK_ID, sd.getPkId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.ACTION_CODE, sd.getActionCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.REASON_ID, sd.getReasonId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.ACTION_AUDIT_ID, sd.getActionAuditId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.ISDN, sd.getIsdnCustomer(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.ACTIVE_STATUS, sd.getActiveStatus(), Param.DataType.STRING, Param.IN));
//                if (sd.getActiveDate() != null) {
//                    paramList.add(new Param(Bonus.ACTIVE_DATE, sd.getActiveDate(), Param.DataType.DATE, Param.IN));
//                }
                paramList.add(new Param(Bonus.PRODUCT_CODE, sd.getProductCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.STAFF_CODE, sd.getStaffCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.CHANNEL_TYPE_ID, sd.getChannelTypeId(), Param.DataType.INT, Param.IN));
                paramList.add(new Param(Bonus.ITEM_FEE_ID, sd.getItemFeeId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.AMOUNT, sd.getAmount(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.RESULT_CODE, sd.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.DESCRIPTION, sd.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.DATE_ACTION, sd.getIssueDateTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param(Bonus.DURATION, sd.getDuration(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.EWALLET_ERROR_CODE, sd.geteWalletErrCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.ID_BATCH, sd.getIdBatch(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.TOTAL_CURRENT_VALUE, sd.getTotalCurrentValue(), Param.DataType.INT, Param.IN));
                paramList.add(new Param(Bonus.TOTAL_CURRENT_TIMES, sd.getTotalCurrentAddTimes(), Param.DataType.INT, Param.IN));
                paramList.add(new Param(Bonus.ACTION_PROFILE_ID, sd.getActionProfileId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.RECEIVE_DATE, sd.getReceiverDate(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param(Bonus.AGENT_ID, sd.getAgentId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.AGENT_ISDN, sd.getAgentIsdn(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.ACCOUNT_ID, sd.getAccountId(), Param.DataType.LONG, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "BONUS_PROCESS");
            logTimeDb("Time to insertQueueHis BONUS_PROCESS, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis BONUS_PROCESS batchid " + batchId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return null;
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
                Bonus sd = (Bonus) rc;
                if (sd.getMessage() == null || sd.getIsdn() == null || sd.getMessage().trim().length() <= 0
                        || sd.getIsdn().trim().length() <= 0) {
                    continue;
                }
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("MO_HIS_ID", sd.getActionAuditId(), Param.DataType.LONG, Param.IN));
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
            logger.error(AppManager.logException(timeSt, ex));
            return null;
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
            //            The first delete queue timeout
            deleteQueueTimeout(ids);
            for (String sd : ids) {
                sb.append(":" + sd);
                ParamList paramList = new ParamList();
                paramList.add(new Param(Bonus.ACTION_AUDIT_ID, Long.valueOf(sd), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.RESULT_CODE, "FW_99", Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.DESCRIPTION, "FW_Timeout", Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "BONUS_PROCESS");
            logTimeDb("Time to processTimeoutRecord, insert BONUS_PROCESS, total result: " + res.length, timeSt);
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
            try {
                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "BONUS_PROCESS");
                logTimeDb("Time to retry processTimeoutRecord, insert BONUS_PROCESS, total result: " + res.length, timeSt);
            } catch (Exception ex1) {
                logger.error("ERROR retry processTimeoutRecord ", ex1);
                logger.error(AppManager.logException(timeSt, ex));
            }
        }
    }

    public Agent getAgentInfoByUser(String staffCode) {
        /**
         * SELECT staff_id, isdn_wallet, channel_type_id FROM sm.staff WHERE
         * staff_code = ? and status = 1 AND ROWNUM < 2;
         */
        ParamList paramList = new ParamList();
        Agent agent = null;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("STAFF_CODE", staffCode.trim().toUpperCase(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("STATUS", 1, Param.DataType.LONG, Param.IN));
//            paramList.add(new Param("channel_wallet", "", Param.OperatorType.IS_NOT_NULL, Param.DataType.CONST, Param.IN));
//            paramList.add(new Param("isdn_wallet", "", Param.OperatorType.IS_NOT_NULL, Param.DataType.CONST, Param.IN));
            paramList.add(new Param("tel", "", Param.OperatorType.IS_NOT_NULL, Param.DataType.CONST, Param.IN));
            paramList.add(new Param("STAFF_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("STAFF_ID", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("tel", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("isdn_wallet", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("channel_type_id", null, Param.DataType.INT, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "sm.staff");
            agent = new Agent();
            while (rs.next()) {
                String code = rs.getString("STAFF_CODE");
                String id = rs.getString("STAFF_ID");
                String isdn = rs.getString("tel");
                String isdnEmola = rs.getString("isdn_wallet");
                int channelTypeId = rs.getInt("channel_type_id");
                if (isdn != null && isdn.trim().length() > 0) {
                    agent.setStaffCode(code);
                    agent.setStaffId(Long.valueOf(id));
                    agent.setIsdnWallet(isdnEmola);
                    agent.setChannelTypeId(channelTypeId);
                    break;
                }
            }
            logTimeDb("Time to getAgentInfoByUser staffCode " + staffCode + " isdnEmola: " + agent.getIsdnWallet(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getAgentInfoByUser: " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        }
        return agent;
    }

    public boolean checkAllowChannel(int channelId, String isdn) {
        /**
         * SELECT COUNT (1) FROM ba_channel_config WHERE channel_type_id =
         * p_channel_id AND status = general_status_off; IF (v_count > 0) RETURN
         * result_fail;
         */
        ParamList paramList = new ParamList();
        boolean result = true;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("channel_type_id", channelId, Param.DataType.INT, Param.IN));
            paramList.add(new Param("STATUS", "0", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("channel_type_id", null, Param.DataType.INT, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "ba_channel_config");
            while (rs.next()) {
                String id = rs.getString("channel_type_id");
                if (id != null && id.trim().length() > 0) {
                    result = false;
                    break;
                }
            }
            logTimeDb("Time to checkAllowChannel channelId " + channelId + " isdn " + isdn + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAllowChannel default return false " + channelId + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        }
        return result;
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

    public boolean checkProductAllow(String productCode, String isdn) {
        /**
         * SELECT product_code FROM ba_product_config WHERE product_code =
         * p_product_code AND status = 0;
         */
        ParamList paramList = new ParamList();
        boolean result = true;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("product_code", productCode, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("STATUS", "0", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("product_code", null, Param.DataType.STRING, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "ba_product_config");
            while (rs.next()) {
                String product = rs.getString("product_code");
                if (product != null && product.trim().length() > 0) {
                    result = false;
                    break;
                }
            }
            logTimeDb("Time to checkProductAllow productCode " + productCode + ", isdn " + isdn + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkProductAllow default return false " + productCode + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        }
        return result;
    }

    public ItemFee getItemFee(int channelId, String productCode, String actionCode, long reasonId, String isdn) {
        /**
         * SELECT bif.item_fee_id, bif.amount FROM ba_item_fee bif,
         * ba_action_reason_fee arf, action_reason ar WHERE bif.channel_type_id
         * = p_channel_type_id AND bif.status = 1 AND arf.item_fee_id =
         * bif.item_fee_id AND arf.action_reason_id = ar.action_reason_id AND
         * ar.action_code = p_action_code AND ar.reason_id = p_reason_id AND
         * ar.prepaid = p_prepaid AND bif.product_code = p_product_code AND
         * ROWNUM <= 1;
         */
        ParamList paramList = new ParamList();
        ItemFee itemfee = null;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("bif.channel_type_id", channelId, Param.DataType.INT, Param.IN));
            paramList.add(new Param("bif.status", 1, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("arf.item_fee_id", "bif.item_fee_id", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("arf.action_reason_id", "ar.action_reason_id", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("ar.action_code", actionCode, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("ar.reason_id", reasonId, Param.DataType.LONG, Param.IN));
            if (productCode != null && productCode.length() > 0) {
                paramList.add(new Param("bif.product_code", productCode, Param.DataType.STRING, Param.IN));
            }
            paramList.add(new Param("ROWNUM", 1, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("bif.item_fee_id as itemFeeId", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("bif.amount as amount", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("ar.check_profile as checkProfile", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "ba_item_fee bif, ba_action_reason_fee arf, action_reason ar");
            itemfee = new ItemFee();
            while (rs.next()) {
                long itemFeeId = rs.getLong("itemFeeId");
                long amount = rs.getLong("amount");
                long checkProfile = rs.getLong("checkProfile");
                if (amount > 0) {
                    itemfee.setItemFeeId(itemFeeId);
                    itemfee.setAmount(amount);
                    itemfee.setCheckProfile(checkProfile);
                    break;
                }
            }
            logTimeDb("Time to getItemFee for isdn " + isdn + " productCode " + productCode
                    + " channelId " + channelId + " actioncode" + actionCode + " reasonid " + reasonId
                    + " amount " + itemfee.getAmount() + " checkProfile " + itemfee.getCheckProfile(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSubInfoBySubId isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        }
        return itemfee;
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
//        List<ParamList> listParam = new ArrayList<ParamList>();
//        String batchId = "";
//        long timeSt = System.currentTimeMillis();
//        Date lastDateAction = null;
//        SimpleDateFormat sdf;
//        try {
//            sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
////            In sqlMo must order by asc issue_date of action_audit, to get last issue_date for each batch.
//            if (lrc == null || lrc.size() <= 0) {
////                Not have recort, return
//                logger.info("Not have recort, so do not update lasttime process");
//                return;
//            } else {
//                Bonus lastBonus = (Bonus) lrc.get(lrc.size() - 1);
//                batchId = lastBonus.getBatchId();
//                if (lastBonus.getIssueDateTime() != null) {
//                    lastDateAction = lastBonus.getIssueDateTime();
//                } else {
//                    logger.warn("Can not get lastDateAction " + batchId);
//                    return;
//                }
//            }
//            ParamList paramList = new ParamList();
//            paramList.add(new Param("PARAM_NAME", SQL_MO_PARAM_1, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("PARAM_VALUE", sdf.format(lastDateAction), Param.DataType.STRING, Param.IN));
//            listParam.add(paramList);
//            int[] res = poolStore.updateTable(listParam.toArray(new ParamList[listParam.size()]), "CONFIG");
//            logTimeDb("Time to update CONFIG to updateSqlMoParam lastDateAudit " + sdf.format(lastDateAction) + " batchid " + batchId, timeSt);
//        } catch (Exception ex) {
//            sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
//            logger.error("ERROR update CONFIG to updateSqlMoParam lastDateAudit "
//                    + sdf.format(lastDateAction) + " batchid " + batchId, ex);
//            try {
//                int[] res = poolStore.updateTable(listParam.toArray(new ParamList[listParam.size()]), "CONFIG");
//                logTimeDb("Time to retry updateSqlMoParam lastDateAudit "
//                        + (lastDateAction != null ? sdf.format(lastDateAction) : null) + " batchid " + batchId, timeSt);
//            } catch (Exception ex1) {
//                logger.error("ERROR retry updateSqlMoParam lastDateAudit "
//                        + (lastDateAction != null ? sdf.format(lastDateAction) : null) + " batchid " + batchId, ex1);
//            }
//        }
    }

    public boolean checkMaxAddMaxValueInDay(int channelId, String isdn, int currValue, int currAdd) {
        /**
         * SELECT max_reg_num FROM ba_channel_config WHERE 1 = 1 AND
         * channel_type_id = 1000521;
         */
//        return false if over max_reg_num in day for isdn or total value so big        
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
//            Get limit add and limit value in day
            String cfLimitAddBonusInDay = ResourceBundle.getBundle("configPayBonus").getString("limitAddBonusInDay");
            String cfLimitValueBonus = ResourceBundle.getBundle("configPayBonus").getString("limitValueBonus");
            int limitAddBonusInDay;
            int limitValueBonusInDay;
            int maxAddInDay = 0;
            if (cfLimitAddBonusInDay != null && cfLimitAddBonusInDay.length() > 0) {
                limitAddBonusInDay = Integer.valueOf(cfLimitAddBonusInDay);
            } else {
                logger.warn("Do not have config limitAddBonusInDay, set default to 500");
                limitAddBonusInDay = 500;
            }
            if (cfLimitValueBonus != null && cfLimitValueBonus.length() > 0) {
                limitValueBonusInDay = Integer.valueOf(cfLimitValueBonus);
            } else {
                logger.warn("Do not have config limitValueBonus, set default to 2000 MT");
                limitValueBonusInDay = 2000;
            }
//          Get max add in day by channel_id            
            ParamList paramList = new ParamList();
            paramList.add(new Param("channel_type_id", channelId, Param.DataType.INT, Param.IN));
            paramList.add(new Param("max_reg_num", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "ba_channel_config");
            while (rs.next()) {
                maxAddInDay = (int) rs.getLong("max_reg_num");
                if (maxAddInDay <= 0 || maxAddInDay > limitAddBonusInDay) {
                    logger.warn("maxAddInDay do not have config or greater than " + limitAddBonusInDay
                            + " so set again maxAddInDay to " + limitAddBonusInDay + " isdn " + isdn);
                    maxAddInDay = limitAddBonusInDay;
                    break;
                }
            }
//          Check current add in day by isdn
            if (currValue > limitValueBonusInDay) {
                logger.warn("Current value " + currValue + " is over limitValue " + limitValueBonusInDay + " isdn " + isdn);
                result = true;
            }
            if (currAdd > maxAddInDay) {
                logger.warn("Current count time" + currAdd + " is over limitAddTime " + maxAddInDay + " isdn " + isdn);
                result = true;
            }
            logTimeDb("Time to checkMaxAddMaxValueInDay channelId " + channelId + " isdn " + isdn + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkMaxAddMaxValueInDay default return true " + channelId + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = true;
        }
        return result;
    }

    public int getCurrentValueInDay(String isdn) {
        /**
         * SELECT count(amount) from BONUS_PROCESS WHERE isdn = ? and
         * date_action >= trunc(sysdate) and result_code = 0;
         */
        ParamList paramList = new ParamList();
        int curValue = 0;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("AGENT_ISDN", isdn, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("date_action", "trunc(sysdate)", Param.OperatorType.GREATER_EQUAL, Param.DataType.CONST, Param.IN));
            paramList.add(new Param("RESULT_CODE", "0", Param.DataType.STRING, Param.IN));
            paramList.add(new Param("sum(amount) as currValue", null, Param.DataType.INT, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "BONUS_PROCESS");
            while (rs.next()) {
                curValue = rs.getInt("currValue");
                break;
            }
            logTimeDb("Time to getCurrentValueInDay isdn " + isdn + ": " + curValue, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getCurrentValueInDay default return -1: " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            curValue = -1;
        }
        return curValue;
    }

    public int getCurrentTimeAddInDay(String isdn) {
        /**
         * SELECT count(*) from BONUS_PROCESS WHERE isdn = ? and date_action >=
         * trunc(sysdate) and RESULT_CODE = 0;
         */
        ParamList paramList = new ParamList();
        int totalTimeAdd = 0;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("AGENT_ISDN", isdn, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("date_action", "trunc(sysdate)", Param.OperatorType.GREATER_EQUAL, Param.DataType.CONST, Param.IN));
            paramList.add(new Param("RESULT_CODE", "0", Param.DataType.STRING, Param.IN));
            paramList.add(new Param("count(*) as totalTimeAdd", null, Param.DataType.INT, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "BONUS_PROCESS");
            while (rs.next()) {
                totalTimeAdd = rs.getInt("totalTimeAdd");
                break;
            }
            logTimeDb("Time to getCurretnTimeAddInDay isdn " + isdn + ": " + totalTimeAdd, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getCurretnTimeAddInDay default return -1: " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            totalTimeAdd = -1;
        }
        return totalTimeAdd;
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
            DataResources rs = poolStore.selectTable(paramList, "BONUS_PROCESS");
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

    public ActionProfile checkProfile(long idRecord) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        ActionProfile aProfile = new ActionProfile();
        try {
            aProfile.setEnoughProfile(false); //set defaul false
            aProfile.setActionProfileId(-1); //set default -1
            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            psCheckProfile = connection.prepareStatement(sqlCheckProfile);
            if (QUERY_TIMEOUT > 0) {
                psCheckProfile.setQueryTimeout(QUERY_TIMEOUT);
            }
            psCheckProfile.setLong(1, idRecord);
            rs = psCheckProfile.executeQuery();
            while (rs.next()) {
                long soluong = rs.getLong("soluong");
                long profileId = rs.getLong("action_profile_id");
                aProfile.setActionProfileId(profileId);
                if (soluong >= 3) { //Enough profile front, banck of ID and request form, so set true.
                    aProfile.setEnoughProfile(true);
                    break;
                }
            }
            logTimeDb("Time to checkProfile idRecord " + idRecord + " result: " + aProfile.isEnoughProfile()
                    + " action_profile_id " + aProfile.getActionProfileId(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkProfile defaul return false" + idRecord);
            logger.error(AppManager.logException(timeSt, ex));
            aProfile.setEnoughProfile(false); //set defaul false
        } finally {
            closeResultSet(rs);
            closeStatement(psCheckProfile);
            closeConnection(connection);
        }
        return aProfile;
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

    public OutputChangeCommAccount callSmChangeCommAccount(long accountId, String actionName, long amount, long idRecord, String isdn) {
        /**
         * call proceduce sm.change_comm_account
         */
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        OutputChangeCommAccount output = new OutputChangeCommAccount();
        CallableStatement cs = null;
        try {
            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            cs = connection.prepareCall(callChangeCommAccount);
            cs.setLong(1, accountId); //set accountid
            cs.setLong(2, 18); //set requesttype, default 18 like in proc_add_anypay_bonus in profile database
            cs.setString(3, null); //set isdn
            cs.setLong(4, 0); //set totalisdn
            cs.setObject(5, null); //set saletransid
            cs.setString(6, actionName); //set actionName
            cs.setLong(7, amount); //set amount
            cs.setDate(8, new java.sql.Date(timeSt)); //set reqDate
            cs.registerOutParameter(9, java.sql.Types.VARCHAR); //out errorCode
            cs.registerOutParameter(10, java.sql.Types.VARCHAR); //out errMsg
            cs.registerOutParameter(11, java.sql.Types.NUMERIC); //out requestId
            cs.execute();
            output.setErrCode(cs.getString(9));
            output.setRequestId(cs.getLong(11));
            logTimeDb("Time to callSmChangeCommAccount idRecord " + idRecord + " isdn " + isdn
                    + " result: " + output.getErrCode() + " errMsg " + cs.getString(10) + " requestId " + output.getRequestId(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR callSmChangeCommAccount return FAIL");
            output.setErrCode("FAIL");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(cs);
            closeConnection(connection);
        }
        return output;
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

    public int insertSendSms(String isdn, String isdnCustomer, long requetsId, long actionAuditId) {
        /*
         * INSERT INTO send_sms (send_sms_id,source_isdn,target_isdn,sms_type,content,status,create_date,process_time,schedule_date,time_from,
         time_to,source_import,num_process,app_name,params,key,request_id,action_audit_id)
         VALUES   (send_sms_seq.NEXTVAL,'86904',?,'1',NULL,'0',SYSDATE,NULL,NULL, NULL, NULL, NULL,0,NULL,?,?,?,?);
         */
        List<ParamList> listParam = new ArrayList<ParamList>();
        int[] res = new int[0];
        long timeSt = System.currentTimeMillis();
        String param = ";Register customer's info for Smartphone;";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
            String sms_code = ResourceBundle.getBundle("configPayBonus").getString("sms_code");
            String valueAnypayAdd = ResourceBundle.getBundle("configPayBonus").getString("valueAnypayBonus");
            String anypayKeyMsg = ResourceBundle.getBundle("configPayBonus").getString("anypayKeyMsg");
            param = valueAnypayAdd + param + isdnCustomer + ";" + sdf.format(new Date());
            ParamList paramList = new ParamList();
            paramList.add(new Param("send_sms_id", "send_sms_seq.NEXTVAL", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("source_isdn", sms_code, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("target_isdn", isdn, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("sms_type", "1", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("status", "0", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("create_date", "sysdate", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("num_process", 0, Param.DataType.INT, Param.IN));
            paramList.add(new Param("params", param, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("key", anypayKeyMsg, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("request_id", requetsId, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("action_audit_id", actionAuditId, Param.DataType.LONG, Param.IN));
            listParam.add(paramList);
            if (listParam.size() > 0) {
                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "send_sms");
                logTimeDb("Time to insertSendSms , isdn " + isdn + " result: " + res[0], timeSt);
            } else {
                logTimeDb("List Record to insertSendSms is empty, isdn " + isdn, timeSt);
            }
            return res[0];
        } catch (Exception ex) {
            logger.error("ERROR insertSendSms isdn " + isdn, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public int[] deleteQueueTimeout(List<String> listId) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        StringBuilder sf = new StringBuilder();
        try {
            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlDeleteMo);
            sf.setLength(0);
            for (String id : listId) {
                ps.setLong(1, Long.valueOf(id));
                ps.addBatch();
                sf.append(id);
                sf.append(", ");
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueueTimeout bonus_input listId " + sf.toString(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueueTimeout bonus_input, listId " + sf.toString(), timeStart);
        }
    }

    public boolean checkWarnMaxAddMaxValueInDay(int channelId, String isdn, int currValue, int currAdd) {
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
//            Get value to warn add and limit value in day
            String cfLimitAddBonusInDay = ResourceBundle.getBundle("configPayBonus").getString("warnAddBonusInDay");
            String cfLimitValueBonus = ResourceBundle.getBundle("configPayBonus").getString("warnValueBonus");
            int limitAddBonusInDay;
            int limitValueBonusInDay;
            if (cfLimitAddBonusInDay != null && cfLimitAddBonusInDay.length() > 0) {
                limitAddBonusInDay = Integer.valueOf(cfLimitAddBonusInDay);
            } else {
                logger.warn("Do not have config limitAddBonusInDay, set default to 500");
                limitAddBonusInDay = 20;
            }
            if (cfLimitValueBonus != null && cfLimitValueBonus.length() > 0) {
                limitValueBonusInDay = Integer.valueOf(cfLimitValueBonus);
            } else {
                logger.warn("Do not have config limitValueBonus, set default to 2000 MT");
                limitValueBonusInDay = 200;
            }
//          Check current add in day by isdn
            if (currValue > limitValueBonusInDay) {
                logger.warn("Current value " + currValue + " is over warnValue " + limitValueBonusInDay + " isdn " + isdn);
                result = true;
            }
            if (currAdd > limitAddBonusInDay) {
                logger.warn("Current count time" + currAdd + " is over warnAddTime " + limitAddBonusInDay + " isdn " + isdn);
                result = true;
            }
            logTimeDb("Time to checkWarnMaxAddMaxValueInDay channelId " + channelId + " isdn " + isdn + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkWarnMaxAddMaxValueInDay default return true " + channelId + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = true;
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
            connection = getConnection("dbapp2");
            sql = "INSERT INTO mt (mt_id,msisdn,message,mo_his_id,retry_num,receive_time,channel) "
                    + "VALUES(mt_SEQ.nextval,?,?,null,0,sysdate,?)";
            ps = connection.prepareStatement(sql);
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

    public int insertBonusApprove(Bonus bonus) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "INSERT INTO bonus_approve (ACTION_PROFILE_ID,ACTION_AUDIT_ID,RECEIVE_DATE,ISDN_ACCOUNT,STAFF_CODE,SHOP_CODE,SUB_ID,"
                    + " ISSUE_DATETIME, ACTION_ID, REASON_ID, CHECK_INFO, APPROVE_STATUS, staff_checked, checked_datetime) "
                    + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bonus.getActionProfileId());
            ps.setLong(2, bonus.getActionAuditId());
            ps.setTimestamp(3, new Timestamp(bonus.getReceiverDate().getTime()));
            ps.setString(4, bonus.getIsdnCustomer());
            ps.setString(5, bonus.getStaffCode());
            ps.setString(6, bonus.getShopCode());
            ps.setString(7, bonus.getSubId());
            ps.setTimestamp(8, new Timestamp(bonus.getIssueDateTime().getTime()));
            ps.setLong(9, bonus.getActionId());
            ps.setLong(10, bonus.getReasonId());
            ps.setString(11, bonus.getCheckInfo());
            ps.setString(12, "0");
            ps.setString(13, bonus.getStaffCheck());
            ps.setTimestamp(14, new Timestamp(bonus.getTimeCheck().getTime()));
            result = ps.executeUpdate();
            logger.info("End insertBonusApprove isdn " + bonus.getIsdnCustomer() + " action_profile_id "
                    + bonus.getActionProfileId() + " ACTION_AUDIT_ID "
                    + bonus.getActionAuditId() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR bonus_approve: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(bonus.getIsdnCustomer())
                    .append(" action_profile_id ")
                    .append(bonus.getActionProfileId())
                    .append(" ACTION_AUDIT_ID ")
                    .append(bonus.getActionAuditId())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    //LinhNBV start modified on September 04 2017: Get isdn of channel
    public String getTelByStaffCode(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String tel = null;
        String sqlMo = " select cellphone from vsa_v3.users where user_name = ? ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, staffCode.toLowerCase());
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                tel = rs1.getString("cellphone");
            }
            if (tel == null) {
                tel = "";
                logger.info("tel is null - staff_code: " + staffCode);
            }
            logTimeDb("Time to getTelByStaffCode: " + staffCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTelByStaffCode " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return tel;
    }

    public String getSerialByIsdnCustomer(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String serial = null;
        String sqlMo = " select serial from cm_pre.sub_mb where status = 2 and isdn = ? ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                serial = rs1.getString("serial");
            }
            logTimeDb("Time to getSerialByIsdnCustomer: " + isdn, timeSt);
            if (serial == null) {
                serial = "";
            }
        } catch (Exception ex) {
            logger.error("ERROR getSerialByIsdnCustomer " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            serial = "";
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return serial;
    }

    public boolean checkAlreadyWaitApprove(long idRecord) {
        /**
         * SELECT action_audit_id FROM BONUS_PROCESS WHERE action_audit_id =
         * idRecord
         */
        ParamList paramList = new ParamList();
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("action_audit_id", idRecord, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("action_audit_id", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "bonus_approve");
            while (rs.next()) {
                long id = rs.getLong("action_audit_id");
                if (id > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkAlreadyWaitApprove idRecord " + idRecord + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAlreadyWaitApprove defaul return false" + idRecord);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        }
        return result;
    }

    public boolean checkChannelHaveContract(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        try {
            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(
                    //LinhNBV start modified on April 12 2018: Add conditional is_checked = 1 --> Profile contract of channel is correct 
                    "select staff_code from sm.staff where status = 1 and image_url is not null and staff_code = ? and is_checked = 1");
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
            logTimeDb("Time to checkChannelHaveContract " + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkChannelHaveContract defaul return false");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
}
