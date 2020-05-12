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
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.DateFormat;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbSubProfileProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbSubProfileProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
    private String callChangeCommAccount = "{call sm.change_comm_account(?,?,?,?,?,?,?,?,?,?,?)}";
    private String sqlDeleteMo = "update cm_pre.sub_profile_info set bonus_status = 1 where sub_profile_id = ?";

    public DbSubProfileProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbSubProfileProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        Bonus record = new Bonus();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("sub_profile_id"));
            record.setActionAuditId(rs.getLong("action_audit_id"));
            record.setPkId(rs.getLong("sub_id"));
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
            record.setBts2G(rs.getString("bts_2g"));
            record.setBlockOcsHlr(rs.getLong("block_ocs_hlr"));
            record.setCustId(rs.getLong("cust_id"));
            record.setModifyType(rs.getString("modify_type"));
            record.setRequestEmola(rs.getString("request_emola"));
            record.setDaySentSms(rs.getLong("day_sent_sms"));
            record.setSimSerial(rs.getString("sim_serial"));
            record.setKitBatchId(rs.getLong("kit_batch_id"));
            record.setMainProduct(rs.getString("main_product"));
            record.setMainPrice(rs.getString("main_price"));
            record.setVasCode(rs.getString("vas_code"));
            record.setHandsetModel(rs.getString("handset_serial"));
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
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        String batchId = "";
        int[] res = new int[0];
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                Bonus sd = (Bonus) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getId());
                ps.addBatch();
            }
            res = ps.executeBatch();
            return res;
        } catch (Exception ex) {
            logger.error("ERROR updateQueue SUB_PROFILE_INFO batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to updateQueue SUB_PROFILE_INFO, batchid " + batchId + " total result: " + res.length, timeStart);            
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
                paramList.add(new Param(Bonus.PK_ID, sd.getId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.ACTION_CODE, sd.getActionCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.REASON_ID, sd.getReasonId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.ACTION_AUDIT_ID, sd.getActionAuditId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.ISDN, sd.getIsdnCustomer(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.ACTIVE_STATUS, sd.getActiveStatus(), Param.DataType.STRING, Param.IN));
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
                paramList.add(new Param(Bonus.AGENT_ID, sd.getAgentId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.AGENT_ISDN, sd.getAgentIsdn(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.ACCOUNT_ID, sd.getAccountId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("check_staff", sd.getStaffCheck(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("check_info", sd.getCheckInfo(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("check_time", sd.getTimeCheck(), Param.DataType.TIMESTAMP, Param.IN));
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

    //LamNT update status OCS_HLR
    public long updateSubProfileInfo(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "UPDATE SUB_PROFILE_INFO SET BLOCK_OCS_HLR=0 WHERE ISDN= ? ";
            ps = connection.prepareStatement(sql);
            logger.info("Start SUB_PROFILE_INFO isdn " + isdn);
            ps.setString(1, isdn);
            res = ps.executeUpdate();
            logger.info("End SUB_PROFILE_INFO isdn " + isdn + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR SUB_PROFILE_INFO isdn " + isdn);
            logger.error(br + ex.toString());
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }
//End LamNT

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
            paramList.add(new Param("shop_id", null, Param.DataType.LONG, Param.OUT));
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
                    agent.setShopId(rs.getLong("shop_id"));
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
            DataResources rs = poolStore.selectTable(paramList, "profile.ba_channel_config");
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
            DataResources rs = poolStore.selectTable(paramList, "profile.ba_product_config");
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
            DataResources rs = poolStore.selectTable(paramList, "profile.ba_item_fee bif, profile.ba_action_reason_fee arf, profile.action_reason ar");
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
            DataResources rs = poolStore.selectTable(paramList, "profile.ba_channel_config");
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
            DataResources rs = poolStore.selectTable(paramList, "profile.BONUS_PROCESS");
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
            DataResources rs = poolStore.selectTable(paramList, "profile.BONUS_PROCESS");
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

    public OutputChangeCommAccount callSmChangeCommAccount(long accountId, String actionName, long amount, long idRecord, String isdn) {
        /**
         * call proceduce sm.change_comm_account
         */
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        OutputChangeCommAccount output = new OutputChangeCommAccount();
        CallableStatement cs = null;
        try {
            String dbName = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
            connection = ConnectionPoolManager.getConnection(dbName);
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

    public int[] deleteQueueTimeout(List<String> listId) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        StringBuilder sf = new StringBuilder();
        try {
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
            logger.error("ERROR deleteQueueTimeout SUB_PROFILE_INFO listId " + sf.toString(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueueTimeout SUB_PROFILE_INFO, listId " + sf.toString(), timeStart);
        }
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

    public int insertBonusApprove(Bonus bonus) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            String dbName = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
            connection = getConnection(dbName);
            sql = "INSERT INTO bonus_approve (ACTION_PROFILE_ID,ACTION_AUDIT_ID,RECEIVE_DATE,ISDN_ACCOUNT,STAFF_CODE,SHOP_CODE,SUB_ID,"
                    + " ISSUE_DATETIME, ACTION_ID, REASON_ID, CHECK_INFO, APPROVE_STATUS) "
                    + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
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

//LamNT update Assign Staff check
    public long updateAssign(String assignStaff, Integer subProfileId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update sub_profile_info set assigned_user = ?, modify_type=3 where sub_profile_id = ? ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, assignStaff);
            ps.setInt(2, subProfileId);
            res = ps.executeUpdate();
            logger.info("End updateAssign where ID " + subProfileId + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateAssign ID " + subProfileId);
            logger.error(br + ex.toString());
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }
//End LamNT

//LamNT get user online
    public List<String> getUserOnline() {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        List<String> users = new ArrayList<String>();
        String user = null;
        String sql = " select distinct(spl.user_name) as name from cm_pre.sub_profile_log spl,cm_pre.sub_profile_role spr "
                + " where lower(spl.user_name)=lower(spr.user_name) and  spl.logout_time is null and spr.role_id = 0 ";
        PreparedStatement ps = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            ps = connection.prepareStatement(sql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = ps.executeQuery();
            while (rs1.next()) {
                user = rs1.getString("name");
                users.add(user);
            }
        } catch (Exception ex) {
            logger.error("ERROR getUserOnline ");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(ps);
            closeConnection(connection);
        }
        return users;
    }
//End LamNT

//LamNT get config
    public List<Config> getConfig() {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs1 = null;
        List<Config> lst = new ArrayList<Config>();
        String sqlMo = "SELECT CONFIG_ID,VALUES_ID,VALUES_NAME FROM CONFIG_SMS_PROPERTY ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Config conf = new Config();
                conf.setConfigId(rs1.getLong("CONFIG_ID"));
                conf.setValuesId(rs1.getString("VALUES_ID"));
                conf.setValuesName(rs1.getString("VALUES_NAME"));
                lst.add(conf);
                continue;
            }
        } catch (Exception ex) {
            logger.error("ERROR getConfig ");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        return lst;
    }
//End LamNT

    public boolean checkAlreadyWarningInDay(String isdn, String msg) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp2");
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
            connection = getConnection("dbapp2");
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

//LamNT update Assign Staff check
    public long updateSentSms(Integer daySentSms, Integer subProfileId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update sub_profile_info set day_sent_sms = ? where sub_profile_id = ? ";
            ps = connection.prepareStatement(sql);
            logger.info("Start updateSentSms isdn " + subProfileId);
            ps.setInt(1, daySentSms);
            ps.setInt(2, subProfileId);
            res = ps.executeUpdate();
            logger.info("End updateSentSms where ID " + subProfileId + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSentSms ID " + subProfileId);
            logger.error(br + ex.toString());
            res = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }
//End LamNT

    //LinhNBV start modified on September 04 2017: Get isdn of channel
    public String getTelByStaffCode(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String tel = null;
        String sqlMo = " select cellphone from vsa_v3.users where user_name = ? and status = 1";
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

    public boolean checkChannelHaveContract(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(
                    //LinhNBV start modified on April 12 2018: Add condition to check profile of channel correct or not
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

    public boolean checkMovitelStaff(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
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
            paramList.add(new Param("BONUS_TYPE", log.getBonusType(), Param.DataType.LONG, Param.IN));
            PoolStore.PoolResult prs = poolStore.insertTable(paramList, "EWALLET_LOG");
            logTimeDb("Time to insertEwalletLog isdn " + log.getMobile(), timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getMobile());
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public Customer getCustomerByCustId(Long custId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Customer customer = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from cm_pre.customer where cust_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, custId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String subName = rs.getString("name");
                String tempGender = rs.getString("sex");
                String gender = "";
                if ("M".equals(tempGender)) {
                    gender = "0";
                } else if ("F".equals(tempGender)) {
                    gender = "1";
                } else {
                    gender = "0";
                }
                String birthDate = rs.getString("birth_date");
                if (birthDate == null) {
                    DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
                    birthDate = dateFormat.format(new Date());
                }
                String idNo = rs.getString("id_no");

                customer = new Customer();
                customer.setSubName(subName);
                customer.setGender(gender);
                customer.setCustId(custId);
                customer.setBirthDate(birthDate);
                customer.setIdNo(idNo);

                break;
            }
            logger.info("End getCustomerByCustId: custId " + custId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getCustomerByCustId ").append(custId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            customer = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return customer;
        }
    }

    public int insertLogCallWsWallet(String isdn, Long ewalletId, String request, String custName, String idNo, String parentId, String issuePlace) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "insert into Log_Call_Ws_Wallet (id,isdn,ewallet_id,action_type,status_process,number_process,insert_date,description,customer_name,dob,id_no,channel_type,parent_id,idissueplace,idissuedate)\n"
                    + "values (log_call_ws_wallet_seq.nextval,?,?,'1','0','0',sysdate,?,?,null,?,'',?,?,null)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setLong(2, ewalletId);
            ps.setString(3, request);
            ps.setString(4, custName);
            ps.setString(5, idNo);
            if (parentId == null) {
                parentId = "";
            }
            ps.setString(6, parentId);
            ps.setString(7, issuePlace);
            result = ps.executeUpdate();
            logger.info("End insertLogCallWsWallet staffId " + ewalletId + " isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleToKeyPos: ").
                    append(sql).append("\n")
                    .append(" staffId ")
                    .append(ewalletId)
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

    public String getShopPath(long shopId, String isdn, String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String result = "";
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement("select shop_path from sm.shop where status = 1 and shop_id = ?");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setLong(1, shopId);
            rs = ps.executeQuery();
            if (rs.next()) {
                result = rs.getString("shop_path");
            }
            logTimeDb("Time to getShopPath shopId " + shopId + " isdn " + isdn + " staffCode " + staffCode
                    + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getShopPath shopId " + shopId + " isdn " + isdn + " staffCode " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertBonusSecond(Bonus bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "INSERT INTO bonus_second (ACTION_AUDIT_ID,ISDN,CREATE_STAFF,CHECK_INFO,EMOLA_ISDN,ITEM_FEE_ID,FIRST_AMOUNT,"
                    + "SECOND_AMOUNT, channel_type_id, action_code) \n"
                    + " VALUES(?,?,?,?,?,?,?,?, ?, ?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getActionAuditId());
            ps.setString(2, bn.getIsdnCustomer());
            ps.setString(3, bn.getStaffCode());
            ps.setString(4, bn.getCheckInfo());
            ps.setString(5, bn.getIsdn());
            ps.setLong(6, bn.getItemFeeId());
            ps.setLong(7, bn.getAmount());
            ps.setLong(8, bn.getSecondAmount());
            ps.setInt(9, bn.getChannelTypeId());
            ps.setString(10, bn.getActionCode());
            result = ps.executeUpdate();
            logger.info("End insertBonusSecond staff " + bn.getStaffCode() + " isdn " + bn.getIsdnCustomer() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertBonusSecond: ").
                    append(sql).append("\n")
                    .append(" staff ")
                    .append(bn.getStaffCode())
                    .append(" isdn ")
                    .append(bn.getIsdnCustomer())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertBonusFirstSimHandset(Bonus bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbVasPromoSt");
            sql = "INSERT INTO Pay_First_Mobile_Phone (ACTION_AUDIT_ID,ISDN,CREATE_STAFF,CHECK_INFO,EMOLA_ISDN,ITEM_FEE_ID,FIRST_AMOUNT,"
                    + "SECOND_AMOUNT, channel_type_id, action_code) \n"
                    + " VALUES(?,?,?,?,?,?,?,?, ?, ?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getActionAuditId());
            ps.setString(2, bn.getIsdnCustomer());
            ps.setString(3, bn.getStaffCode());
            ps.setString(4, bn.getCheckInfo());
            ps.setString(5, bn.getIsdn());
            ps.setLong(6, bn.getItemFeeId());
            ps.setLong(7, bn.getAmount());
            ps.setLong(8, bn.getSecondAmount());
            ps.setInt(9, bn.getChannelTypeId());
            ps.setString(10, bn.getActionCode());
            result = ps.executeUpdate();
            logger.info("End insertBonusMobileShop staff " + bn.getStaffCode() + " isdn " + bn.getIsdnCustomer() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertBonusMobileShop: ").
                    append(sql).append("\n")
                    .append(" staff ")
                    .append(bn.getStaffCode())
                    .append(" isdn ")
                    .append(bn.getIsdnCustomer())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
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
            if (isdn.startsWith("258")) {
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

    public boolean isSim4G(String serial, Long stockModelId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        long transId = 0;
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.stock_sim where serial = to_number(?) and stock_model_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            ps.setLong(2, stockModelId);
            rs = ps.executeQuery();
            while (rs.next()) {
                transId = rs.getLong("stock_model_id");
                if (transId > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End isSim4G serial " + serial
                    + " result " + result + " transId " + transId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR isSim4G: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial)
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

    public ProductConnectKit getProductConnectKit(String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        ProductConnectKit product = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from product.product_connect_kit where status = 1 and upper(product_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {

                Long bonusCenter = rs.getLong("bonus_center");
                Long bonusChannel = rs.getLong("bonus_channel");
                Long bonusCtv = rs.getLong("bonus_ctv");

                long isProduct = rs.getLong("is_product");
                String mainProduct = rs.getString("main_product");
                String moneyFee = rs.getString("money_fee");
                String description = rs.getString("description");
                String vasConnection = rs.getString("vas_connection");
                String vasParam = rs.getString("vas_param");
                String vasActionType = rs.getString("vas_action_type");
                String vasChannel = rs.getString("vas_channel");

                product = new ProductConnectKit();
                product.setProductCode(productCode);
                product.setMoneyFee(moneyFee);
                product.setBonusCenter(bonusCenter);
                product.setBonusChannel(bonusChannel);
                product.setBonusCtv(bonusCtv);
                product.setIsProduct(isProduct);
                product.setMainProduct(mainProduct);
                product.setDescription(description);
                product.setVasConnection(vasConnection);
                product.setVasParam(vasParam);
                product.setVasActionType(vasActionType);
                product.setVasChannel(vasChannel);
                break;
            }
            logger.info("End getProductConnectKit productCode " + productCode
                    + ",time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getProductConnectKit: ").
                    append(sql).append("\n")
                    .append(" productCode ")
                    .append(productCode);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return product;
        }
    }

    public int insertMO(String msisdn, String vasCode, String connName, String param, String actionType, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(connName);
            sql = "INSERT INTO mo (MO_ID,MSISDN,COMMAND,PARAM,RECEIVE_TIME,ACTION_TYPE,CHANNEL,CHANNEL_TYPE) \n"
                    + "VALUES(mo_seq.nextval,?,?,?,sysdate,?,?,'MBCCS')";
            ps = connection.prepareStatement(sql);
            ps.setString(1, msisdn);
            ps.setString(2, vasCode);
            if (param != null && !param.isEmpty()) {
                ps.setString(3, param);
            } else {
                ps.setString(3, "");
            }
            ps.setString(4, actionType);
            ps.setString(5, channel);
            result = ps.executeUpdate();
            logger.info("End insertMO msisdn " + msisdn + " vasCode " + vasCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertMO: ").
                    append(sql).append("\n")
                    .append(" msisdn ")
                    .append(msisdn)
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

    public String getMainPriceConfig(String mainPrice, String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String mainPriceConfig = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select (case when list_benefit is null then 'NA' else list_benefit end)||'^'||commission_value as main_price_config from product.money_connect_kit where money_name = ? and product_code = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, mainPrice);
            ps.setString(2, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                mainPriceConfig = rs.getString("main_price_config");;
                break;
            }
            logger.info("End getComissionBasedOnPrice " + mainPrice + ", mainPriceConfig: " + mainPriceConfig + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getComissionBasedOnPrice: ").
                    append(sql).append("\n")
                    .append(" mainPrice ")
                    .append(mainPrice);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return mainPriceConfig;
        }
    }

    public String getBasedConfigConnectKit(String staffCode, String columnName) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String basedConfig = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
//            sql = "select distinct main_product_name from product_connect_kit where main_product_name is not null";
//            LinhNBV 20200303: filter main product by staff_code
            sql = "SELECT listagg(" + columnName + ",'|') within group (order by order_by_main_product) as based_config\n"
                    + "from (select distinct " + columnName + ", order_by_main_product from product.main_product_connect_kit where (for_user is null or for_user like ?) \n"
                    + "and status = 1 order by order_by_main_product)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, "%" + staffCode.toUpperCase() + "%");
            rs = ps.executeQuery();
            while (rs.next()) {
                basedConfig = rs.getString("based_config");
            }
            logger.info("End getBasedConfigConnectKit: " + basedConfig + ", staffCode: " + staffCode
                    + ", columnName: " + columnName + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getBasedConfigConnectKit: ");
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return basedConfig;
        }
    }

    public String getMainProductConnectKit(String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String mainProduct = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from product.main_product_connect_kit where product_code = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                mainProduct = rs.getString("main_product");
                break;
            }
            logger.info("End getMainProductConnectKit productCode: " + productCode + ", mainProduct: " + mainProduct + ", time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getMainProductConnectKit productCode: ").
                    append(productCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return mainProduct;
        }
    }

    public long getPriceByStockModelCode(String stockModelCode, String priceType, String pricePolicy) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        long price = 0;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.price where status = 1 and stock_model_id = (select stock_model_id from sm.stock_model where stock_model_code = ?)\n"
                    + "and status = 1 and sta_date <= sysdate and (end_date > trunc(sysdate) or end_date is null)\n"
                    + "and type = ? and price_policy = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, stockModelCode);
            ps.setString(2, priceType);
            ps.setString(3, pricePolicy);
            rs = ps.executeQuery();
            while (rs.next()) {
                price = rs.getLong("price");
                break;
            }
            logger.info("End getPriceByStockModelCode stockModelCode " + stockModelCode + ", priceType: " + priceType + " price " + price + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getPriceByStockModelCode: ").
                    append(sql).append("\n")
                    .append(" stockModelCode ")
                    .append(stockModelCode)
                    .append(" priceType ")
                    .append(priceType);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return price;
        }
    }

    public long getDiscountForHandset(String stockModelCode, String mainProduct, String shopId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        long discountAmount = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from product.product_connect_kit_handset where stock_model_id = (select stock_model_id from sm.stock_model where status = 1 and stock_model_code = ?) and main_product like ? and for_branch like ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, stockModelCode);
            ps.setString(2, "%" + mainProduct + "%");
            ps.setString(3, "%" + shopId + "%");
            rs = ps.executeQuery();
            while (rs.next()) {
                discountAmount = rs.getLong("discount");
                break;
            }
            logger.info("End getDiscountForHandset stockModelCode: " + stockModelCode + ", discount: " + discountAmount + ", time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getDiscountForHandset stockModelCode: ").
                    append(stockModelCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return discountAmount;
        }
    }

    public String getShopPathByStaffCode(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String shopPath = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select shop_path from sm.shop where shop_id = (select shop_id from sm.staff where lower(staff_code) = lower(?) and status = 1)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                shopPath = rs.getString("shop_path");
                break;
            }
            logger.info("End getShopPathByStaffCode staffCode " + staffCode + " shopPath " + shopPath + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getShopPathByStaffCode: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" shopPath ")
                    .append(shopPath);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return shopPath;
        }
    }

    public String getPricePolicyHandset(String mainProduct, String stockModelCode, String shopId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String pricePolicy = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            sql = "select * from product.product_connect_kit_handset where status = 1 and main_product like ? and stock_model_code = ? and (for_branch is null or for_branch like ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, "%" + mainProduct + "%");
            ps.setString(2, stockModelCode);
            ps.setString(3, "%" + shopId + "%");
            rs = ps.executeQuery();
            while (rs.next()) {
                pricePolicy = rs.getString("price_policy");;
                break;
            }
            logger.info("End getPricePolicyHandset mainProduct " + mainProduct + ", pricePolicy: " + pricePolicy + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getPricePolicyHandset: ").
                    append(sql).append("\n")
                    .append(" mainProduct ")
                    .append(mainProduct);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return pricePolicy;
        }
    }

    public String getProductCode(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String productCode = null;
        String sqlMo = "select * from sub_mb where isdn = ? and status = 2";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                productCode = rs1.getString("product_code");
                break;
            }
            if (productCode == null) {
                productCode = "";
                logger.info("productCode is null - isdn: " + isdn);
            }
            logTimeDb("Time to getProductCode, isdn: " + isdn + " productCode " + productCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getProductCode, isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return productCode;
        }
    }

    public boolean checkAlreadyPayBonus(Long actionAuditId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            String sql = "select * from profile.ewallet_log where import_date > trunc(sysdate-7) and action_audit_id = ?";
            ps = connection.prepareStatement(sql);
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setLong(1, actionAuditId);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logTimeDb("Time to checkAlreadyPayBonus " + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAlreadyPayBonus defaul return false");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
}
