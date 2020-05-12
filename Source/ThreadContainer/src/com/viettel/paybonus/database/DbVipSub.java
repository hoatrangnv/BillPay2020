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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbVipSub extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbVipSub.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDeleteMo = "delete vip_sub_detail where vip_sub_detail_id = ?";
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public DbVipSub() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbVipSubNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbVipSub(String sessionName, Logger logger) throws SQLException, Exception {
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
        VipSubDetail record = new VipSubDetail();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong(VipSubDetail.VIP_SUB_DETAIL_ID));
            record.setVipSubDetailId(rs.getLong(VipSubDetail.VIP_SUB_DETAIL_ID));
            record.setVipSubInfoId(rs.getLong(VipSubDetail.VIP_SUB_INFO_ID));
            record.setIsdn(rs.getString(VipSubDetail.ISDN).trim());
            record.setHisNextProcessTime(rs.getTimestamp(VipSubDetail.NEXT_PROCESS_TIME));
            record.setCycleType(rs.getString(VipSubDetail.CYCLE_TYPE));
            record.setMoneyAcc(rs.getString(VipSubDetail.MONEY_ACC));
            record.setMoneyValue(rs.getString(VipSubDetail.MONEY_VALUE));
            record.setSmsAcc(rs.getString(VipSubDetail.SMS_ACC));
            record.setSmsValue(rs.getString(VipSubDetail.SMS_VALUE));
            record.setSmsOutAcc(rs.getString(VipSubDetail.SMS_OUT_ACC));
            record.setSmsOutValue(rs.getString(VipSubDetail.SMS_OUT_VALUE));
            record.setDataAcc(rs.getString(VipSubDetail.DATA_ACC));
            record.setDataValue(rs.getString(VipSubDetail.DATA_VALUE));
            record.setVoiceAcc(rs.getString(VipSubDetail.VOICE_ACC));
            record.setVoiceValue(rs.getString(VipSubDetail.VOICE_VALUE));
            record.setVoiceOutAcc(rs.getString(VipSubDetail.VOICE_OUT_ACC));
            record.setVoiceOutValue(rs.getString(VipSubDetail.VOICE_OUT_VALUE));
            record.setPricePlan(rs.getString("price_plan"));
            record.setCreateTime(rs.getTimestamp(VipSubDetail.CREATE_TIME));
            record.setCreateUser(rs.getString(VipSubDetail.CREATE_USER));
            record.setStatus(rs.getString(VipSubDetail.STATUS));
            record.setTotalMoney(rs.getLong("total_money"));
            record.setEachMoney(rs.getLong("EACH_MONEY"));
            record.setPrepaidMonth(rs.getLong("PREPAID_MONTH"));
            record.setApprovalTime(rs.getTimestamp("approval_date"));
            record.setApprovalUser(rs.getString("APPROVAL_USER"));
            record.setRemainPrepaidMonth(rs.getLong("remain_prepaid_month"));
            record.setPolicyId(rs.getString("policy_id")); //Huynq13 20180425 add for PolicyId
//          Huynq13 20180521 add for new account
            record.setVoiceInOutAcc(rs.getString("voice_in_out_acc"));
            record.setVoiceInOutValue(rs.getString("voice_in_out_value"));
            record.setMoneySpecialAcc(rs.getString("money_special_acc"));
            record.setMoneySpecialValue(rs.getString("money_value_special"));
            record.setPolicyName(rs.getString("policy_name"));
            record.setGroupId(rs.getLong("group_id"));
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
            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbVipSubNameConfig");
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                VipSubDetail sd = (VipSubDetail) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getVipSubDetailId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue VIP_SUB_DETAIL batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue VIP_SUB_DETAIL, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                VipSubDetail sd = (VipSubDetail) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("VIP_SUB_PROCESS_LOG_ID", "vip_sub_process_log_seq.nextval", Param.DataType.CONST, Param.IN));
                paramList.add(new Param(VipSubDetail.VIP_SUB_DETAIL_ID, sd.getVipSubDetailId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(VipSubDetail.ISDN, sd.getIsdn(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(VipSubDetail.HIS_NEXT_PROCESS_TIME, sd.getHisNextProcessTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param(VipSubDetail.NEXT_PROCESS_TIME, sd.getNextProcessTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param("CLUSTER_NAME", sd.getClusterName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("NODE_NAME", sd.getNodeName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(VipSubDetail.RESULT_CODE, sd.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(VipSubDetail.DESCRIPTION, sd.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("total_money", sd.getTotalMoney(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("EACH_MONEY", sd.getEachMoney(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("PREPAID_MONTH", sd.getPrepaidMonth(), Param.DataType.LONG, Param.IN));
                if (checkMakeSaleTrans(sd.getVipSubInfoId())) {
                    try {
                        if (sd.getPrepaidMonth() != null && sd.getPrepaidMonth().toString().trim().length() > 0
                                && sd.getPrepaidMonth() > 0 //                                && sd.getApprovalTime() != null
                                //                                && (sd.getApprovalUser() == null || sd.getApprovalUser().trim().length() <= 0)
                                ) {
                            if (sd.getMakeSaleTrans() != null && sd.getMakeSaleTrans() > 0) {
                                logger.info(sd.getVipSubDetailId()
                                        + " already make saletrans so not make more");
                                paramList.add(new Param("MAKE_SALE_TRANS", "1", Param.DataType.CONST, Param.IN));
                            }
//                            Calendar cal1 = Calendar.getInstance();
//                            cal1.setTime(sd.getApprovalTime());
//                            Integer addDay = Integer.valueOf(sd.getCycleType().trim()) * Integer.valueOf(sd.getPrepaidMonth().toString());
//                            cal1.add(Calendar.DATE, addDay);
//                            Date currentTime = new Date();
//                            if (currentTime.before(cal1.getTime()) && checkAlreadyAddBonusSuccess(sd.getVipSubDetailId())) {
//                                logger.info(sd.getVipSubDetailId()
//                                        + " already make saletrans and current is in last approvaltime + cycleday*prepaidpay, so not make more");
//                                paramList.add(new Param("MAKE_SALE_TRANS", "1", Param.DataType.CONST, Param.IN));
//                            }
                        }
                    } catch (Exception e) {
                        logger.error("Had exception when determine making saletrans more " + e.toString());
                    }
                }
                paramList.add(new Param("VIP_SUB_INFO_ID", sd.getVipSubInfoId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("policy_id", sd.getPolicyId(), Param.DataType.STRING, Param.IN));//Huynq13 20180425 add for PolicyId
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "vip_sub_process_log");
            logTimeDb("Time to insertQueueHis vip_sub_process_log, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis vip_sub_process_log batchid " + batchId, ex);
            try {
                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "vip_sub_process_log");
                logTimeDb("Time to retry insertQueueHis vip_sub_process_log, batchid " + batchId + " total result: " + res.length, timeSt);
                return res;
            } catch (Exception ex1) {
                logger.error("ERROR retry insertQueueHis vip_sub_process_log batchid " + batchId, ex1);
                logger.error(AppManager.logException(timeSt, ex));
                return null;
            }
        }
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        int[] res = new int[0];
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                VipSubDetail sd = (VipSubDetail) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("VIP_SUB_DETAIL_ID", sd.getVipSubDetailId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("VIP_SUB_INFO_ID", sd.getVipSubInfoId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("ISDN", sd.getIsdn(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("NEXT_PROCESS_TIME", sd.getNextProcessTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param("CYCLE_TYPE", sd.getCycleType(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("MONEY_ACC", sd.getMoneyAcc(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("MONEY_VALUE", sd.getMoneyValue(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("SMS_ACC", sd.getSmsAcc(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("SMS_VALUE", sd.getSmsValue(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("SMS_OUT_ACC", sd.getSmsOutAcc(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("SMS_OUT_VALUE", sd.getSmsOutValue(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("DATA_ACC", sd.getDataAcc(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("DATA_VALUE", sd.getDataValue(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("VOICE_ACC", sd.getVoiceAcc(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("VOICE_VALUE", sd.getVoiceValue(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("VOICE_OUT_ACC", sd.getVoiceOutAcc(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("VOICE_OUT_VALUE", sd.getVoiceOutValue(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("CREATE_TIME", sd.getCreateTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param("CREATE_USER", sd.getCreateUser(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("STATUS", sd.getStatus(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("total_money", sd.getTotalMoney(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("EACH_MONEY", sd.getEachMoney(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("PREPAID_MONTH", sd.getPrepaidMonth(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("APPROVAL_DATE", sd.getApprovalTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param("APPROVAL_USER", sd.getApprovalUser(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("remain_prepaid_month", sd.getRemainPrepaidMonth(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("price_plan", sd.getPricePlan(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("policy_id", sd.getPolicyId(), Param.DataType.STRING, Param.IN));//Huynq13 20180425 add for PolicyId
                paramList.add(new Param("voice_in_out_acc", sd.getVoiceInOutAcc(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("voice_in_out_value", sd.getVoiceInOutValue(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("money_special_acc", sd.getMoneySpecialAcc(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("money_value_special", sd.getMoneySpecialValue(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("GROUP_ID", sd.getGroupId(), Param.DataType.LONG, Param.IN));
                listParam.add(paramList);
            }
            if (listParam.size() > 0) {
                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "vip_sub_detail");
                logTimeDb("Time to insertQueueOutput vip_sub_detail, batchid " + batchId + " total result: " + res.length, timeSt);
            } else {
                logTimeDb("List Record to insert Queue Output is empty, batchid " + batchId, timeSt);
            }
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueOutput batchid " + batchId, ex);
            try {
                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "vip_sub_detail");
                logTimeDb("Time to retry insertQueueOutput vip_sub_detail, batchid " + batchId + " total result: " + res.length, timeSt);
                return res;
            } catch (Exception ex1) {
                logger.error("ERROR retry insertQueueOutput vip_sub_detail, batchid " + batchId, ex1);
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
//            The first delete queue timeout
            deleteQueueTimeout(ids);
//            Save history
            for (String sd : ids) {
                sb.append(":" + sd);
                ParamList paramList = new ParamList();
                paramList.add(new Param(VipSubDetail.VIP_SUB_DETAIL_ID, Long.valueOf(sd), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(VipSubDetail.RESULT_CODE, "FW_99", Param.DataType.STRING, Param.IN));
                paramList.add(new Param(VipSubDetail.DESCRIPTION, "FW_Timeout", Param.DataType.STRING, Param.IN));
                paramList.add(new Param("log_time", "sysdate", Param.DataType.CONST, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "vip_sub_process_log");
            logTimeDb("Time to processTimeoutRecord, insert vip_sub_process_log, total result: " + res.length, timeSt);
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
            try {
                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "vip_sub_process_log");
                logTimeDb("Time to retry processTimeoutRecord, insert vip_sub_process_log, total result: " + res.length, timeSt);
            } catch (Exception ex1) {
                logger.error("ERROR retry processTimeoutRecord ", ex1);
                logger.error(AppManager.logException(timeSt, ex));
            }
        }
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

    public boolean checkAlreadyProcessRecord(String isdn, String processDate) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select isdn from vip_sub_process_log where isdn = ? and his_next_process_time = to_date(?,'yyyyMMddhh24miss') and result_code = '0'";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, isdn);
            psMo.setString(2, processDate);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String soluong = rs1.getString("isdn");
                if (soluong != null && soluong.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkAlreadyProcessRecord msisdn " + isdn + " processDate " + processDate + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAlreadyProcessRecord msisdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public boolean checkBonusOneTime(String isdn) {
        /**
         * SELECT isdn FROM log_update_info_his WHERE isdn = isdn
         */
        ParamList paramList = new ParamList();
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("isdn", isdn, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("log_update_info_id", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "log_update_info_his");
            while (rs.next()) {
                long id = rs.getLong("log_update_info_id");
                if (id > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkBonusOneTime isdn " + isdn + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkBonusOneTime defaul return false" + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        }
        return result;
    }

    public int[] deleteQueueTimeout(List<String> listId) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        StringBuilder sf = new StringBuilder();
        try {
            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbVipSubNameConfig");
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
            logger.error("ERROR deleteQueueTimeout VIP_SUB_DETAIL listId " + sf.toString(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueueTimeout VIP_SUB_DETAIL, listId " + sf.toString(), timeStart);
        }
    }

    public boolean checkPreSub(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select isdn from cm_pre.sub_mb where isdn = ? and status = 2";
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
                String soluong = rs1.getString("isdn");
                if (soluong != null && soluong.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkMovitelSub msisdn " + isdn + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkMovitelSub msisdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public boolean checkPosSub(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select isdn from cm_pos.sub_mb where isdn = ? and status = 2";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pos");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String soluong = rs1.getString("isdn");
                if (soluong != null && soluong.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkMovitelSub msisdn " + isdn + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkMovitelSub msisdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
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

    public boolean checkMakeSaleTrans(long vipSubInfoId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select make_sale_trans from vip_sub_process_log where vip_sub_info_id = ? and make_sale_trans = 1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, vipSubInfoId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String soluong = rs1.getString("make_sale_trans");
                if (soluong != null && soluong.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkMakeSaleTrans msisdn " + vipSubInfoId + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkMakeSaleTrans msisdn " + vipSubInfoId);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public boolean checkAlreadyAddBonusSuccess(long vipSubDetailId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select vip_sub_detail_id from vip_sub_process_log where vip_sub_detail_id = ? and result_code = '0'";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, vipSubDetailId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                long soluong = rs1.getLong("vip_sub_detail_id");
                if (soluong > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkAlreadyAddBonusSuccess msisdn " + vipSubDetailId + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAlreadyAddBonusSuccess msisdn " + vipSubDetailId);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public boolean checkFirstMonth(long vipSubDetailId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select vip_sub_detail_id from vip_sub_detail where vip_sub_detail_id = ? and remain_prepaid_month is null";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, vipSubDetailId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                long soluong = rs1.getLong("vip_sub_detail_id");
                if (soluong > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkFirstMonth vipSubDetailId " + vipSubDetailId + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkFirstMonth vipSubDetailId " + vipSubDetailId);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public String getGroupType(long groupId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String result = null;
        String sqlMo = "select group_type from vip_sub_groups where group_id = ? ";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, groupId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = rs1.getString("group_type");
                return result;
            }
            logTimeDb("Time to getGroupType group_id " + groupId + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getGroupType group_id  " + groupId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }
}
