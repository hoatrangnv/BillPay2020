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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
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
public class DbBocProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbBocProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDeleteMo = "delete log_update_info where LOG_UPDATE_INFO_ID = ?";

    public DbBocProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbBocNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbBocProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        LogUpdateInfo record = new LogUpdateInfo();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong(LogUpdateInfo.LOG_UPDATE_INFO_ID));
            record.setLogUpdateInfoId(rs.getLong(LogUpdateInfo.LOG_UPDATE_INFO_ID));
            record.setStaffCode(rs.getString(LogUpdateInfo.STAFF_CODE));
            record.setIsdn(rs.getString(LogUpdateInfo.ISDN));
            record.setContractOne(rs.getString(LogUpdateInfo.NUMBER_CONTACT1));
            record.setContractTwo(rs.getString(LogUpdateInfo.NUMBER_CONTACT2));
            record.setCreateTime(rs.getTimestamp(LogUpdateInfo.CREATE_TIME));
            record.setCheckStatus(rs.getLong("check_status"));
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
        int[] res = new int[0];
        try {
            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbBocNameConfig");
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                LogUpdateInfo sd = (LogUpdateInfo) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getLogUpdateInfoId());
                ps.addBatch();
            }
            res = ps.executeBatch();            
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue LOG_UPDATE_INFO batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue LOG_UPDATE_INFO, batchid " + batchId, timeStart);
            return res;
        }
    }

//    public int deleteQueueV2(Long logUpdateInfoId) {
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder brBuilder = new StringBuilder();
//        int result = 0;
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("dbapp2");
//            ps = connection.prepareStatement(sqlDeleteMo);
//            ps.setLong(1, logUpdateInfoId);
//            result = ps.executeUpdate();
//            logger.info("End deleteQueueV2 logUpdateInfoId " + logUpdateInfoId + " result " + result + " time "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            brBuilder.setLength(0);
//            brBuilder.append(loggerLabel).append(new Date()).
//                    append("\nERROR deleteQueueV2: ").
//                    append(sqlDeleteMo).append("\n")
//                    .append(" logUpdateInfoId ")
//                    .append(logUpdateInfoId)
//                    .append(" result ")
//                    .append(result);
//            logger.error(brBuilder + ex.toString());
//            result = -1;
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            return result;
//        }
//    }
//    public int insertQueueHisV2(LogUpdateInfo sd) {
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder br = new StringBuilder();
//        String sql = "";
//        int result = 0;
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("dbapp2");
//            sql = "INSERT INTO log_update_info_his (STAFF_CODE,ISDN,NUMBER_CONTACT1,NUMBER_CONTACT2,CREATE_TIME,PROCESS_TIME,MONEY_ADD,ACCOUNT_CODE,LOG_UPDATE_INFO_ID,RESULT_CODE,DESCRIPTION) \n"
//                    + "VALUES(?,?,'0','0',?,sysdate,null,null,?,?,?)";
//            ps = connection.prepareStatement(sql);
//            ps.setString(1, sd.getStaffCode());
//            ps.setString(2, sd.getIsdn());
//            ps.setTimestamp(3, new Timestamp(sd.getCreateTime().getTime()));
//            ps.setLong(4, sd.getLogUpdateInfoId());
//            ps.setString(5, sd.getResultCode());
//            ps.setString(6, sd.getDescription());
//
//            result = ps.executeUpdate();
//            logger.info("End insertQueueHisV2 isdn " + sd.getIsdn() + " result " + result + " time "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).
//                    append("\nERROR insertQueueHisV2: ").
//                    append(sql).append("\n")
//                    .append(" isdn ")
//                    .append(sd.getIsdn())
//                    .append(" result ")
//                    .append(result);
//            logger.error(br + ex.toString());
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            return result;
//        }
//    }
    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {                
                LogUpdateInfo sd = (LogUpdateInfo) rc;                                
                    batchId = sd.getBatchId();
                    ParamList paramList = new ParamList();
                    paramList.add(new Param(LogUpdateInfo.LOG_UPDATE_INFO_ID, sd.getLogUpdateInfoId(), Param.DataType.LONG, Param.IN));
                    paramList.add(new Param(LogUpdateInfo.STAFF_CODE, sd.getStaffCode(), Param.DataType.STRING, Param.IN));
                    paramList.add(new Param(LogUpdateInfo.ISDN, sd.getIsdn(), Param.DataType.STRING, Param.IN));
                    paramList.add(new Param(LogUpdateInfo.NUMBER_CONTACT1, sd.getContractOne(), Param.DataType.STRING, Param.IN));
                    paramList.add(new Param(LogUpdateInfo.NUMBER_CONTACT2, sd.getContractTwo(), Param.DataType.STRING, Param.IN));
                    paramList.add(new Param(LogUpdateInfo.ACCOUNT_CODE, sd.getAccountCode(), Param.DataType.STRING, Param.IN));
                    paramList.add(new Param(LogUpdateInfo.MONEY_ADD, sd.getMoneyAdd(), Param.DataType.LONG, Param.IN));
                    paramList.add(new Param(LogUpdateInfo.CREATE_TIME, sd.getCreateTime(), Param.DataType.TIMESTAMP, Param.IN));
                    paramList.add(new Param(LogUpdateInfo.PROCESS_TIME, "sysdate", Param.DataType.CONST, Param.IN));
                    paramList.add(new Param(LogUpdateInfo.RESULT_CODE, sd.getResultCode(), Param.DataType.STRING, Param.IN));
                    paramList.add(new Param(LogUpdateInfo.DESCRIPTION, sd.getDescription(), Param.DataType.STRING, Param.IN));
                    listParam.add(paramList);                
            }
            if (listParam.size() > 0) {
                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "LOG_UPDATE_INFO_HIS");
                logTimeDb("Time to insertQueueHis LOG_UPDATE_INFO_HIS, batchid " + batchId + " total result: " + res.length, timeSt);
                return res;
            } else {
                int[] res = new int[0];
                return res;
            }

        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis LOG_UPDATE_INFO_HIS batchid " + batchId, ex);
            try {
                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "LOG_UPDATE_INFO_HIS");
                logTimeDb("Time to retry insertQueueHis LOG_UPDATE_INFO_HIS, batchid " + batchId + " total result: " + res.length, timeSt);
                return res;
            } catch (Exception ex1) {
                logger.error("ERROR retry insertQueueHis LOG_UPDATE_INFO_HIS batchid " + batchId, ex1);
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
                LogUpdateInfo sd = (LogUpdateInfo) rc;
                if (sd.getMessage() == null || sd.getIsdn() == null || sd.getMessage().trim().length() <= 0
                        || sd.getIsdn().trim().length() <= 0) {
                    continue;
                }
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("MO_HIS_ID", sd.getLogUpdateInfoId(), Param.DataType.LONG, Param.IN));
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
            deleteQueueTimeout(ids);
            for (String sd : ids) {
                sb.append(":" + sd);
                ParamList paramList = new ParamList();
                paramList.add(new Param(LogUpdateInfo.LOG_UPDATE_INFO_ID, Long.valueOf(sd), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(LogUpdateInfo.RESULT_CODE, "FW_99", Param.DataType.STRING, Param.IN));
                paramList.add(new Param(LogUpdateInfo.DESCRIPTION, "FW_Timeout", Param.DataType.STRING, Param.IN));
                paramList.add(new Param(LogUpdateInfo.PROCESS_TIME, "sysdate", Param.DataType.CONST, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "LOG_UPDATE_INFO_HIS");
            logTimeDb("Time to processTimeoutRecord, insert LOG_UPDATE_INFO_HIS, total result: " + res.length, timeSt);
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
            try {
                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "LOG_UPDATE_INFO_HIS");
                logTimeDb("Time to retry processTimeoutRecord, insert LOG_UPDATE_INFO_HIS, total result: " + res.length, timeSt);
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

    public boolean checkAlreadyProcessRecord(long idRecord) {
        /**
         * SELECT log_update_info_id FROM log_update_info WHERE
         * log_update_info_id = idRecord
         */
        ParamList paramList = new ParamList();
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("log_update_info_id", idRecord, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("log_update_info_id", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "log_update_info_his");
            while (rs.next()) {
                long id = rs.getLong("log_update_info_id");
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
            paramList.add(new Param("result_code", null, Param.DataType.STRING, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "log_update_info_his");
            while (rs.next()) {
                long id = rs.getLong("log_update_info_id");
                String resultCode = rs.getString("result_code");
                if (id > 0 && "0".equals(resultCode)) {
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
            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbBocNameConfig");
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
            logger.error("ERROR deleteQueueTimeout log_update_info listId " + sf.toString(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueueTimeout log_update_info, listId " + sf.toString(), timeStart);
        }
    }

    //LinhNBV start modified on Jun 27 2018: Check profile correct or not correct
    public Long getProfileCheckStatus(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        Long checkStatus = null;
        try {
            connection = getConnection("cm_pre");
            ps = connection.prepareStatement("select * from cm_pre.sub_profile_info where isdn = ?");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                checkStatus = rs.getLong("check_status");

                break;
            }
            logTimeDb("Time to getProfileCheckStatus " + " checkStatus: " + checkStatus, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getProfileCheckStatus default return false");
            logger.error(AppManager.logException(timeSt, ex));
            checkStatus = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return checkStatus;
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
}
