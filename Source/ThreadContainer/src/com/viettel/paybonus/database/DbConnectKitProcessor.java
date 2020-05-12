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
public class DbConnectKitProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbConnectKitProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
//    private String callChangeCommAccount = "{call sm.change_comm_account(?,?,?,?,?,?,?,?,?,?,?)}";
    private String sqlDeleteMo = "update cm_pre.sub_profile_info set connect_kit_status = 1 where sub_profile_id = ?";

    public DbConnectKitProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbConnectKitProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
            record.setSimSerial(rs.getString("sim_serial"));
            record.setVoucherCode(rs.getString("voucher_code"));
            record.setCustId(rs.getLong("cust_id"));
            record.setConnectKitStatus(rs.getString("connect_kit_status"));
            record.setVasCode(rs.getString("vas_code"));
            record.setPrepaidMonth(rs.getInt("prepaid_month"));
            record.setHandsetModel(rs.getString("handset_serial"));
            record.setPayMethod(rs.getInt("pay_method"));
            record.setBankName(rs.getString("bank_name"));
            record.setBankTranCode(rs.getString("bank_tran_code"));
            record.setBankTranAmount(rs.getString("bank_tran_amount"));
            record.setReferenceId(rs.getString("reference_id"));
            record.setMainProduct(rs.getString("main_product"));
            record.setMainPrice(rs.getString("main_price"));
            record.setResultCode("0");
            record.setDescription("Start Processing");
            //LinhNBV add serial SIM to record
            //record.setSimSerial(rs.getString("sim_serial"));
            //end.
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
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                Bonus sd = (Bonus) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR updateQueue SUB_PROFILE_INFO batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to updateQueue SUB_PROFILE_INFO, batchid " + batchId, timeStart);
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
                paramList.add(new Param("SIM_SERIAL", sd.getSimSerial(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("PAY_BONUS_DESC", sd.geteWalletDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("ADD_ON_PRODUCT", (sd.getVasCode() != null && !sd.getVasCode().isEmpty()) ? sd.getVasCode() : sd.getProductCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("MAIN_PRODUCT", sd.getMainProduct(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("NODE_NAME", sd.getNodeName(), Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "KIT_PROCESS");
            logTimeDb("Time to insertQueueHis KIT_PROCESS, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis KIT_PROCESS batchid " + batchId, ex);
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
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "KIT_PROCESS");
            logTimeDb("Time to processTimeoutRecord, insert KIT_PROCESS, total result: " + res.length, timeSt);
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
            try {
                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "KIT_PROCESS");
                logTimeDb("Time to retry processTimeoutRecord, insert KIT_PROCESS, total result: " + res.length, timeSt);
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
            paramList.add(new Param("isdn_wallet", "", Param.OperatorType.IS_NOT_NULL, Param.DataType.CONST, Param.IN));
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
                if (isdnEmola != null && isdnEmola.trim().length() > 0) {
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

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
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
//LinhNBV 20181104: Modified check staff don't have target

    public boolean checkStaffTarget(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(
                    //LinhNBV start modified on April 12 2018: Add condition to check profile of channel correct or not
                    "select * from profile.bonus_agent_vip where status = 1 and upper(staff_code) = upper(?)");
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
            logTimeDb("Time to checkStaffTarget " + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkStaffTarget defaul return false");
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
            paramList.add(new Param("BONUS_TYPE", 4L, Param.DataType.LONG, Param.IN));
            PoolStore.PoolResult prs = poolStore.insertTable(paramList, "EWALLET_LOG");
            logTimeDb("Time to insertEwalletLog isdn " + log.getMobile(), timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getMobile());
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    //LinhNBV start modified on Jun 06 2018: Add method using update or rollback status stock_isdn_mobile or stock_sim.
    public int destroySubMbRecord(Long status, String serial, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
//            sql = "update sub_mb set status = ? where serial = ? and isdn = ?";
            sql = "delete sub_mb where serial = ? and isdn = ?";
            ps = connection.prepareStatement(sql);
//            ps.setLong(1, status);
            ps.setString(1, serial);
            ps.setString(2, isdn);
            result = ps.executeUpdate();
            logger.info("End destroySubMbRecord isdn " + isdn + " serial " + serial + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR destroySubMbRecord: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" serial ")
                    .append(serial)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateStockIsdn(Long status, String lastUpdateUser, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update stock_isdn_mobile set status = ?, last_update_user = ?, last_update_time = sysdate where to_number(isdn) = ? \n"
                    + "and (status = 1 or status = 3 or status = 5 or status = 6)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, status);
            ps.setString(2, lastUpdateUser);
            ps.setString(3, isdn);
            result = ps.executeUpdate();
            logger.info("End updateIsdnStatusToWaitingConnect isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateIsdnStatusToWaitingConnect: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateStockSim(Long status, Long hlrStatus, String serial, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;

        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update stock_sim set status = ?, hlr_status = ?, hlr_reg_date = sysdate where to_number(serial) = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, status);
            ps.setLong(2, hlrStatus);
            ps.setString(3, serial);

            result = ps.executeUpdate();
            logger.info("End updateStockSimToWaitingConnect serial " + serial + " isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            StringBuilder brBuilder = new StringBuilder();
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateStockSimToWaitingConnect: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial)
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getImsiEkiSimBySerial(String serial, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder btBuilder = new StringBuilder();
        StringBuilder result = new StringBuilder();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select imsi, eki from stock_sim where to_number(serial) = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            rs = ps.executeQuery();
            while (rs.next()) {
                String imsi = rs.getString("imsi");
                result.append(imsi).append("|");
                String eki = rs.getString("eki");
                result.append(eki);
                logger.info("checkSerialSimIsSale " + imsi + " isdn " + isdn);
            }
            logger.info("End checkSerialSimIsSale serial " + serial + " isdn " + isdn
                    + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            btBuilder.setLength(0);
            btBuilder.append(loggerLabel).append(new Date()).append("\nERROR checkSerialSimIsSale ---- serial ").append(serial).append(" Message: ").
                    append(ex.getMessage());
            logger.error(btBuilder + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result.toString();
        }
    }

    public String getProductCode(String serial, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String productCode = null;
        String sqlMo = "select * from sub_mb where serial = ? and isdn = ? and status = 2";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, serial);
            psMo.setString(2, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                productCode = rs1.getString("product_code");
            }
            if (productCode == null) {
                productCode = "";
                logger.info("productCode is null - serial: " + serial);
            }
            logTimeDb("Time to getProductCode: " + serial + " isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getProductCode " + serial + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return productCode;
        }
    }

    public Long getReasonIdByProductCode(String productCode, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Long reasonId = null;
        String sqlMo = "select * from Reason r where r.status = 1 and r.type = '00'\n"
                + "and r.reason_Id in (select m.reason_Id from Mapping m where (m.product_Code = ? or m.product_Code is null) \n"
                + "and m.tel_Service_Id = 1 and m.status = 1 \n"
                + "and ( m.end_Date is null or m.end_Date >= trunc(sysdate))\n"
                + ")\n"
                + "order by NLSSORT(r.code,'NLS_SORT=vietnamese')";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, productCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                reasonId = rs1.getLong("reason_id");
                logger.info("Get reason_i " + reasonId + " base on productCode " + productCode + " for isdn " + isdn);
            }
            logTimeDb("Time to getReasonIdByProductCode: " + reasonId
                    + " base on productCode " + productCode + " for isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getReasonIdByProductCode " + reasonId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return reasonId;
        }
    }

    public String getSaleServiceCode(Long reasonId, String productCode, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String saleServiceCode = null;
        String sqlMo = "Select * from Mapping m, Reason r \n"
                + "where m.reason_Id = r.reason_Id and r.status = 1 and m.status = 1 \n"
                + "and m.channel is null and m.reason_Id = ? and (m.product_Code = ? or m.product_Code is null)";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, reasonId);
            psMo.setString(2, productCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                saleServiceCode = rs1.getString("sale_service_code");
                logger.info("sale_service_code " + saleServiceCode
                        + " base on productCode " + productCode + " reasonId " + reasonId + " isdn " + isdn);
            }
            logTimeDb("Time to getSaleServiceCode: " + saleServiceCode
                    + " base on productCode " + productCode + " reasonId " + reasonId + " isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSaleServiceCode " + " base on productCode " + productCode + " reasonId " + reasonId + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return saleServiceCode;
        }
    }

    public SaleServices getSaleService(String saleServiceCode, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        SaleServices saleService = null;
        String sqlMo = "select * from sm.sale_services where code = ? and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, saleServiceCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Long saleServiceId = rs1.getLong("sale_services_id");
                String name = rs1.getString("name");
                String accountModelCode = rs1.getString("accounting_model_code");
                String accountModelName = rs1.getString("accounting_model_name");
                saleService = new SaleServices();
                saleService.setSaleServicesId(saleServiceId);
                saleService.setName(name);
                saleService.setAccountModelCode(accountModelCode);
                saleService.setAccountModelName(accountModelName);
                logger.info("Result getSaleService  " + saleServiceCode
                        + " saleServiceId " + saleServiceId + " isdn " + isdn);
                break;
            }
            logTimeDb("Time to getSaleService: " + saleServiceCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSaleService " + saleServiceCode + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return saleService;
        }
    }

    public int updateSubIdNo(Long custId, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update sub_id_no set end_datetime = sysdate, status = 0 where lower(id_no) = (select lower(id_no) from customer where cust_id =?) and isdn = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, custId);
            ps.setString(2, isdn);
            result = ps.executeUpdate();
            logger.info("End updateSubIdNo isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            StringBuilder brBuilder = new StringBuilder();
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSubIdNo: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public SaleServicesPrice getSaleServicesPrice(Long saleServicesId, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        SaleServicesPrice saleServicesPrice = null;
        String sqlMo = "select * from Sale_Services_Price where sale_Services_Id = ? \n"
                + "and sta_Date <= to_date(to_char(trunc(sysdate),'MM/DD/YYYY')||' 23:59:59','MM/DD/YYYY HH24:MI:SS')\n"
                + "and (((end_Date >= to_date(to_char(trunc(sysdate),'MM/DD/YYYY')||' 00:00:00','MM/DD/YYYY HH24:MI:SS')) and (end_Date is not null)) or (end_Date is null)) \n"
                + "and status = 1 and price_Policy = 1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, saleServicesId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Long saleServicesPriceId = rs1.getLong("sale_services_price_id");
                Double price = rs1.getDouble("price");
                Double vat = rs1.getDouble("vat");
                saleServicesPrice = new SaleServicesPrice();
                saleServicesPrice.setSaleServicesPriceId(saleServicesPriceId);
                saleServicesPrice.setPrice(price);
                saleServicesPrice.setVat(vat);
                logger.info("Result getSaleServicesPrice saleServicesId "
                        + saleServicesId + " saleServicesPriceId " + saleServicesPriceId + " isdn " + isdn);
                break;
            }
            logTimeDb("Time to getSaleServicesPrice: " + saleServicesId + " isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSaleServicesPrice " + saleServicesId + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return saleServicesPrice;
        }
    }

    public Price getPrice(String isdn, Long saleServicesId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Price priceObj = null;
        String sqlMo = "select * from price where 1 = 1 and price_id = (select price_id from Sale_Services_Detail where stock_model_id = (select stock_model_id from stock_isdn_mobile where isdn = ?) and status= 1 \n"
                + "and sale_Services_Model_Id in (select sale_Services_Model_Id from Sale_Services_Model where sale_Services_Id = ? and stock_Type_Id = 1 )) \n"
                + "and status = 1 and trunc(sta_date) <= trunc(sysdate) and (end_Date is null or trunc(end_Date) >= trunc(sysdate))";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, isdn);
            psMo.setLong(2, saleServicesId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Long priceId = rs1.getLong("price_id");
                Long stockModelId = rs1.getLong("stock_model_id");
                Double price = rs1.getDouble("price");
                priceObj = new Price();
                priceObj.setPriceId(priceId);
                priceObj.setStockModelId(stockModelId);
                priceObj.setPrice(price);
                logger.info("Result getPrice " + saleServicesId + " isdn " + isdn + " priceId " + priceId);
                break;
            }
            logTimeDb("Time to getPrice: " + saleServicesId + " isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getPrice " + saleServicesId + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return priceObj;
    }

    public Long getSequence(String sequenceName, String dbName) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Long sequenceValue = null;
        String sqlMo = "select " + sequenceName + ".nextval as sequence from dual";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbName);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                sequenceValue = rs1.getLong("sequence");
            }
            logTimeDb("Time to getSequence: " + sequenceName, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSequence " + sequenceName);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return sequenceValue;
    }

    public String getIsdnWalletByStaffCode(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String isdnWallet = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select isdn_wallet from staff where lower(staff_code) = lower(?) and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                isdnWallet = rs.getString("isdn_wallet");
                break;
            }
            logger.info("End getIsdnWalletByStaffCode: " + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getIsdnWalletByStaffCode ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            isdnWallet = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return isdnWallet;
        }
    }

    public int insertEwalletConnectKitLog(Long transactionType, String requestId, String amount,
            String voucherCode, String staffCode, String request, String response, String errCode, String description, String orgRequestId,
            String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into EWALLET_CONNECT_KIT_LOG values (EWALLET_CONNECT_KIT_LOG_SEQ.nextval,?,?,?,?,?,sysdate,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, transactionType);
            ps.setString(2, requestId);
            ps.setString(3, amount);
            ps.setString(4, voucherCode);
            ps.setString(5, staffCode);
            ps.setString(6, request);
            ps.setString(7, response);
            ps.setString(8, errCode);
            ps.setString(9, description);
            ps.setString(10, orgRequestId);
            result = ps.executeUpdate();
            logger.info("End insertEwalletConnectKitLog staffCode " + staffCode + " isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertEwalletConnectKitLog: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
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

    public String getShopIdStaffIdByStaffCode(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        StringBuilder result = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from staff where lower(staff_code) = lower(?) and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long shopId = rs.getLong("shop_id");
                Long staffId = rs.getLong("staff_id");
                result = new StringBuilder();
                result.append(shopId).append("|").append(staffId);
                break;
            }
            logger.info("End getShopIdStaffIdByStaffCode: " + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getShopIdStaffIdByStaffCode ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            result = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result.toString();
        }
    }

    public int insertSaleTransOrder(String bankName, String bankTranAmount, String bankTranCode, Long saleTransId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "insert into sale_trans_order(sale_trans_order_id,bank_name,amount,is_check,order_code,sale_trans_date,sale_trans_id,status,sale_trans_type,note)\n"
                    + "values(sale_trans_order_seq.nextval,?,?,3,?,sysdate,?,5,4,'Clear by bankTransfer from connectKitBatch.')";
            ps = connection.prepareStatement(sql);
            ps.setString(1, bankName);
            ps.setString(2, bankTranAmount);
            ps.setString(3, bankTranCode);
            ps.setLong(4, saleTransId);
            result = ps.executeUpdate();
            logger.info("End insertSaleTransOrder saleTransId " + saleTransId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTransOrder: ").
                    append(sql).append("\n")
                    .append(" saleTransId ")
                    .append(saleTransId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertSaleTrans(Long saleTransId, Long shopId, Long staffId, Long saleServiceId, Long saleServicePriceId, Double amountTax,
            Long subId, String isdn, Long reasonId, String ewalletRequestId, int payMethod, String bankTransCode, double discount, double vasAmount, Long subProfileId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO sm.sale_trans (SALE_TRANS_ID,SALE_TRANS_DATE,SALE_TRANS_TYPE,STATUS,CHECK_STOCK,INVOICE_USED_ID,"
                    + "INVOICE_CREATE_DATE,SHOP_ID,STAFF_ID,PAY_METHOD,SALE_SERVICE_ID,SALE_SERVICE_PRICE_ID,AMOUNT_SERVICE,"
                    + "AMOUNT_MODEL,DISCOUNT,PROMOTION,AMOUNT_TAX,AMOUNT_NOT_TAX,VAT,TAX,SUB_ID,ISDN,CUST_NAME,CONTRACT_NO,"
                    + "TEL_NUMBER,COMPANY,ADDRESS,TIN,NOTE,DESTROY_USER,DESTROY_DATE,APPROVER_USER,APPROVER_DATE,REASON_ID,"
                    + "TELECOM_SERVICE_ID,TRANSFER_GOODS,SALE_TRANS_CODE,STOCK_TRANS_ID,CREATE_STAFF_ID,RECEIVER_ID,SYN_STATUS,"
                    + "RECEIVER_TYPE,FROM_SALE_TRANS_ID,DAILY_SYN_STATUS,CURRENCY,CHANNEL,SALE_TRANS,SERIAL_STATUS,"
                    + "INVOICE_DESTROY_ID,SALE_PROGRAM,SALE_PROGRAM_NAME,PARENT_MASTER_AGENT_ID,PAYMENT_PAPERS_CODE,AMOUNT_PAYMENT,"
                    + "LAST_UPDATE,CLEAR_DEBIT_STATUS,CLEAR_DEBIT_TIME,CLEAR_DEBIT_USER,CLEAR_DEBIT_REQUEST_ID,IN_TRANS_ID)  "
                    + " VALUES(?,sysdate,'4','2',NULL,NULL,NULL,?,?,'1',?,?,NULL,NULL,?,NULL, "
                    + "?,?,17,?,?,?,NULL,NULL,?,NULL,NULL,\n"
                    + "NULL,NULL,NULL,NULL,NULL,NULL,?,1,NULL,?,NULL,NULL,NULL,'0',NULL,NULL,0,'MT',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,\n"
                    + "sysdate,?,?,?,?,?)";//'SYSTEM_AUTO_CN_KIT'
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransId);
            ps.setLong(2, shopId);
            ps.setLong(3, staffId);
            if (saleServiceId > 0) {
                ps.setLong(4, saleServiceId);
            } else {
                ps.setString(4, "");
            }
            if (saleServicePriceId > 0) {
                ps.setLong(5, saleServicePriceId);
            } else {
                ps.setString(5, "");
            }
            if (discount > 0) {
                ps.setDouble(6, discount / 1.17);
            } else {
                ps.setString(6, "");
            }
            ps.setDouble(7, amountTax);
            Double amountNotTax = amountTax / 1.17;
            ps.setDouble(8, amountNotTax);
            Double tax = amountTax - amountNotTax;
            ps.setDouble(9, tax);
            ps.setLong(10, subId);
            ps.setString(11, isdn);
            ps.setString(12, isdn);
            ps.setLong(13, reasonId);
            String prefix = "";
            if (vasAmount > 0 && amountTax == 0) {
                prefix = "EMOLA0000";
            } else {
                prefix = "SS0000";
            }
            String saleTransCode = prefix + String.format("%0" + 9 + "d", saleTransId);
            ps.setString(14, saleTransCode);
            if ((payMethod == 1 && ewalletRequestId != null && ewalletRequestId.length() > 0) || amountTax == 0) {
                ps.setLong(15, 1L);
                ps.setTimestamp(16, new Timestamp(new Date().getTime()));
                ps.setString(17, "SYSTEM_AUTO_CN_KIT");
                ps.setString(18, ewalletRequestId);
            } //            else if (payMethod == 0) {
            //                ps.setLong(15, 1L);
            //                ps.setTimestamp(16, new Timestamp(new Date().getTime()));
            //                ps.setString(17, "CLEAR_BY_BANK_TRANSFER");
            //                ps.setString(18, bankTransCode);
            //            } 
            else {
                ps.setString(15, "");
                ps.setNull(16, java.sql.Types.DATE);
                ps.setString(17, "");
                ps.setString(18, "");

            }
            ps.setLong(19, subProfileId);
            result = ps.executeUpdate();
            logger.info("End insertSaleTrans saleTransId " + saleTransId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTrans: ").
                    append(sql).append("\n")
                    .append(" saleTransId ")
                    .append(saleTransId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertSaleTransDetail(Long saleTransDetailId, Long saleTransId, String stockModelId, String priceId, String saleServiceId, String saleServicePriceId,
            String stockTypeId, String stockTypeName, String stockModelCode, String stockModelName, String saleServicesCode, String saleServicesName, String accountModelCode,
            String accountModelName, String saleServicesPriceVat, String priceVat, String price, String saleServicesPrice, Double amountTax, Double discountAmout, Long quantity) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO sm.sale_trans_detail (SALE_TRANS_DETAIL_ID,SALE_TRANS_ID,SALE_TRANS_DATE,STOCK_MODEL_ID,STATE_ID,PRICE_ID,QUANTITY,DISCOUNT_ID,"
                    + "TRANSFER_GOOD,PROMOTION_ID,PROMOTION_AMOUNT,NOTE,UPDATE_STOCK_TYPE,USER_DELIVER,DELIVER_DATE,USER_UPDATE,DELIVER_STATUS,SALE_SERVICES_ID,"
                    + "SALE_SERVICES_PRICE_ID,STOCK_TYPE_ID,STOCK_TYPE_CODE,STOCK_TYPE_NAME,STOCK_MODEL_CODE,STOCK_MODEL_NAME,SALE_SERVICES_CODE,SALE_SERVICES_NAME,"
                    + "ACCOUNTING_MODEL_CODE,ACCOUNTING_MODEL_NAME,CURRENCY,VAT_AMOUNT,SALE_SERVICES_PRICE_VAT,PRICE_VAT,PRICE,SALE_SERVICES_PRICE,AMOUNT,"
                    + "DISCOUNT_AMOUNT,AMOUNT_BEFORE_TAX,AMOUNT_TAX,AMOUNT_AFTER_TAX)\n"
                    + "VALUES(?,?,sysdate,?,1,?,?,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,"
                    + "?,?,?,NULL,?,?,?,?,?,?,?,'MT',?,?,?,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransDetailId);
            ps.setLong(2, saleTransId);
            ps.setString(3, stockModelId);
            ps.setString(4, priceId);
            ps.setLong(5, quantity);
            ps.setString(6, saleServiceId);
            ps.setString(7, saleServicePriceId);
            ps.setString(8, stockTypeId);
            ps.setString(9, stockTypeName);
            ps.setString(10, stockModelCode);
            ps.setString(11, stockModelName);
            ps.setString(12, saleServicesCode);
            ps.setString(13, saleServicesName);
            ps.setString(14, accountModelCode);
            ps.setString(15, accountModelName);
            Double amountNotTax = amountTax / 1.17;
            Double tax = amountTax - amountNotTax;
            ps.setDouble(16, tax);
            ps.setString(17, saleServicesPriceVat);
            ps.setString(18, priceVat);
            ps.setString(19, price);
            ps.setString(20, saleServicesPrice);
            ps.setDouble(21, amountTax);
            if (discountAmout > 0) {
                ps.setDouble(22, discountAmout / 1.17);
                ps.setDouble(23, (amountTax - discountAmout) / 1.17);
                ps.setDouble(24, (amountTax - discountAmout) - (amountTax - discountAmout) / 1.17);
                ps.setDouble(25, amountTax - discountAmout);
            } else {
                ps.setString(22, "");
                ps.setDouble(23, amountNotTax);
                ps.setDouble(24, tax);
                ps.setDouble(25, amountTax);
            }



            result = ps.executeUpdate();
            logger.info("End insertSaleTrans saleTransId " + saleTransId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTrans: ").
                    append(sql).append("\n")
                    .append(" saleTransId ")
                    .append(saleTransId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertSaleTransSerial(Long saleTransSerialId, Long saleTransDetailId, Long stockModelId, String isdn) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO sm.sale_trans_serial \n"
                    + "VALUES(?,?,?,sysdate,NULL,NULL,NULL,?,?,1)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransSerialId);
            ps.setLong(2, saleTransDetailId);
            ps.setLong(3, stockModelId);
            ps.setString(4, isdn);
            ps.setString(5, isdn);

            result = ps.executeUpdate();
            logger.info("End insertSaleTransSerial saleTransDetailId " + saleTransDetailId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTransSerial: ").
                    append(sql).append("\n")
                    .append(" saleTransDetailId ")
                    .append(saleTransDetailId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public Long getStockModelIdByIsdn(String isdn) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Long stockModelId = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select stock_model_id from sm.stock_isdn_mobile where isdn = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                stockModelId = rs.getLong("stock_model_id");
                break;
            }
            logger.info("End getStockModelIdByIsdn: " + isdn + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getStockModelIdByIsdn ").append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            stockModelId = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return stockModelId;
        }
    }

    public StockModel findStockModelById(Long stockModelId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        StockModel stockModel = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from sm.stock_model where stock_model_id = ? and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, stockModelId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String stockModelCode = rs.getString("stock_model_code");
                String name = rs.getString("name");
                String accountModelCode = rs.getString("accounting_model_code");
                String accountModelName = rs.getString("accounting_model_name");
                stockModel = new StockModel();
                stockModel.setStockModelCode(stockModelCode);
                stockModel.setName(name);
                stockModel.setAccountingModelCode(accountModelCode);
                stockModel.setAccountingModelName(accountModelName);
                break;
            }
            logger.info("End findStockModelById: " + stockModelId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR findStockModelById ").append(stockModelId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            stockModel = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return stockModel;
        }
    }

    public int insertDataKitVas(String isdn, String serial, String userCreated, String productCode) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into kit_vas(kit_vas_id, isdn, serial, status, create_user, create_date, product_code) "
                    + "values (kit_vas_seq.nextval,?,?,1,?,sysdate,?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setString(2, serial);
            ps.setString(3, userCreated);
            ps.setString(4, productCode);
            result = ps.executeUpdate();
            logger.info("End insertDataKitVas userCreated " + userCreated + " isdn: " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertDataKitVas: ").
                    append(sql).append("\n")
                    .append(" userCreated ")
                    .append(userCreated)
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

    public int insertActionAudit(String isdn, String serial, String des, long subId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "INSERT INTO action_audit (ACTION_AUDIT_ID,ISSUE_DATETIME,ACTION_CODE,REASON_ID,SHOP_CODE,USER_NAME,"
                    + " PK_TYPE,PK_ID,IP,DESCRIPTION) "
                    + " VALUES(seq_action_audit.nextval,sysdate,'00',4432,'ITBILLING','ITBILLING', "
                    + "'3',?,'127.0.0.1',?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, subId);
            ps.setString(2, des);
            result = ps.executeUpdate();
            logger.info("End insertActionAudit isdn " + isdn + " serial " + serial
                    + " subId " + subId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionAudit: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" serial ")
                    .append(serial)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getStaffType(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String result = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from sm.staff where lower(staff_code) = lower(?) and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getString("type");
                break;
            }
            logger.info("End getStaffType: staffCode" + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getStaffType ").append(staffCode).append(" Message: ").
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

    public List<String> getChiefCenterUser(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<String> lstChiefCenter = new ArrayList<String>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from staff where shop_id = (select shop_id from staff where upper(staff_code) = upper(?)) and status = 1 and channel_type_id = 14 and type = 6 \n"
                    + "and upper(staff_code) not in (select upper(staff_code) from user_not_pay_commission)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                String tmpChiefCenter = rs.getString("staff_code");
                lstChiefCenter.add(tmpChiefCenter);
            }
            logger.info("End getChiefCenterUser: staffCode" + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getChiefCenterUser ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return lstChiefCenter;
        }
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

    public Comission getComissionStaff(String staffCode, String productCode, String isdn) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Comission comission = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            String sql = "select * from product_connect_kit where product_code = ? and status = 1 and bonus_channel > 0";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                long bonusCenter = rs.getLong("bonus_center");
                long bonusCtv = rs.getLong("bonus_ctv");
                long bonusChannel = rs.getLong("bonus_channel");
                long bonusCtvCenter = rs.getLong("bonus_ctv_center");
                long bonusChannelCtv = rs.getLong("bonus_channel_ctv");
                long bonusChanelCenter = rs.getLong("bonus_channel_center");
                comission = new Comission();
                comission.setBonusCenter(bonusCenter);
                comission.setBonusCtv(bonusCtv);
                comission.setBonusChannel(bonusChannel);
                comission.setBonusCtvCenter(bonusCtvCenter);
                comission.setBonusChannelCtv(bonusChannelCtv);
                comission.setBonusChannelCenter(bonusChanelCenter);
                break;
            }
            logger.info("End getComissionStaff: staffCode " + staffCode + " isdn " + isdn + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getComissionStaff ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            comission = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return comission;
        }
    }

    public int insertLogMakeSaleTransFail(Long saleTransId, String isdn, String serial, Long shopId, Long staffId, Long saleServicesId, Long saleServicesPriceId,
            Long reasonId, Long saleTransDetailId, Long saleTransSerialId, String tableName) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into kit_make_sale_trans_fail values (kit_make_sale_trans_fail_seq.nextval,?,?,?,?,?,?,?,?,?,?,?,sysdate)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransId);
            ps.setString(2, isdn);
            ps.setString(3, serial);
            ps.setLong(4, shopId);
            ps.setLong(5, staffId);
            ps.setLong(6, saleServicesId);
            ps.setLong(7, saleServicesPriceId);
            ps.setLong(8, reasonId);
            ps.setLong(9, saleTransDetailId);
            ps.setLong(10, saleTransSerialId);
            ps.setString(11, tableName);
            result = ps.executeUpdate();
            logger.info("End insertLogMakeSaleTransFail isdn " + isdn + " serial " + serial
                    + " saleTransId " + saleTransId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertLogMakeSaleTransFail: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" serial ")
                    .append(serial)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getMainProduct(String productCode, String isdn) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String mainProduct = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            String sql = "select * from product.Price_Plan where status = 1 \n"
                    + "and price_Plan_Id in (Select price_Plan_Id from product.Product_Offer_Pp \n"
                    + "where product_Offer_Id = (select offer_id from product.product_offer where product_id = (select product_id from product.product \n"
                    + "where status = 1 and product_type = 'P' and upper(product_code) = upper(?))) \n"
                    + "and (expire_Date is null or expire_Date >= trunc(sysdate)))";

            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                mainProduct = rs.getString("price_plan_code");
                logger.info("price_plan_code " + mainProduct + " isdn " + isdn);
                break;
            }
            logger.info("End getMainProduct: productCode" + productCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getMainProduct productCode").append(productCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString() + " isdn " + isdn);
            mainProduct = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return mainProduct;
        }
    }

    public int insertLogRegisterInfo(String staffCode, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp2");
            sql = "INSERT INTO log_register_info (log_register_info_id,staff_code,isdn,create_time) VALUES(log_register_info_seq.nextval,?,?, sysdate)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            ps.setString(2, isdn);
            result = ps.executeUpdate();
            logger.info("End insertLogRegisterInfo staffCode " + staffCode + " isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertLogRegisterInfo: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
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

    public int insertActionLogPr(String isdn, String serial, String shopCode, String staffCode,
            String request, String response, String responseCode) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "INSERT INTO action_log_pr \n"
                    + "VALUES(seq_action_log_pr.nextval,NULL,?,?,NULL,NULL,sysdate,'127.0.0.1',?,?,?,?,?,NULL)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setString(2, serial);
            ps.setString(3, shopCode);
            ps.setString(4, staffCode);
            ps.setString(5, request);
            ps.setString(6, response);
            ps.setString(7, responseCode);

            result = ps.executeUpdate();
            logger.info("End insertActionLogPr serial " + serial + " isdn: " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionLogPr: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial)
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

    public Long getK4SNO(String imsi) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Long k4sno = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from k4sno_connect_kit where (to_number(?) between imsi_start and imsi_end)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, imsi);
            rs = ps.executeQuery();
            while (rs.next()) {
                k4sno = rs.getLong("k4sno");
                logger.info("k4sno " + k4sno + " imsi " + imsi);
                break;
            }
            logger.info("End getK4SNO: imsi" + imsi + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getK4SNO imsi").append(imsi).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString() + " imsi " + imsi);
            k4sno = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return k4sno;
        }
    }

    public String getTPLID(String isdn, boolean isSim4G) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String tplId = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from tplid_connect_kit where prefix = substr(?,0,4)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                if (isSim4G) {
                    tplId = rs.getString("tplid_4g");
                } else {
                    tplId = rs.getString("tplid");
                }

                logger.info("tplid " + tplId + " isdn " + isdn);
                break;
            }
            logger.info("End getTPLID: isdn" + isdn + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getTPLID isdn").append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString() + " isdn " + isdn);
            tplId = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return tplId;
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
            ps.setString(3, param);
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

    public boolean checkUserCodeOfBranch(String staffCode, Long shopId) {
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
            sql = "select * from staff where lower(staff_code) = lower(?) and status = 1 and shop_Id in (select shop_Id from Shop where shop_Path like '%" + shopId + "%')";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                transId = rs.getLong("staff_id");
                if (transId > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkUserCodeOfBranch staffCode " + staffCode
                    + " result " + result + " transId " + transId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkUserCodeOfBranch: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
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

    public int insertSubMbCabo(String isdn, String staffCode, String serial, String resultAddPrice, String schemaDb, Long shopId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(schemaDb);
            sql = "insert into sub_mb_cab (sub_cab_id, isdn, staff_code, import_date, result_ocs, serial,sub_state,shop_id) \n"
                    + "values (sub_cab_seq.nextval, ?,?,sysdate,?,?,1,?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setString(2, staffCode);
            ps.setString(3, resultAddPrice);
            ps.setString(4, serial);
            ps.setLong(5, shopId);
            result = ps.executeUpdate();
            logger.info("End insertSubMbCabo isdn " + isdn + " staffCode " + staffCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSubMbCabo: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkVipProductConnectKit(String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            sql = "select * from product.product_connect_kit where vip_product = 1 and status = 1 and upper(product_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long transId = rs.getLong("product_id");
                if (transId > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkVipProductConnectKit productCode " + productCode
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkVipProductConnectKit: ").
                    append(sql).append("\n")
                    .append(" productCode ")
                    .append(productCode)
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

    public int insertEwalletDeductRequest(long actionAuditId, String isdn, String staffCode, double money,
            String serial, String product, Date createTime, String client, String fundCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp1");
            sql = "INSERT INTO ewallet_deduct_request (EWALLET_DEDUCT_REQUEST_ID,ACTION_AUDIT_ID,ISDN,CREATE_STAFF,MONEY,SIM_SERIAL,"
                    + "PRODUCT_CODE,CREATE_TIME,CLIENT,FUND_CODE) \n"
                    + "VALUES(ewallet_deduct_request_seq.nextval,?,?,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            ps.setString(2, isdn);
            ps.setString(3, staffCode);
            ps.setDouble(4, money);
            ps.setString(5, serial);
            ps.setString(6, product);
            ps.setTimestamp(7, new Timestamp(createTime.getTime()));
            ps.setString(8, client);
            ps.setString(9, fundCode);
            result = ps.executeUpdate();
            logger.info("End insertEwalletDeductRequest msisdn " + isdn + " actionAuditId " + actionAuditId
                    + " money " + money + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertEwalletDeductRequest: ").
                    append(sql).append("\n")
                    .append(" msisdn ")
                    .append(isdn)
                    .append(" actionAuditId ")
                    .append(actionAuditId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertBonusConnectKit(String staffCode, String productCode, String isdn, long actionAudit, int channelType, String actionCode,
            String serial, Date createTime, int prepaidMonth, double bonusForCustomer, double bonusForPrepaid, Long subProfileId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp1");
            sql = "INSERT INTO bonus_connect_kit (action_audit_id,isdn,create_time,create_staff,productcode,serial,"
                    + "count_process,last_process,channel_type_id,action_code,prepaid_month,bonus_for_customer,bonus_for_prepaid, sub_profile_id) \n"
                    + "VALUES(?,?,?,?,?,?,0,null,?,?,?,?,?,?) ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAudit);
            ps.setString(2, isdn);
            ps.setTimestamp(3, new Timestamp(createTime.getTime()));
            ps.setString(4, staffCode);
            ps.setString(5, productCode);
            ps.setString(6, serial);
            ps.setInt(7, channelType);
            ps.setString(8, actionCode);
            if (prepaidMonth > 0) {
                ps.setInt(9, prepaidMonth);
            } else {
                ps.setString(9, "");
            }
            if (bonusForCustomer > 0) {
                ps.setDouble(10, bonusForCustomer);
            } else {
                ps.setString(10, "");
            }
            if (bonusForPrepaid > 0) {
                ps.setDouble(11, bonusForPrepaid);
            } else {
                ps.setString(11, "");
            }
            ps.setLong(12, subProfileId);
            result = ps.executeUpdate();
            logger.info("End insertBonusConnectKit isdn " + isdn + " actionAuditId " + actionAudit
                    + " staffCode " + staffCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertBonusConnectKit: ").
                    append(sql).append("\n")
                    .append(" msisdn ")
                    .append(isdn)
                    .append(" actionAuditId ")
                    .append(actionAudit)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkVipAgentHasDebit(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp1");
            sql = "select * from ewallet_deduct_daily where bill_cycle_date = trunc(sysdate -1) and result_code = '0'";
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            if (rs.next()) {
                Long transId = rs.getLong("ewallet_deduct_daily_id");
                if (transId > 0) {
                    result = true;
                }
            }
            logger.info("End checkVipAgentHasDebit isdn " + isdn
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkVipAgentHasDebit: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
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

    public int deleteSubProfile(long subProfileId, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
//            sql = "update sub_mb set status = ? where serial = ? and isdn = ?";
            sql = "delete cm_pre.sub_profile_info where sub_profile_id = ?";
            ps = connection.prepareStatement(sql);
//            ps.setLong(1, status);
            ps.setLong(1, subProfileId);
            result = ps.executeUpdate();
            logger.info("End deleteSubProfile isdn " + isdn + " subProfileId " + subProfileId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR deleteSubProfile: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" subProfileId ")
                    .append(subProfileId)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertKitElitePrepaid(String isdn, int addMonth, String createUser, String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "INSERT INTO kit_elite_prepaid (id,prepaid_month,remain_month,prepaid_type,status,"
                    + "create_time,remain_time,create_user,product_code,isdn) \n"
                    + "VALUES(kit_elite_prepaid_seq.nextval,?,?,1,1,sysdate,sysdate,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setInt(1, addMonth);
            ps.setInt(2, addMonth);
            ps.setString(3, createUser);
            ps.setString(4, productCode);
            ps.setString(5, isdn);
            result = ps.executeUpdate();
            logger.info("End insertKitElitePrepaid isdn " + isdn + " createUser " + createUser + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertKitElitePrepaid: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" createUser ")
                    .append(createUser)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public double getDiscountAmountForHandset(String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        double discountAmount = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from kit_prepaid_promotion where product_code = ? and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                discountAmount = rs.getDouble("discount_amount");
                break;
            }
            logger.info("End getDiscountAmountForHandset, productCode: " + productCode
                    + ", discountAmount" + discountAmount + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getDiscountAmountForHandset productCode").append(productCode).
                    append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString() + " productCode " + productCode);
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return discountAmount;
        }
    }

    public Long getStockModelId(String stockModelCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Long stockModelId = 0L;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from sm.stock_model where stock_model_code = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, stockModelCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                stockModelId = rs.getLong("stock_model_id");
                break;
            }
            logger.info("End getStockModelId stockModelCode: " + stockModelCode + ", stockModelId: " + stockModelId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getStockModelId stockModelCode: ").append(stockModelCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return stockModelId;
        }
    }

//    public Long getPricePolicyByStaffCode(String staffCode) {
//        ResultSet rs = null;
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder br = new StringBuilder();
//        Long pricePolicy = 1L;//pricePolicy Default...
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("dbsm");
//            String sql = "select * from shop where shop_id = (select shop_id from staff "
//                    + "where upper(staff_code) = upper(?) and status = 1)";
//            ps = connection.prepareStatement(sql);
//            ps.setString(1, staffCode);
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                pricePolicy = rs.getLong("price_policy");
//                break;
//            }
//            logger.info("End getPricePolicyByStaffCode staffCode: " + staffCode + " time: "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).append("\nERROR getPricePolicyByStaffCode staffCode: ").
//                    append(staffCode).append(" Message: ").
//                    append(ex.getMessage());
//            logger.error(br + ex.toString());
//        } finally {
//            closeResultSet(rs);
//            closeStatement(ps);
//            closeConnection(connection);
//            return pricePolicy;
//        }
//    }
//    public Price getPriceForSaleRetail(Long stockModelId, Long pricePolicy) {
//        long timeSt = System.currentTimeMillis();
//        ResultSet rs1 = null;
//        Connection connection = null;
//        Price priceObj = null;
//        String sqlMo = "select * from price where stock_model_id = ? and \n"
//                + "type = 1 and price_policy = ? \n"
//                + "and sta_date <= sysdate and ((end_date >= sysdate and end_date is not null) or end_date is null)\n"
//                + "and status = 1";
//        PreparedStatement psMo = null;
//        try {
//            connection = ConnectionPoolManager.getConnection("dbsm");
//            psMo = connection.prepareStatement(sqlMo);
//            if (QUERY_TIMEOUT > 0) {
//                psMo.setQueryTimeout(QUERY_TIMEOUT);
//            }
//            psMo.setLong(1, stockModelId);
//            psMo.setLong(2, pricePolicy);
//            rs1 = psMo.executeQuery();
//            while (rs1.next()) {
//                Long priceId = rs1.getLong("price_id");
//                Double price = rs1.getDouble("price");
//                Double vat = rs1.getDouble("vat");
//                String currency = rs1.getString("CURRENCY");
//
//                priceObj = new Price();
//                priceObj.setPriceId(priceId);
//                priceObj.setStockModelId(stockModelId);
//                priceObj.setVat(vat);
//                priceObj.setCurrency(currency);
//                priceObj.setPrice(price);
//                break;
//            }
//            logTimeDb("Time to getPriceForSaleRetail, stockModelId: " + stockModelId + " pricePolicy " + pricePolicy, timeSt);
//        } catch (Exception ex) {
//            logger.error("ERROR getPriceForSaleRetail, stockModelId: " + stockModelId + " pricePolicy " + pricePolicy);
//            logger.error(AppManager.logException(timeSt, ex));
//        } finally {
//            closeResultSet(rs1);
//            closeStatement(psMo);
//            closeConnection(connection);
//        }
//        return priceObj;
//    }
    public long getDiscountForHandset(Long stockModelId, String mainProduct, String shopId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        long discountAmount = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            String sql = "select * from product.product_connect_kit_handset where stock_model_id = ? and main_product like ? and (for_branch is null or for_branch like ?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, stockModelId);
            ps.setString(2, "%" + mainProduct + "%");
            ps.setString(3, "%" + shopId + "%");
            rs = ps.executeQuery();
            while (rs.next()) {
                discountAmount = rs.getLong("discount");
                break;
            }
            logger.info("End getDiscountForHandset stockModelId: " + stockModelId + ", discount: " + discountAmount + ", time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getDiscountForHandset stockModelId: ").
                    append(stockModelId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return discountAmount;
        }
    }

    public int expStockTotal(String staffCode, Long stockModelId, Long quantity) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update stock_total set quantity = quantity - ?, "
                    + " quantity_issue = quantity_issue - ?, modified_date = sysdate "
                    + " where owner_id = (select (case when staff_owner_id is not null then staff_owner_id else staff_id end) as staff_id from sm.staff where status = 1  and lower(staff_code) = lower(?)) and owner_type = 2 and stock_model_id = ? and state_id = 1 and quantity >= ? "
                    + " and quantity_issue >= ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, quantity);
            ps.setLong(2, quantity);
            ps.setString(3, staffCode);
            ps.setLong(4, stockModelId);
            ps.setLong(5, quantity);
            ps.setLong(6, quantity);
            result = ps.executeUpdate();
            logger.info("End expStockTotal staffCode " + staffCode + "stockModelId: " + stockModelId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR expStockTotal: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" stockModelId ")
                    .append(stockModelId)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateSeialExp(String staffCode, Long stockModelId, String serial) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update sm.stock_handset set status = 0 where stock_model_id = ? "
                    + " and owner_type = 2 and owner_id = (select (case when staff_owner_id is not null then staff_owner_id else staff_id end) as staff_id from sm.staff where status = 1  and lower(staff_code) = lower(?)) and  to_number(serial) = to_number(?)";//last time lock on mBCCS
            ps = connection.prepareStatement(sql);
            ps.setLong(1, stockModelId);
            ps.setString(2, staffCode);
            ps.setString(3, serial);
            result = ps.executeUpdate();
            logger.info("End updateSeialExp staffCode " + staffCode + "stockModelId: " + stockModelId + ", serial: " + serial
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSeialExp: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" stockModelId ")
                    .append(stockModelId)
                    .append(" serial ")
                    .append(serial)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
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

    public boolean checkProductConnectKit(String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        boolean result = false;
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            sql = "select * from product.product_connect_kit where upper(product_code) = upper(?) and is_product = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkProductConnectKit, productCode: " + productCode + ", result:" + result + ", time:"
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkProductConnectKit: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public Long getPriceProductConnectKit(String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        Long price = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            sql = "select * from product.product_connect_kit where upper(product_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                price = rs.getLong("money_fee");;
                break;
            }
            logger.info("End getPriceProductConnectKit productCode " + productCode + ", price: " + price + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getPriceProductConnectKit: ").
                    append(sql).append("\n")
                    .append(" productCode ")
                    .append(productCode);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return price;
        }
    }

    public Price getPriceIsdnToConnect(String isdn, String productCode) {
        ResultSet rs1 = null;
        Connection connection = null;
        Price priceObj = null;
        String sqlMo = "select * from price where 1 = 1 and price_id = (select price_id from Sale_Services_Detail \n"
                + "where stock_model_id = (select stock_model_id from stock_isdn_mobile where isdn = ?) and status= 1 \n"
                + "and sale_Services_Model_Id in (select sale_Services_Model_Id from Sale_Services_Model where sale_Services_Id = (select sale_Services_Id from sm.sale_services where status = 1 "
                + "and code = (Select m.sale_service_code from cm_pre.Mapping m, cm_pre.Reason r \n"
                + "where m.reason_Id = r.reason_Id and r.status = 1 and m.status = 1 \n"
                + "and m.channel is null and m.reason_Id = (select r.reason_Id from cm_pre.Reason r where r.status = 1 and r.type = '00'\n"
                + "and r.reason_Id in (select m.reason_Id from cm_pre.Mapping m where (m.product_Code = ? or m.product_Code is null) \n"
                + "and m.tel_Service_Id = 1 and m.status = 1 \n"
                + "and ( m.end_Date is null or m.end_Date >= trunc(sysdate))) ) and (m.product_Code = ? or m.product_Code is null))) and stock_Type_Id = 1 )) \n"
                + "and status = 1 and trunc(sta_date) <= trunc(sysdate) and (end_Date is null or trunc(end_Date) >= trunc(sysdate))";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setString(1, isdn);
            psMo.setString(2, productCode);
            psMo.setString(3, productCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Long priceId = rs1.getLong("price_id");
                Long stockModelId = rs1.getLong("stock_model_id");
                Double price = rs1.getDouble("price");
                priceObj = new Price();
                priceObj.setPriceId(priceId);
                priceObj.setStockModelId(stockModelId);
                priceObj.setPrice(price);
                logger.info("Result getPriceIsdnToConnect, productCode: " + productCode + " isdn " + isdn + " priceId " + priceId);
                break;
            }
            logger.info("getPriceIsdnToConnect isdn: " + isdn + " ---- productCode: " + productCode);
        } catch (Exception ex) {
            logger.error("ERROR getPriceIsdnToConnect. Exception: " + ex.getMessage());
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return priceObj;
    }

    public long getMoneyValueBasedOnMainPrice(String productCode, String mainPrice) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        long price = 0;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            sql = "select * from product.money_connect_kit where upper(product_code) = upper(?) and money_name = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            ps.setString(2, mainPrice);
            rs = ps.executeQuery();
            while (rs.next()) {
                price = rs.getLong("money_value");;
                break;
            }
            logger.info("End getMoneyValueBasedOnMainPrice productCode " + productCode + ", price: " + price + ", mainPrice: " + mainPrice + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getMoneyValueBasedOnMainPrice: ").
                    append(sql).append("\n")
                    .append(" productCode ")
                    .append(productCode);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return price;
        }
    }

    public Price getPriceByStockModelCode(String stockModelCode, String priceType, String pricePolicy) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        Price priceObj = null;
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
                Long priceId = rs.getLong("price_id");
                Double price = rs.getDouble("price");
                Double vat = rs.getDouble("vat");
                String currency = rs.getString("CURRENCY");

                priceObj = new Price();
                priceObj.setPriceId(priceId);
                priceObj.setVat(vat);
                priceObj.setCurrency(currency);
                priceObj.setPrice(price);
                break;
            }
            logger.info("End getPriceByStockModelCode stockModelCode " + stockModelCode + ", priceType: "
                    + priceType + " time "
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
            return priceObj;
        }
    }

    public String getSerialOfHandset(String stockModelCode, String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String handsetSerial = "";
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from sm.stock_handset where status = 1 "
                    + "and stock_model_id = (select stock_model_id from sm.stock_model where status = 1 and stock_model_code = ?) and owner_type = 2 \n"
                    + "and owner_id = (select (case when staff_owner_id is not null then staff_owner_id else staff_id end) as staff_id from sm.staff where status =1  "
                    + "and lower(staff_code) = lower(?)) and rownum < 2";
            ps = connection.prepareStatement(sql);
            ps.setString(1, stockModelCode);
            ps.setString(2, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                handsetSerial = rs.getString("serial");
                break;
            }
            logger.info("End getSerialOfHandset, stockModelCode: " + stockModelCode + ", staffCode: " + staffCode + ", serial: " + handsetSerial
                    + ", time: " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getSerialOfHandset: stockModelCode").append(stockModelCode).
                    append("staffCode: ").append(staffCode).
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return handsetSerial;
        }
    }

//    public String getShopIdStaffIdOfManager(String staffCode) {
//        ResultSet rs = null;
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder br = new StringBuilder();
//        StringBuilder result = null;
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("dbsm");
//            String sql = "select * from staff where staff_id = (select (case when staff_owner_id is not null then staff_owner_id else staff_id end) as staff_id from sm.staff where status =1 \n"
//                    + "and lower(staff_code) = lower(?))";
//            ps = connection.prepareStatement(sql);
//            ps.setString(1, staffCode);
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                Long shopId = rs.getLong("shop_id");
//                Long staffId = rs.getLong("staff_id");
//                result = new StringBuilder();
//                result.append(shopId).append("|").append(staffId);
//                break;
//            }
//            logger.info("End getIsdnWalletByStaffCode: " + staffCode + " time: "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).append("\nERROR getIsdnWalletByStaffCode ").append(staffCode).append(" Message: ").
//                    append(ex.getMessage());
//            logger.error(br + ex.toString());
//            result = null;
//        } finally {
//            closeResultSet(rs);
//            closeStatement(ps);
//            closeConnection(connection);
//            return result.toString();
//        }
//    }
    public String getMainProductConnectKit(String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String mainProduct = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
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

    public String getBasedConfigConnectKit(String staffCode, String columnName) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String basedConfig = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
//            sql = "select distinct main_product_name from product_connect_kit where main_product_name is not null";
//            LinhNBV 20200303: filter main product by staff_code
            sql = "SELECT listagg(" + columnName + ",'|') within group (order by order_by_main_product) as based_config\n"
                    + "from (select distinct " + columnName + ", order_by_main_product from main_product_connect_kit where (for_user is null or for_user like ?) \n"
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

    public long getRemainHandsetInStock(String stockModelCode, String ownerCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        long remainHandset = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select count(1) as handset_total from sm.stock_handset where stock_model_id = (select stock_model_id from sm.stock_model where stock_model_code = ? and status = 1)\n"
                    + "and owner_id = (select staff_id from sm.staff where status = 1 and lower(staff_code) = lower(?)) and owner_type = 2 and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, stockModelCode);
            ps.setString(2, ownerCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                remainHandset = rs.getLong("handset_total");
                break;
            }
            logger.info("End getRemainHandsetInStock stockModelCode: " + stockModelCode + ", ownerCode: " + ownerCode + ", remainHandset: " + remainHandset + ", time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getRemainHandsetInStock stockModelCode: ").
                    append(stockModelCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return remainHandset;
        }
    }
}
