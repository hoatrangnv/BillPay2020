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
public class DbBonusConnectKit extends DbProcessorAbstract {

    private String loggerLabel = DbBonusConnectKit.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbBonusConnectKit() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbBonusConnectKit(String sessionName, Logger logger) throws SQLException, Exception {
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
        BonusConnectKit record = new BonusConnectKit();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("action_audit_id"));
            record.setActionAuditId(rs.getLong("action_audit_id"));
            record.setIsdn(rs.getString("isdn"));
            record.setCreateTime(rs.getTimestamp("create_time"));
            record.setCreateStaff(rs.getString("CREATE_STAFF"));
            record.setProductCode(rs.getString("productcode"));
            record.setSerial(rs.getString("serial"));
            record.setCountProcess(rs.getLong("count_process"));
            record.setLastProcess(rs.getTimestamp("last_process"));
            record.setChannelTypeId(rs.getInt("CHANNEL_TYPE_ID"));
            record.setActionCode(rs.getString("ACTION_CODE"));
            record.setPrepaidMonth(rs.getInt("prepaid_month"));
            record.setBonusForCustomer(rs.getLong("bonus_for_customer"));
            record.setBonusForPrepaid(rs.getLong("bonus_for_prepaid"));
            record.setSubProfileId(rs.getLong("sub_profile_id"));
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

    public int[] updateBonusConnectKit(List<BonusConnectKit> lstBn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int[] res = new int[0];
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "UPDATE bonus_connect_kit SET count_process = ?, last_process = sysdate WHERE action_audit_id = ? ";
            ps = connection.prepareStatement(sql);
            for (BonusConnectKit bn : lstBn) {
                ps.setLong(1, bn.getCountProcess());
                ps.setLong(2, bn.getActionAuditId());
                ps.addBatch();
            }
            res = ps.executeBatch();
            logger.info("End updateBonusConnectKit "
                    + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBonusConnectKit id ");
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

    public boolean checkAlreadyProcessRecord(long idRecord) {
        ParamList paramList = new ParamList();
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("action_audit_id", idRecord, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("date_process", "trunc(sysdate - 7)", Param.OperatorType.GREATER_EQUAL, Param.DataType.CONST, Param.IN));
            paramList.add(new Param("result_code", "0", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("action_audit_id", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "profile.bonus_connect_kit_his");
            int count = 0;
            while (rs.next()) {
                long id = rs.getLong("action_audit_id");
                if (id > 0) {
                    count++;
                    if (count >= 2) {
                        result = true;
                        break;
                    }
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

    public int insertBonusConnectKitHis(BonusConnectKit bn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "INSERT INTO bonus_connect_kit_his (ACTION_AUDIT_ID,ISDN,CREATE_STAFF,productcode,serial,"
                    + "result_code, ewallet_error_code, description, count_process, node_name, cluster_name, duration, "
                    + "channel_type_id, action_code,prepaid_month,bonus_for_customer,bonus_for_prepaid, sub_profile_id)"
                    + " VALUES(?,?,?,?,?,?,?,?, ?, ?, ?, ?, ?, ?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, bn.getActionAuditId());
            ps.setString(2, bn.getIsdn());
            ps.setString(3, bn.getCreateStaff());
            ps.setString(4, bn.getProductCode());
            ps.setString(5, bn.getSerial());
            ps.setString(6, bn.getResultCode());
            ps.setString(7, bn.geteWalletErrCode());
            ps.setString(8, bn.getDescription());
            ps.setLong(9, bn.getCountProcess());
            ps.setString(10, bn.getNodeName());
            ps.setString(11, bn.getClusterName());
            ps.setLong(12, bn.getDuration());
            ps.setInt(13, bn.getChannelTypeId());
            ps.setString(14, bn.getActionCode());
            if (bn.getPrepaidMonth() > 0) {
                ps.setInt(15, bn.getPrepaidMonth());
            } else {
                ps.setString(15, "");
            }
            if (bn.getBonusForCustomer() > 0) {
                ps.setLong(16, bn.getBonusForCustomer());
            } else {
                ps.setString(16, "");
            }
            if (bn.getBonusForPrepaid() > 0) {
                ps.setLong(17, bn.getBonusForPrepaid());
            } else {
                ps.setString(17, "");
            }
            if (bn.getSubProfileId() != null && bn.getSubProfileId() > 0) {
                ps.setLong(18, bn.getSubProfileId());
            } else {
                ps.setString(18, "");
            }
            result = ps.executeUpdate();
            logger.info("End insertBonusConnectKitHis id " + bn.getId() + " isdn " + bn.getIsdn() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertBonusConnectKitHis: ").
                    append(sql).append("\n")
                    .append(" id ")
                    .append(bn.getId())
                    .append(" isdn ")
                    .append(bn.getIsdn())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int deleteBonusConnectKit(BonusConnectKit bn) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        int res = 0;
        try {
            connection = getConnection(dbNameCofig);
            ps = connection.prepareStatement("delete bonus_connect_kit where sub_profile_id = ?");
            ps.setLong(1, bn.getSubProfileId());
            res = ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("ERROR deleteBonusConnectKit id " + bn.getId() + " isdn " + bn.getIsdn(), ex);
            logger.error(AppManager.logException(timeStart, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteBonusConnectKit, id " + bn.getId() + " isdn " + bn.getIsdn() + " result " + res, timeStart);
            return res;
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
            String sql = "select * from product_connect_kit where upper(product_code) = upper(?) and status = 1 and bonus_channel > 0";
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
                String bonusAgentOwner = rs.getString("BONUS_AGENT_OWNER");
                String bonusAgentStaff = rs.getString("BONUS_AGENT_STAFF");
                long moneyBrDirector = rs.getLong("money_br_director");
                comission = new Comission();
                comission.setBonusCenter(bonusCenter);
                comission.setBonusCtv(bonusCtv);
                comission.setBonusChannel(bonusChannel);
                comission.setBonusCtvCenter(bonusCtvCenter);
                comission.setBonusChannelCtv(bonusChannelCtv);
                comission.setBonusChannelCenter(bonusChanelCenter);
                comission.setBonusAgentOwner(bonusAgentOwner);
                comission.setBonusAgentStaff(bonusAgentStaff);
                comission.setMoneyBrDirector(moneyBrDirector);
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

    public int[] updateBonusConectKit(List<BonusConnectKit> lstBn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int[] res = new int[0];
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "UPDATE bonus_connect_kit SET count_process = ?, last_process = sysdate WHERE action_audit_id = ? ";
            ps = connection.prepareStatement(sql);
            for (BonusConnectKit bn : lstBn) {
                ps.setLong(1, bn.getCountProcess());
                ps.setLong(2, bn.getActionAuditId());
                ps.addBatch();
            }
            res = ps.executeBatch();
            logger.info("End updateBonusConectKit "
                    + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBonusConectKit id ");
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
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

    public String getShopCodeByShopId(String shopId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String shopCode = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.shop where shop_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, shopId);
            rs = ps.executeQuery();
            while (rs.next()) {
                shopCode = rs.getString("shop_code");
                break;
            }
            logger.info("End getShopCodeByShopId shopId " + shopId + " shopCode " + shopCode + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getShopPathByStaffCode: ").
                    append(sql).append("\n")
                    .append(" shopId ")
                    .append(shopId)
                    .append(" shopCode ")
                    .append(shopCode);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return shopCode;
        }
    }

    public boolean checkCorrectProfile(String isdn) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select a.isdn from cm_pre.sub_profile_info a, cm_pre.sub_mb b where a.isdn = b.isdn and b.status = 2 \n"
                    + "and a.cust_id = b.cust_id and a.isdn = ? and a.check_status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                String otpResult = rs.getString("isdn");
                if (otpResult != null && otpResult.length() > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkCorrectProfile isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkCorrectProfile: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getVasCode(Long subProfileId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String vasCode = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select vas_code from cm_pre.sub_profile_info where sub_profile_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, subProfileId);
            rs = ps.executeQuery();
            while (rs.next()) {
                vasCode = rs.getString("vas_code");
                break;
            }
            logger.info("End getVasCode subProfileId " + subProfileId + " vasCode " + vasCode + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getVasCode: ").
                    append(sql).append("\n")
                    .append(" subProfileId ")
                    .append(subProfileId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return vasCode;
        }
    }
}
