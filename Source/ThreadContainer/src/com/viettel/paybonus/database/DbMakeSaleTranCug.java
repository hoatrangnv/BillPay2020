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
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Date;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbMakeSaleTranCug extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbMakeSaleTranCug.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbMakeSaleTranCug() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbcug";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbMakeSaleTranCug(String sessionName, Logger logger) throws SQLException, Exception {
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
        TransactionInfo record = new TransactionInfo();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("sub_count"));
            record.setMoney(rs.getDouble("MONEY"));
            record.setTransTime(rs.getTimestamp("trans_date"));
            record.setTranInitCount(rs.getLong("sub_count"));
            record.setTableName(rs.getString("table_name"));
            record.setPolicyId(rs.getString("policy_id"));
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
        int[] res = new int[0];
        return res;
    }

    public int updateTransLog(long saleTransId, String saleTransCode, String policyId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update vip_sub_process_log set make_sale_trans = 1, sale_trans_id = ?, sale_trans_code = ?"
                    + " where log_time >= trunc(sysdate) "
                    + " and (make_sale_trans <> 1 or make_sale_trans is null) and policy_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransId);
            ps.setString(2, saleTransCode);
            ps.setString(3, policyId);
            res = ps.executeUpdate();
            logger.info("End updateTransLog time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateTransLog: ");
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public int insertSaleTrans(Long saleTransId, Long shopId, Long staffId, Double discount, Double amountPay,
            Double amountNotTax, Double tax, Long reasonId, String saleTransCode, String policyId,
            String custName, String custTel, String custAddress) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO sale_trans (SALE_TRANS_ID,SALE_TRANS_DATE,SALE_TRANS_TYPE,STATUS,SHOP_ID,STAFF_ID,PAY_METHOD, "
                    + "DISCOUNT,AMOUNT_TAX,AMOUNT_NOT_TAX,VAT, "
                    + "TAX, "
                    + "REASON_ID,TELECOM_SERVICE_ID, "
                    + "SALE_TRANS_CODE, "
                    + "SYN_STATUS,DAILY_SYN_STATUS,CURRENCY, cust_name, tel_number, address) "
                    + "VALUES(?,sysdate,'4','2',?,?,'1',"
                    + "?,?,?,17, "
                    + "?,"
                    + "?,1, "
                    + "?, "
                    + "'0',0,'MT',?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransId);
            ps.setLong(2, shopId);
            ps.setLong(3, staffId);
            ps.setDouble(4, discount);
            ps.setDouble(5, amountPay);
            ps.setDouble(6, amountNotTax);
            ps.setDouble(7, tax);
            ps.setLong(8, reasonId);
            ps.setString(9, saleTransCode);
            ps.setString(10, custName);
            ps.setString(11, custTel);
            ps.setString(12, custAddress);
            res = ps.executeUpdate();
            logger.info("End insertSaleTrans policyId " + policyId + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTrans client " + policyId);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public int insertSaleTransDetail(Long saleTransId, Double quantity, Double discount, String policyId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO sale_Trans_detail (SALE_TRANS_DETAIL_ID,SALE_TRANS_ID,SALE_TRANS_DATE,STOCK_MODEL_ID,STATE_ID,PRICE_ID,QUANTITY,UPDATE_STOCK_TYPE, "
                    + "STOCK_TYPE_ID,STOCK_TYPE_CODE,STOCK_TYPE_NAME,STOCK_MODEL_CODE,STOCK_MODEL_NAME, "
                    + "ACCOUNTING_MODEL_CODE,ACCOUNTING_MODEL_NAME,CURRENCY, discount_amount) "
                    + "VALUES(sale_trans_detail_seq.nextval,?,sysdate,906210,1,503255,?,0, "
                    + "6,'SALE_CUG','SALE_CUG','SALE_CUG','SALE_CUG','SALE_CUG','SALE_CUG','MT', ? )";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransId);
            ps.setDouble(2, quantity);
            ps.setDouble(3, discount);
            res = ps.executeUpdate();
            logger.info("End insertSaleTransDetail policyId " + policyId + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTransDetail policyId " + policyId);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        int[] res = new int[0];
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                TransactionInfo sd = (TransactionInfo) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("make_trans_his_id", sd.getId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("client", sd.getClient(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("money", sd.getMoney(), Param.DataType.DOUBLE, Param.IN));
                paramList.add(new Param("trans_time", sd.getTransTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param("create_time", "sysdate", Param.DataType.CONST, Param.IN));
                paramList.add(new Param("tran_init_count", sd.getTranInitCount(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("tran_update_count", sd.getTranUpdateCount(), Param.DataType.INT, Param.IN));
                paramList.add(new Param("result_code", sd.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("description", sd.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("thread_name", sd.getTheadName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("cluster_name", sd.getClusterName(), Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            if (listParam.size() > 0) {
                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "make_trans_his");
                logTimeDb("Time to insertQueueOutput make_trans_his, batchid " + batchId + " total result: " + res.length, timeSt);
            } else {
                logTimeDb("List Record to insert Queue Output is empty, batchid " + batchId, timeSt);
            }
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueOutput batchid " + batchId, ex);
            try {
                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "MT");
                logTimeDb("Time to retry insertQueueOutput make_trans_his, batchid " + batchId + " total result: " + res.length, timeSt);
                return res;
            } catch (Exception ex1) {
                logger.error("ERROR retry insertQueueOutput make_trans_his, batchid " + batchId, ex1);
                logger.error(AppManager.logException(timeSt, ex));
                return null;
            }
        }
    }

    public long getSaleTransId(String client) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        long result = 0;
        String sqlMo = "select sale_trans_seq.nextval as saleTransid from dual";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = rs1.getLong("saleTransid");
                break;
            }
            logTimeDb("Time to getSaleTransId for client " + client + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSaleTransId client " + client);
            logger.error(AppManager.logException(timeSt, ex));
            result = 0;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        int[] res = new int[0];
        return res;
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
        StringBuilder sb = new StringBuilder();
        try {
            for (String sd : ids) {
                sb.append(":" + sd);
            }
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString() + " detail " + ex.getMessage());
        }
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
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
            ps.setString(1, msisdn.trim());
            ps.setString(2, message.trim());
            ps.setString(3, channel.trim());
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

    public VipSubInfo getVipSubInfo(String policyId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        VipSubInfo result = new VipSubInfo();
        String sqlMo = "select * from vip_sub_info where vip_sub_info_id = "
                + "(select vip_sub_info_id from vip_sub_detail where policy_id = ? and rownum < 2)";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, policyId);
            rs1 = psMo.executeQuery();
            if (rs1.next()) {
                result.setCustName(rs1.getString("cust_name"));
                result.setCustTel(rs1.getString("cust_tel"));
                result.setCustAddress(rs1.getString("cust_address"));
                result.setVipSubInfoId(rs1.getLong("vip_sub_info_id"));
            }
            logTimeDb("Time to getVipSubInfo for policyId " + policyId + " result: " + result.getCustName(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getVipSubInfo policyId " + policyId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return result;
        }
    }

    public String getUser(String policyId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String result = "";
        String sqlMo = "select upper(create_user) staff from vip_sub_detail where policy_id = ? and rownum < 2";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, policyId);
            rs1 = psMo.executeQuery();
            if (rs1.next()) {
                result = rs1.getString("staff");
            }
            logTimeDb("Time to getUser for policyId " + policyId + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getUser policyId " + policyId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return result;
        }
    }

    public StaffInfo getStaffInfo(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        StaffInfo result = new StaffInfo();
        String sqlMo = "select * from sm.staff where upper(staff_code) = ? and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, staffCode.trim().toUpperCase());
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result.setStaffId(rs1.getLong("staff_id"));
                result.setShopId(rs1.getLong("shop_id"));
                result.setStaffCode(rs1.getString("staff_code"));
                result.setTel(rs1.getString("isdn_wallet"));
                result.setChannelTypeId(rs1.getInt("channel_type_id"));
                break;
            }
            logTimeDb("Time to getStaffInfo for staffCode " + staffCode + " result isdn_wallet: " + result.getTel(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getVipSubInfo staffCode " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
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
            paramList.add(new Param("BONUS_TYPE", 3L, Param.DataType.LONG, Param.IN));
            PoolStore.PoolResult prs = poolStore.insertTable(paramList, "EWALLET_LOG");
            logTimeDb("Time to insertEwalletLog isdn " + log.getMobile(), timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getMobile());
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public boolean checkRenewPolicy(String policyId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select isdn from vip_sub_process_log where isdn in "
                + "(select isdn from vip_sub_detail where policy_id = ?) and make_sale_trans = 1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, policyId);
            rs1 = psMo.executeQuery();
            int countRs = 0;
            while (rs1.next()) {
                countRs++;
//                if at leat 2 same isdn that already add policy in history that can understand this is renew policy
                if (countRs > 2) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkRenewPolicy for policyId " + policyId + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkRenewPolicy policyId " + policyId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return result;
        }
    }
}
