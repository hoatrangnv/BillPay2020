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
public class DbMakeSaleTranVipSub extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbMakeSaleTranVipSub.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbMakeSaleTranVipSub() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "appBccsGw";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbMakeSaleTranVipSub(String sessionName, Logger logger) throws SQLException, Exception {
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
            record.setId(rs.getLong("id"));
            record.setClient(rs.getString("CLIENT"));
            record.setMoney(rs.getDouble("MONEY"));
            record.setTransTime(rs.getTimestamp("trans_date"));
            record.setTranInitCount(rs.getLong("sub_count"));
            record.setTableName(rs.getString("table_name"));
            record.setVipSubInfoId(rs.getLong("vip_sub_info_id"));
            record.setResultCode("0");
            record.setDescription("Processing");
            record.setCreateUser(rs.getString("create_user"));
            record.setPaymentMethod(rs.getString("payment_method"));
            record.setDocId(rs.getLong("curr_doc_id"));
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

    public int updateTransLog(String client, String tableName, long saleTransId, String saleTransCode, long vipSubInfoId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            if (tableName != null && tableName.trim().toLowerCase().equals("vip_sub_process_log")) {
                connection = getConnection("dbvipsub");
                sql = "update vip_sub_process_log set make_sale_trans = 1, sale_trans_id = ?, sale_trans_code = ?"
                        + " where log_time >= trunc(sysdate) and result_code = '0' "
                        + " and (make_sale_trans <> 1 or make_sale_trans is null) and vip_sub_info_id = ?";
                ps = connection.prepareStatement(sql);
                ps.setLong(1, saleTransId);
                ps.setString(2, saleTransCode);
                ps.setLong(3, vipSubInfoId);
            } else {
                connection = getConnection(dbNameCofig);
                sql = "update topup_log set make_sale_trans = 1"
                        + " where start_date >= trunc(sysdate-1) and start_date < trunc(sysdate) and client = ? "
                        + " and (make_sale_trans <> 1 or make_sale_trans is null)";
                ps = connection.prepareStatement(sql);
                ps.setString(1, client);
            }
            res = ps.executeUpdate();
            logger.info("End updateTransLog time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateTransLog: ");
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public int insertSaleTrans(Long saleTransId, Long shopId, Long staffId, Double discount, Double amountPay,
            Double amountNotTax, Double tax, Long reasonId, String saleTransCode, String client,
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
            logger.info("End insertSaleTrans client " + client + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTrans client " + client);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public int insertSaleTransNoDebit(Long saleTransId, Long shopId, Long staffId, Double discount, Double amountPay,
            Double amountNotTax, Double tax, Long reasonId, String saleTransCode, String client,
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
                    + "SYN_STATUS,DAILY_SYN_STATUS,CURRENCY, cust_name, tel_number, address,"
                    + "clear_debit_status,clear_debit_time,clear_debit_user) "
                    + "VALUES(?,sysdate,'4','2',?,?,'1',"
                    + "?,?,?,17, "
                    + "?,"
                    + "?,1, "
                    + "?, "
                    + "'0',0,'MT',?,?,?"
                    + ",1,sysdate,'SYSTEM_AUTO_VIPSUB')";
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
            logger.info("End insertSaleTransNoDebit client " + client + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTransNoDebit client " + client);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public int insertSaleTransDetail(Long saleTransId, Double quantity, Double discount, String client, String tableName) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            if (tableName != null && tableName.trim().toLowerCase().equals("vip_sub_process_log")) {
                sql = "INSERT INTO sale_Trans_detail (SALE_TRANS_DETAIL_ID,SALE_TRANS_ID,SALE_TRANS_DATE,STOCK_MODEL_ID,STATE_ID,PRICE_ID,QUANTITY,UPDATE_STOCK_TYPE, "
                        + "STOCK_TYPE_ID,STOCK_TYPE_CODE,STOCK_TYPE_NAME,STOCK_MODEL_CODE,STOCK_MODEL_NAME, "
                        + "ACCOUNTING_MODEL_CODE,ACCOUNTING_MODEL_NAME,CURRENCY, discount_amount) "
                        + "VALUES(sale_trans_detail_seq.nextval,?,sysdate,906209,1,503255,?,0, "
                        + "6,'SALE_VIPSUB','SALE_VIPSUB','SALE_VIPSUB','SALE_VIPSUB','SALE_VIPSUB','SALE_VIPSUB','MT', ? )";
            } else {
                sql = "INSERT INTO sale_Trans_detail (SALE_TRANS_DETAIL_ID,SALE_TRANS_ID,SALE_TRANS_DATE,STOCK_MODEL_ID,STATE_ID,PRICE_ID,QUANTITY,UPDATE_STOCK_TYPE, "
                        + "STOCK_TYPE_ID,STOCK_TYPE_CODE,STOCK_TYPE_NAME,STOCK_MODEL_CODE,STOCK_MODEL_NAME, "
                        + "ACCOUNTING_MODEL_CODE,ACCOUNTING_MODEL_NAME,CURRENCY, discount_amount) "
                        + "VALUES(sale_trans_detail_seq.nextval,?,sysdate,906207,1,503255,?,0, "
                        + "6,'Topup','Topup','EMOLA','EMOLA','EMOLA','EMOLA','MT', ? )";
            }
//            sql = "INSERT INTO sale_Trans_detail (SALE_TRANS_DETAIL_ID,SALE_TRANS_ID,SALE_TRANS_DATE,STOCK_MODEL_ID,STATE_ID,PRICE_ID,QUANTITY,UPDATE_STOCK_TYPE, "
//                    + "STOCK_TYPE_ID,STOCK_TYPE_CODE,STOCK_TYPE_NAME,STOCK_MODEL_CODE,STOCK_MODEL_NAME, "
//                    + "ACCOUNTING_MODEL_CODE,ACCOUNTING_MODEL_NAME,CURRENCY, "
//                    + "PRICE_VAT,PRICE, "
//                    + "AMOUNT,DISCOUNT_AMOUNT,AMOUNT_BEFORE_TAX,AMOUNT_TAX,AMOUNT_AFTER_TAX) "
//                    + "VALUES(sale_trans_detail_seq.nextval,?,sysdate,906207,1,503255,1,0, "
//                    + "6,'Topup','Topup','EMOLA','EMOLA','EMOLA','EMOLA','MT', "
//                    + "17,?, "
//                    + "?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransId);
            ps.setDouble(2, quantity);
            ps.setDouble(3, discount);
//            ps.setDouble(2, amount);
//            ps.setDouble(3, amount);
//            ps.setDouble(4, discount);
//            ps.setDouble(5, amountBeforeTax);
//            ps.setDouble(6, amountTax);
//            ps.setDouble(7, amountAfterTax);
            res = ps.executeUpdate();
            logger.info("End insertSaleTransDetail client " + client + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTransDetail client " + client);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
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
                    + "values(sale_trans_order_seq.nextval,?,?,3,?,sysdate,?,5,4,'Clear by bankTransfer from vipsub system.')";
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

    public Long[] getStaffInfo(String staffCode) {
        long timeSt = System.currentTimeMillis();
        Long[] staff = new Long[2];
        ResultSet rs1 = null;
        Connection connection = null;
        long result = 0;
        String sqlMo = "select staff_id,shop_id from sm.staff where lower(staff_code) = ? and status =1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setString(1, staffCode.trim().toLowerCase());
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                staff[0] = rs1.getLong("staff_id");
                staff[1] = rs1.getLong("shop_id");
                break;
            }
            logTimeDb("Time to getShopOfStaff for staffCode " + staffCode + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getShopOfStaff staffCode " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
            staff[0] = -1L;
            staff[1] = -1L;
            return staff;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return staff;
    }

    public String[] getBankDocument(long vipSubDocId) {
        long timeSt = System.currentTimeMillis();
        String[] doc = new String[3];
        ResultSet rs1 = null;
        Connection connection = null;
        long result = 0;
        String sqlMo = "select * from vip_sub_doc where vip_sub_doc_id = ?";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbvipsub");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setLong(1, vipSubDocId);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                doc[0] = rs1.getString("bank");
                doc[1] = rs1.getString("bank_doc_no");
                doc[2] = rs1.getString("bank_amount");
                break;
            }
            logTimeDb("Time to getShopOfStaff for vipSubDocId " + vipSubDocId + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getShopOfStaff vipSubDocId " + vipSubDocId);
            logger.error(AppManager.logException(timeSt, ex));
            return doc;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return doc;
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

    public VipSubInfo getVipSubInfo(long vipSubInfoId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        VipSubInfo result = new VipSubInfo();
        String sqlMo = "select * from vip_sub_info where vip_sub_info_id = ?";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbvipsub");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, vipSubInfoId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result.setCustName(rs1.getString("cust_name"));
                result.setCustTel(rs1.getString("cust_tel"));
                result.setCustAddress(rs1.getString("cust_address"));
                break;
            }
            logTimeDb("Time to getVipSubInfo for vipSubInfoId " + vipSubInfoId + " result: " + result.getCustName(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getVipSubInfo vipSubInfoId " + vipSubInfoId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }
}
