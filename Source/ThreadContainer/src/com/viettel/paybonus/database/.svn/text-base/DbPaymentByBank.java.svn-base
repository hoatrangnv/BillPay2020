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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
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
public class DbPaymentByBank extends DbProcessorAbstract {

    private String loggerLabel = DbPaymentByBank.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDeleteMo = "delete bank_file_detail where bank_file_detail_id = ?";

    public DbPaymentByBank() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbPayByBank";
        ConnectionPoolManager.loadConfig("../etc/database.xml");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbPaymentByBank(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
        dbNameCofig = sessionName;
        ConnectionPoolManager.loadConfig("../etc/database.xml");
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
        BankFileDetail record = new BankFileDetail();
        long timeSt = System.currentTimeMillis();
        try {
            record.setBankFileDetailId(rs.getLong("bank_file_detail_id"));
            record.setBankFileInfoId(rs.getLong("bank_file_info_id"));
            record.setReference(rs.getString("reference"));
            record.setValuePay(rs.getLong("value_pay"));
            record.setValueCom(rs.getLong("value_com"));
            record.setFileTime(rs.getDate("file_time"));
            record.setImportTime(rs.getTimestamp("import_time"));
            record.setTransId(rs.getString("trans_id"));
            record.setTerminalId(rs.getString("terminal_id"));
            record.setTerminalLocation(rs.getString("terminal_location"));
            record.setResultCode("0");
            record.setDescription("start processing");
        } catch (Exception ex) {
            logger.error("ERROR parse BankFileDetail");
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
                BankFileDetail sd = (BankFileDetail) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getBankFileDetailId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue BankFileDetail batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue BankFileDetail, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                BankFileDetail sd = (BankFileDetail) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("bank_file_detail_id", sd.getBankFileDetailId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("bank_file_info_id", sd.getBankFileInfoId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("reference", sd.getReference(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("value_pay", sd.getValuePay(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("VALUE_COM", sd.getValueCom(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("FILE_TIME", sd.getFileTime(), Param.DataType.DATE, Param.IN));
                paramList.add(new Param("IMPORT_TIME", sd.getImportTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param("TRANS_ID", sd.getTransId(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("TERMINAL_ID", sd.getTerminalId(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("TERMINAL_LOCATION", sd.getTerminalLocation(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("ERR_CODE", sd.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("DESCRIPTION", sd.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("PROCESS_TIME", "sysdate", Param.DataType.CONST, Param.IN));
                paramList.add(new Param("NODE_NAME", sd.getNodeName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("CLUSTER_NAME", sd.getClusterName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("DURATION", (System.currentTimeMillis() - sd.getImportTime().getTime()),
                        Param.DataType.LONG, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "bank_file_his");
            logTimeDb("Time to insertQueueHis bank_file_his, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis bank_file_his batchid " + batchId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        }
    }

    public SubAdslLLPrepaid checkFtthPrepaid(long contractId, String account) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        PreparedStatement ps = null;
        SubAdslLLPrepaid result = null;
        String sqlGetUser = "select a.account, a.block_time, a.create_shop, a.create_time, a.create_user, a.EXPIRE_TIME, "
                + "a.SUB_ADSL_LL_PREPAID_ID, a.EXCESS_MONEY, "
                + "a.new_product_code, a.PREPAID_TYPE, a.SALE_TRANS_ID, b.sub_id, b.product_code, b.act_status, c.tel_fax "
                + " from sub_adsl_ll_prepaid a, sub_adsl_ll b, contract c where a.contract_id = ? and a.account = b.account "
                + " and a.contract_id = c.contract_id and b.status = 2 and c.status = 2 order by a.expire_time";
        Connection connection = null;
        try {
            logger.info("Start checkFtthPrepaid for account " + account + " contractId " + contractId);
            connection = getConnection("cm_pos");
            ps = connection.prepareStatement(sqlGetUser);
            if (account.startsWith("258")) {
                account = account.substring(3);
            }
            ps.setLong(1, contractId);
            rs = ps.executeQuery();
            if (rs.next()) {
                String tmp = rs.getString("product_code");
                if (tmp != null && tmp.trim().length() > 0) {
                    result = new SubAdslLLPrepaid();
                    result.setAccount(rs.getString("account"));
                    result.setContractId(contractId);
                    result.setBlockTime(rs.getTimestamp("block_time"));
                    result.setCreateShop(rs.getString("create_shop"));
                    result.setCreateTime(rs.getTimestamp("create_time"));
                    result.setCreateUser(rs.getString("create_user"));
                    result.setExpireTime(rs.getTimestamp("EXPIRE_TIME"));
                    result.setId(rs.getLong("SUB_ADSL_LL_PREPAID_ID"));
                    result.setNewProductCode(tmp);
                    result.setPrepaidType(rs.getLong("PREPAID_TYPE"));
                    result.setSaleTransId(rs.getLong("SALE_TRANS_ID"));
                    result.setSubId(rs.getLong("SUB_ID"));
                    result.setExcessMoney(rs.getLong("EXCESS_MONEY"));
                }
            }
            logTimeDb("Time to checkFtthPrepaid account " + account + " result " + result + " contract_id " + contractId, timeSt);
        } catch (Exception ex) {
            logger.error("Error checkFtthPrepaid account " + account + " contractId " + contractId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int saveExcessMoney(long contractId, String account, long newExcessMoney) {
        long timeSt = System.currentTimeMillis();
        PreparedStatement ps = null;
        int result = 0;
        String sqlGetUser = "update sub_adsl_ll_prepaid set EXCESS_MONEY = ? where contract_Id = ? and account = ?";
        Connection connection = null;
        try {
            logger.info("Start saveExcessMoney for account " + account + " contractId " + contractId + " newExcessMoney " + newExcessMoney);
            connection = getConnection("cm_pos");
            ps = connection.prepareStatement(sqlGetUser);
            if (account.startsWith("258")) {
                account = account.substring(3);
            }
            ps.setLong(1, newExcessMoney);
            ps.setLong(2, contractId);
            ps.setString(3, account);
            result = ps.executeUpdate();
            logTimeDb("Time to saveExcessMoney account " + account + " result " + result + " contract_id " + contractId
                    + " newExcessMoney " + newExcessMoney, timeSt);
        } catch (Exception ex) {
            logger.error("Error saveExcessMoney account " + account + " contractId " + contractId + " newExcessMoney " + newExcessMoney, ex);
            logger.error(AppManager.logException(timeSt, ex));
            result = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public long getContractFbbByRefer(String ref) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        PreparedStatement ps = null;
        long contract = 0;
        String sqlGetUser = "select a.contract_id from contract a where a.status = 2 and a.reference_id = ? and exists ("
                + "select isdn from sub_adsl_ll where status = 2 and contract_id = a.contract_id)";
        Connection connection = null;
        try {
            logger.info("Start getContractFbbByRefer for ref " + ref);
            connection = getConnection("cm_pos");
            ps = connection.prepareStatement(sqlGetUser);
            ps.setString(1, ref);
            rs = ps.executeQuery();
            if (rs.next()) {
                contract = rs.getLong("contract_id");
            }
            logTimeDb("Time to getContractFbbByRefer ref " + ref + " contract_id " + contract, timeSt);
        } catch (Exception ex) {
            logger.error("Error getContractFbbByRefer ref " + ref, ex);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return contract;
        }
    }

    public long getStaffByRefer(String ref) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        PreparedStatement ps = null;
        long contract = 0;
        String sqlGetUser = "select * from staff where status = 1 and reference_id = ?";
        Connection connection = null;
        try {
            logger.info("Start getStaffByRefer for ref " + ref);
            connection = getConnection("dbsm");
            ps = connection.prepareStatement(sqlGetUser);
            ps.setString(1, ref);
            rs = ps.executeQuery();
            if (rs.next()) {
                contract = rs.getLong("staff_id");
            }
            logTimeDb("Time to getStaffByRefer ref " + ref + " staff_id " + contract, timeSt);
        } catch (Exception ex) {
            logger.error("Error getStaffByRefer ref " + ref, ex);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return contract;
        }
    }

    public long[] getSaleTransByStaff(long staffId, long amountTax) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        PreparedStatement ps = null;
        long contract = 0;
        long clearStatus = 0;
        String sqlGetUser = "select * from sale_trans where sale_trans_date > sysdate - 60/(24*60) and staff_id = ? and round(amount_tax) = ? "
                + " and clear_debit_status is null ";
        Connection connection = null;
        long[] result = new long[2];
        try {
            logger.info("Start getSaleTransByStaff for staffId " + staffId + " amountTax " + amountTax);
            connection = getConnection("dbsm");
            ps = connection.prepareStatement(sqlGetUser);
            ps.setLong(1, staffId);
            ps.setLong(2, amountTax);
            rs = ps.executeQuery();
            if (rs.next()) {
                contract = rs.getLong("sale_trans_id");
                String clear = rs.getString("clear_debit_status");
                if (clear != null && "1".equals(clear.trim())) {
                    clearStatus = 1;
                } else {
                    clearStatus = 0;
                }
            }
            result[0] = contract;
            result[1] = clearStatus;
            logTimeDb("Time to getSaleTransByStaff staffId " + staffId + " amountTax " + amountTax + " saleTransId " + contract, timeSt);
        } catch (Exception ex) {
            logger.error("Error getSaleTransByStaff staffId " + staffId + " amountTax " + amountTax, ex);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int clearDebitBankPOS(long staffId, long saleTransId, long bankFileDetailId) {
        long timeSt = System.currentTimeMillis();
        PreparedStatement ps = null;
        int result = 0;
        String sqlGetUser = "update sale_trans set clear_debit_status = 1, clear_debit_time = sysdate, clear_debit_user = 'BANK_POS', clear_debit_request_id = ? "
                + " where sale_trans_date > sysdate - 50/(24*60) and sale_trans_id = ?";
        Connection connection = null;
        try {
            logger.info("Start clearDebitBankPOS for staffId " + staffId + " saleTransId " + saleTransId + " bankFileDetailId " + bankFileDetailId);
            connection = getConnection("dbsm");
            ps = connection.prepareStatement(sqlGetUser);
            ps.setLong(1, bankFileDetailId);
            ps.setLong(2, saleTransId);
            result = ps.executeUpdate();
            logTimeDb("Time to clearDebitBankPOS staffId " + staffId + " result " + result + " saleTransId " + saleTransId
                    + " bankFileDetailId " + bankFileDetailId, timeSt);
        } catch (Exception ex) {
            logger.error("Error clearDebitBankPOS staffId " + staffId + " saleTransId " + saleTransId + " bankFileDetailId " + bankFileDetailId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            result = 0;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        int[] result = new int[0];
        return result;
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
