/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.*;
import com.viettel.threadfw.database.DbProcessorAbstract;
import com.viettel.vas.util.ConnectionPoolManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.log4j.Logger;
import java.util.Date;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbBorrowCashRollbackProcessor extends DbProcessorAbstract {

    private String loggerLabel = DbBorrowCashRollbackProcessor.class.getSimpleName() + ": ";
    private String sqlDeleteMo = "update sm.emola_debit_log set ewallet_errcode = '98', ewallet_response = ? where emola_debit_log_id = ?";

    public DbBorrowCashRollbackProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
    }

    public DbBorrowCashRollbackProcessor(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
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
        EwalletDebitLog record = new EwalletDebitLog();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("emola_debit_log_id"));
            record.setBranch(rs.getString("branch"));
            record.setActionUser(rs.getString("action_user"));
            record.setActionType(rs.getLong("action_type"));
            record.setAmount(rs.getLong("amount"));
            record.setActionOtp(rs.getString("action_otp"));
            record.setSrcMoney(rs.getString("src_money"));
            record.setRequestId(rs.getString("ewallet_request_id"));
            record.setAgentMobile(rs.getString("agent_mobile"));
            record.setDebitUser(rs.getString("debit_user"));
            record.setRequest(rs.getString("ewallet_request"));
            record.setResultCode("0");
            record.setDescription("Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse MoRecord");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    public String getTelByStaffCode(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String tel = null;
        String sqlMo = " select cellphone from vsa_v3.users where user_name = ? and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbsm");
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

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        int[] res = new int[0];
        return res;
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
//            The first delete queue timeout
            deleteQueueTimeout(ids);
//            Save history
            for (String sd : ids) {
                sb.append(":" + sd);
            }
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
        }
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int[] deleteQueueTimeout(List<String> listId) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        StringBuilder sf = new StringBuilder();
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(sqlDeleteMo);
            sf.setLength(0);
            for (String id : listId) {
                ps.setString(1, "FW_Timeout");
                ps.setLong(2, Long.valueOf(id));
                ps.addBatch();
                sf.append(id);
                sf.append(", ");
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueueTimeout EMOLA_DEBIT_LOG listId " + sf.toString(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueueTimeout EMOLA_DEBIT_LOG, listId " + sf.toString(), timeStart);
        }
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        long timeStart = System.currentTimeMillis();
        PreparedStatement ps = null;
        Connection connection = null;
        String batchId = "";
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                EwalletDebitLog sd = (EwalletDebitLog) rc;
                batchId = sd.getBatchId();
                ps.setString(1, sd.getDescription());
                ps.setLong(2, sd.getId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR updateQueue EMOLA_DEBIT_LOG batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to updateQueue EMOLA_DEBIT_LOG, batchid " + batchId, timeStart);
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
            connection = getConnection("dbsm");
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
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
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

    public Long getEmolaDebitLimitStaff(String shopId, String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        Long emolaDebitLimitStaff = 0L;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select limit_money * (select nvl(debit_rate_limit,0)/100 from sm.emola_debit_config where branch = (select substr(shop_code,0,3) as branch from sm.shop\n"
                    + "where status = 1 and shop_id = to_number(?))) as debit_limit_staff from sm.staff where upper(staff_code) = upper(?) and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, shopId);
            ps.setString(2, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                emolaDebitLimitStaff = rs.getLong("debit_limit_staff");
                break;
            }
            logger.info("End getEmolaDebitLimitStaff staffCode " + staffCode + ", emolaDebitLimitStaff: " + emolaDebitLimitStaff + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getEmolaDebitLimitStaff: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return emolaDebitLimitStaff;
        }
    }

    public Long getDebitCurrentAmount(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        Long debitCurrentAmount = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select nvl(sum(debit_current_amount),0) as total_debit_staff from emola_debit_info where lower(debit_user) = lower(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                debitCurrentAmount = rs.getLong("total_debit_staff");
                break;
            }
            logger.info("End getDebitCurrentAmount staffCode " + staffCode + ", debitCurrentAmount: " + debitCurrentAmount + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getDebitCurrentAmount: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return debitCurrentAmount;
        }
    }

    public String getBodUserBranch(String shopId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String bodUser = "";
        StringBuilder br = new StringBuilder();
        String sql = "select * from emola_debit_config where branch = (select substr(shop_code,0,3) as branch "
                + "from sm.shop where status = 1 and shop_id = to_number(?))";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            ps = connection.prepareStatement(sql);
            ps.setString(1, shopId);
            rs = ps.executeQuery();
            while (rs.next()) {
                bodUser = rs.getString("bod_user");
                break;
            }
            logger.info("End getUserBodBranch shopId " + shopId + " bodUser " + bodUser + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getUserBodBranch: ").
                    append(sql).append("\n")
                    .append(" shopId ")
                    .append(shopId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return bodUser;
        }
    }

    public String getShopCode(String shopId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String shopCode = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.shop where status = 1 and shop_id = to_number(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, shopId);
            rs = ps.executeQuery();
            while (rs.next()) {
                shopCode = rs.getString("shop_code");;
                break;
            }
            logger.info("End getShopCode shopId " + shopId + ", shopCode: " + shopCode + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getShopCode: ").
                    append(sql).append("\n")
                    .append(" shopId ")
                    .append(shopId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return shopCode;
        }
    }

    public Long getDebitLimit(String userName) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        Long debitLimit = 0L;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.emola_debit_config where lower(bod_user) = lower(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, userName);
            rs = ps.executeQuery();
            while (rs.next()) {
                debitLimit = rs.getLong("debit_limit");
                break;
            }
            logger.info("End getDebitLimit userName " + userName + ", getDebitLimit: " + debitLimit + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getDebitLimit: ").
                    append(sql).append("\n")
                    .append(" userName ")
                    .append(userName);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return debitLimit;
        }
    }

    public Long getTotalEmolaDebit(String shopId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        Long totalDebit = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select nvl(sum(debit_current_amount),0) as total_emola_debit from emola_debit_info where branch = (select substr(shop_code,0,3) as branch "
                    + "from sm.shop where status = 1 and shop_id = to_number(?))";
            ps = connection.prepareStatement(sql);
            ps.setString(1, shopId);
            rs = ps.executeQuery();
            while (rs.next()) {
                totalDebit = rs.getLong("total_emola_debit");
                break;
            }
            logger.info("End getTotalEmolaDebit shopId " + shopId + ", totalDebit: " + totalDebit + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getTotalEmolaDebit: ").
                    append(sql).append("\n")
                    .append(" shopId ")
                    .append(shopId);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return totalDebit;
        }
    }

    public int checkUserBorrowMoney(String debitUser) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        int result = 0;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.emola_debit_info where lower(debit_user) = lower(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, debitUser);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = 1;
                break;
            }
            logger.info("End checkUserBorrowCash debitUser " + debitUser + ", result: " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkUserBorrowCash: ").
                    append(sql).append("\n")
                    .append(" debitUser ")
                    .append(debitUser);
            logger.error(br + ex.toString());
            result = -1;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateEmolaDebitInfo(String debitUser, Long cashAmount, Long floatAmount, String shopId, String agentId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update emola_debit_info set debit_current_amount = debit_current_amount + ?, "
                    + "last_update_time = sysdate, debit_cash_amount = debit_cash_amount + ?, "
                    + "debit_float_amount = debit_float_amount + ?,"
                    + "branch = (select substr(shop_code,0,3) as branch from sm.shop where status = 1 "
                    + "and shop_id = to_number(?)), agent_id = ? where lower(debit_user) = lower(?)";
            ps = connection.prepareStatement(sql);
            Long totalAmount = cashAmount + floatAmount;
            ps.setLong(1, totalAmount);
            ps.setLong(2, cashAmount);
            ps.setLong(3, floatAmount);
            ps.setString(4, shopId);
            ps.setString(5, agentId);
            ps.setString(6, debitUser);
            result = ps.executeUpdate();
            logger.info("End updateCashDebitInfo debitAmount " + totalAmount + " debitUser " + debitUser + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateCashDebitInfo: ").
                    append(sql).append("\n")
                    .append(" debitAmount ")
                    .append(cashAmount + floatAmount)
                    .append(" debitUser ")
                    .append(debitUser)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertEmolaDebitInfo(String shopId, String debitUser, Long cashAmount, Long floatAmount, String agentId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {

            connection = getConnection("dbsm");
            sql = "insert into emola_debit_info (emola_debit_info_id, branch, debit_user, debit_current_amount, last_update_time, debit_cash_amount, debit_float_amount, agent_id)\n"
                    + "values (emola_debit_info_seq.nextval,(select substr(shop_code,0,3) as branch from sm.shop where status = 1 "
                    + "and shop_id = to_number(?)),?,?,sysdate,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, shopId);
            ps.setString(2, debitUser);
            Long debitAmount = cashAmount + floatAmount;
            ps.setLong(3, debitAmount);
            ps.setLong(4, cashAmount);
            ps.setLong(5, floatAmount);
            ps.setString(6, agentId);

            result = ps.executeUpdate();
            logger.info("End insertEmolaDebitInfo debitUser: " + debitUser + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertEmolaDebitInfo: ").
                    append(sql).append("\n")
                    .append(" debitUser ")
                    .append(debitUser);
            logger.error(br + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertEmolaDebitLog(String shopId, String actionUser, int actionType, Long amount, String actionOtp, String approveOtp, String srcMoney,
            Long currentLimit, String ewalletRequest, String ewalletResponse, String ewalletErrorCode, String ewalletVoucher, String ewalletRequestId,
            String ewalletOrgRequestId, String agentId, String oldAgentId, String agentMobile, Long emolaDebitTransId, String feeWithdraw, int clearType,
            Long currentDebit, String debitUser, String bankDocument, String bankAmount, String bankName) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        logger.info("parameter: shopId: " + shopId + ", actionType: " + actionType + ", actionOtp: " + actionOtp + ", approveOtp: " + approveOtp + ",srcMoney: " + srcMoney + ","
                + "currentLimit: " + currentLimit + ", ewalletRequest: " + ewalletRequest + ", ewalletResponse: " + ewalletResponse
                + ", ewalletErr: " + ewalletErrorCode + ", voucher: " + ewalletVoucher + ", emolaDebitTransId: " + emolaDebitTransId
                + ", feeWithdraw: " + feeWithdraw + ", clearType: " + clearType + ", currentDebit: " + currentDebit + ", debitUser: " + debitUser
                + ", bankDocument: " + bankDocument + ", bankAmount: " + bankAmount + ", bankName: " + bankName);
        long startTime = System.currentTimeMillis();
        try {

            connection = getConnection("dbsm");
            sql = "insert into emola_debit_log (emola_debit_log_id, branch, action_user, action_type, amount, log_time, action_otp, approve_otp, src_money, current_limit, \n"
                    + "ewallet_request, ewallet_response, ewallet_errcode, ewallet_voucher, ewallet_request_id, ewallet_org_request_id, agent_id, old_agent_id, agent_mobile, "
                    + "emola_debit_trans_id, emola_fee_withdraw, clear_type, current_debit, debit_user, bank_document, bank_name, bank_amount)\n"
                    + "values (emola_debit_log_seq.nextval, (select substr(shop_code,0,3) as branch from sm.shop where status = 1 and shop_id = to_number(?)),\n"
                    + "?,?,?,sysdate,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, shopId);
            ps.setString(2, actionUser.toUpperCase());
            ps.setInt(3, actionType);
            ps.setLong(4, amount);
            ps.setString(5, actionOtp);
            ps.setString(6, approveOtp);
            ps.setString(7, srcMoney);
            ps.setLong(8, currentLimit);
            ps.setString(9, ewalletRequest);
            ps.setString(10, ewalletResponse);
            ps.setString(11, ewalletErrorCode);
            ps.setString(12, ewalletVoucher);
            ps.setString(13, ewalletRequestId);
            ps.setString(14, ewalletOrgRequestId);
            ps.setString(15, agentId);
            ps.setString(16, oldAgentId);
            ps.setString(17, agentMobile);
            if (emolaDebitTransId > 0) {
                ps.setLong(18, emolaDebitTransId);
            } else {
                ps.setString(18, "");
            }
            ps.setString(19, feeWithdraw);
            if (clearType >= 0) {
                ps.setInt(20, clearType);
            } else {
                ps.setString(20, "");
            }
            ps.setLong(21, currentDebit);
            ps.setString(22, debitUser);
            if (bankDocument != null && !bankDocument.isEmpty()) {
                ps.setString(23, bankDocument);
            } else {
                ps.setString(23, "");
            }
            if (bankName != null && !bankName.isEmpty()) {
                ps.setString(24, bankName);
            } else {
                ps.setString(24, "");
            }
            if (bankAmount != null && !bankAmount.isEmpty()) {
                ps.setString(25, bankAmount);
            } else {
                ps.setString(25, "");
            }


            result = ps.executeUpdate();
            logger.info("End insertEmolaDebitLog actionUser: " + actionUser + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertEmolaDebitLog: ").
                    append(sql).append("\n")
                    .append(" actionUser ")
                    .append(actionUser);
            logger.error(br + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateEmolaDebitOtp(String otp, String staffCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update emola_debit_otp set status = 0 where otp = ? and upper(staff_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, otp);
            ps.setString(2, staffCode);
            result = ps.executeUpdate();
            logger.info("End updateEmolaDebitOtp otp " + otp + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateEmolaDebitOtp: ").
                    append(sql).append("\n")
                    .append(" otp ")
                    .append(otp);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
}
