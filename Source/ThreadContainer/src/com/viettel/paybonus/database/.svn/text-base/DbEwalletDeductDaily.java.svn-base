/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.EwalletLog;
import com.viettel.paybonus.obj.TopupLog;
import com.viettel.threadfw.manager.AppManager;
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
 * @author LinhNBV
 * @version 1.0
 * @since 22-11-2018
 */
public class DbEwalletDeductDaily extends DbProcessorAbstract {

    private String loggerLabel = DbEwalletDeductDaily.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbEwalletDeductDaily() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "appBccsGw";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbEwalletDeductDaily(String sessionName, Logger logger) throws SQLException, Exception {
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
        TopupLog record = new TopupLog();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("to_topup_log_id"));
            record.setTotalMoney(rs.getDouble("total_money"));
            record.setMoneyAfterDiscount(rs.getDouble("money_after_discount"));
            record.setTotalRecord(rs.getLong("total_record"));
            record.setClient(rs.getString("client"));
            record.setFundCode(rs.getString("fund_code"));
            record.setRequestId(rs.getString("request_id"));
            record.setFromTopupLogId(rs.getLong("from_topup_log_id"));
            record.setToTopupLogId(rs.getLong("to_topup_log_id"));
            record.setBillCycleDate(rs.getDate("bill_cycle_date"));
            record.setResultCode("0");
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

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        int[] res = new int[0];
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                TopupLog sd = (TopupLog) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("ewallet_deduct_daily_id", "ewallet_deduct_daily_seq.nextval", Param.DataType.CONST, Param.IN));
                paramList.add(new Param("process_date", "sysdate", Param.DataType.CONST, Param.IN));
                paramList.add(new Param("bill_cycle_date", sd.getBillCycleDate(), Param.DataType.DATE, Param.IN));
                paramList.add(new Param("parner_code", sd.getClient(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("total_record", sd.getTotalRecord(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("amount", sd.getTotalMoney(), Param.DataType.DOUBLE, Param.IN));
                paramList.add(new Param("amount_emola", sd.getMoneyAfterDiscount(), Param.DataType.DOUBLE, Param.IN));
                paramList.add(new Param("result_code", sd.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("description", sd.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("duration", sd.getDuration(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("from_topup_log_id", sd.getFromTopupLogId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("to_topup_log_id", sd.getToTopupLogId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("request_id", sd.getRequestId(), Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            if (listParam.size() > 0) {
                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "EWALLET_DEDUCT_DAILY");
                logTimeDb("Time to insertQueueOutput EWALLET_DEDUCT_DAILY, batchid " + batchId + " total result: " + res.length, timeSt);
            } else {
                logTimeDb("List Record to insert Queue Output is empty, batchid " + batchId, timeSt);
            }
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueOutput batchid " + batchId, ex);
            try {
                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "MT");
                logTimeDb("Time to retry insertQueueOutput EWALLET_DEDUCT_DAILY, batchid " + batchId + " total result: " + res.length, timeSt);
                return res;
            } catch (Exception ex1) {
                logger.error("ERROR retry insertQueueOutput EWALLET_DEDUCT_DAILY, batchid " + batchId, ex1);
                logger.error(AppManager.logException(timeSt, ex));
                return null;
            }
        }
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

    public int insertEwalletLog(EwalletLog log) {

        ParamList paramList = new ParamList();
        long timeSt = System.currentTimeMillis();
        try {
//            paramList.add(new Param("EWALLET_LOG_ID", log.geteWalletLogId(), Param.DataType.LONG, Param.IN));
            paramList.add(new Param("REQUEST_ID", log.getRequestId(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("CLIENT", log.getStaffCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("MOBILE", log.getIsdn(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("TRANS_ID", log.getTransId(), Param.DataType.STRING, Param.IN));
//            paramList.add(new Param("ACTION_CODE", log.getActionCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("AMOUNT", log.getAmountEmola(), Param.DataType.DOUBLE, Param.IN));
            paramList.add(new Param("FUNCTION_NAME", log.getFunctionName(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("URL", log.getUrl(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("USERNAME", log.getUserName(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("REQUEST", log.getRequest(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("RESPONSE", log.getRespone(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("DURATION", log.getDuration(), Param.DataType.LONG, Param.IN));
            paramList.add(new Param("ERROR_CODE", log.getErrorCode(), Param.DataType.STRING, Param.IN));
            paramList.add(new Param("DESCRIPTION", log.getDescription(), Param.DataType.STRING, Param.IN));
            PoolStore.PoolResult prs = poolStore.insertTable(paramList, "EWALLET_LOG");
            logTimeDb("Time to insertEwalletLog isdn " + log.getIsdn(), timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getIsdn());
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public boolean checkAlreadyProcessRecord(Date billCycle) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(
                    "select * from ewallet_deduct_daily where process_date > trunc(sysdate) and result_code = '0' and bill_cycle_date = ? ");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setDate(1, new java.sql.Date(billCycle.getTime()));
            rs = ps.executeQuery();
            while (rs.next()) {
                String channel = rs.getString("request_id");
                if (channel != null && channel.trim().length() > 0) {
                    result = true;
                }
                break;
            }
            logTimeDb("Time to checkAlreadyProcessRecord " + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAlreadyProcessRecord default return false");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }

    public int updateDebitInWsuser(String debitStatus, String debitValue, String wsUserId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection(this.dbNameCofig);
            sql = " update ws_user set debit_status = ?,debit_time = sysdate ,debit_value =? where username = ?  ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, debitStatus);
            ps.setString(2, debitValue);
            ps.setString(3, wsUserId);
            result = ps.executeUpdate();
            logger.info("End sendSms updateDebitInWsuser " + debitStatus + " debitValue " + debitValue + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateDebitInWsuser: ").
                    append(sql).append("\n")
                    .append(" debitStatus ")
                    .append(debitStatus)
                    .append(" debitValue ")
                    .append(debitValue)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int clearDebitInWsuser(String debitStatus, String wsUserId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection(this.dbNameCofig);
            sql = " update ws_user set debit_status = ?,debit_time = null ,debit_value =null where username = ?  ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, debitStatus);
            ps.setString(2, wsUserId);
            result = ps.executeUpdate();
            logger.info("End sendSms clearDebitInWsuser " + debitStatus + " wsUserId " + wsUserId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR clearDebitInWsuser: ").
                    append(sql).append("\n")
                    .append(" debitStatus ")
                    .append(debitStatus)
                    .append(" wsUserId ")
                    .append(wsUserId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getDebitInWsUser(String client) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String result = "0";
        try {
            connection = ConnectionPoolManager.getConnection(this.dbNameCofig);
            ps = connection.prepareStatement("select debit_value from ws_user where username = ?");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setString(1, client);
            rs = ps.executeQuery();
            if (rs.next()) {
                String channel = rs.getString("debit_value");
                if ((channel != null) && (channel.trim().length() > 0)) {
                    result = channel;
                }
            }
            logTimeDb("Time to getDebitInWsUser  result: " + result, timeSt);
        } catch (Exception ex) {
            this.logger.error("ERROR getDebitInWsUser default return 0");
            this.logger.error(AppManager.logException(timeSt, ex));
            return result;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }

    public String getAllDebit() {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String result = "0";
        try {
            connection = ConnectionPoolManager.getConnection(this.dbNameCofig);
            ps = connection.prepareStatement("select sum(total_money) as sum_total_money  from (select sum(money) total_money from topup_log a where  a.start_date > trunc(sysdate-90) and a.start_date < trunc(sysdate) and a.result_code = '0'   and not exists (select 1 from ewallet_deduct_daily where bill_cycle_date = trunc(a.start_date) and result_code = '0') group by a.client, trunc(a.start_date) )");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs = ps.executeQuery();
            if (rs.next()) {
                String channel = rs.getString("sum_total_money");
                if ((channel != null) && (channel.trim().length() > 0)) {
                    result = channel;
                }
            }
            logTimeDb("Time to getAllDebit  result: " + result, timeSt);
        } catch (Exception ex) {
            this.logger.error("ERROR getAllDebit default return 0");
            this.logger.error(AppManager.logException(timeSt, ex));
            return result;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }
}
