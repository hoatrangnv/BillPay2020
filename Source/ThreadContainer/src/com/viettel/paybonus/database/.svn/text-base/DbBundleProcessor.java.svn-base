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
public class DbBundleProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbBundleProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbBundleProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbnetcombo";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbBundleProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        BundleHis record = new BundleHis();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("mo_his_id"));
            record.setMoHisId(rs.getLong("mo_his_id"));
            record.setOwnerSub(rs.getString("owner_sub"));
            record.setFtthAccount(rs.getString("ftth_account"));
            record.setStartTime(rs.getTimestamp("start_time"));
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

    public int cancelBundle(String ownerSub, String endReason) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update bundle_his set end_time = sysdate, end_reason = ? where owner_sub = ? and end_time is null";
            ps = connection.prepareStatement(sql);
            ps.setString(1, endReason);
            ps.setString(2, ownerSub);
            result = ps.executeUpdate();
            logger.info("End updateBundleHis ownerSub " + ownerSub + " endReason " + endReason + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBundleHis: ").
                    append(sql).append("\n")
                    .append(" ownerSub ")
                    .append(ownerSub)
                    .append(" endReason ")
                    .append(endReason)
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

    public ContractInfo getContractInfo(String account) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        ContractInfo result = null;
        String sqlMo = "select contract_id, tel_fax from contract where contract_id = (select contract_id from sub_adsl_ll "
                + " where account = ? and status = 2) and status = 2";
        PreparedStatement psMo = null;
        long contractId = 0;
        try {
            connection = getConnection("cm_pos");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, account);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                contractId = rs1.getLong("contract_id");
                if (contractId > 0) {
                    result = new ContractInfo();
                    result.setContractId(contractId);
                    result.setTelFax(rs1.getString("tel_fax"));
                    result.setAccount(account);
                    break;
                }
            }
            logTimeDb("Time to getContractInfo account " + account, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getContractInfo account " + account);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public boolean checkTelChanged(String owner, long contractId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select old_value from action_detail where issue_datetime > sysdate - 1 "
                + " and action_audit_id in (select action_audit_id from action_audit where issue_datetime > sysdate - 1 and pk_id = ?)"
                + " and col_name = 'TEL_FAX'";
        PreparedStatement psMo = null;
        String account = "";
        try {
            connection = getConnection("cm_pos");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, contractId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                account = rs1.getString("old_value");
                if (account != null && account.trim().length() > 0) {
                    if (!account.startsWith("258")) {
                        account = "258" + account.trim();
                    }
                    if (account.equals(owner)) {
                        result = true;
                        break;
                    }
                }
            }
            logTimeDb("Time to checkTelChanged owner " + owner + " contractId " + contractId
                    + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkTelChanged owner " + owner + " contractId " + contractId);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public boolean checkContractDebit(String owner, long contractId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        boolean result = false;
        String sqlMo = "select count(*) monthdebit from debit_contract where bill_cycle >= trunc(add_months(sysdate,-2),'mm') "
                + " and bill_cycle < trunc(sysdate,'mm') and (payment < sta_of_cycle) and sta_of_cycle > 100 and contract_id = ?";
        PreparedStatement psMo = null;
        long debitCount = 0;
        try {
            connection = getConnection("db_payment");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, contractId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                debitCount = rs1.getLong("monthdebit");
                if (debitCount >= 2) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkContractDebit owner " + owner + " contractId " + contractId
                    + " result: " + result + " debitmonth " + debitCount, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkContractDebit owner " + owner + " contractId " + contractId);
            logger.error(AppManager.logException(timeSt, ex));
            result = false;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public ArrayList<BundleHis> getListBundle(String owner) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        ArrayList<BundleHis> result = new ArrayList();
        String sqlMo = "select mo_his_id,owner_sub,member_sub,ftth_account,start_time  from bundle_his "
                + " where owner_sub = ? and end_time is null";
        PreparedStatement psMo = null;
        String account = "";
        try {
            connection = getConnection("dbnetcombo");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, owner);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                account = rs1.getString("owner_sub");
                if (account != null && account.trim().length() > 0) {
                    BundleHis temp = new BundleHis();
                    temp.setOwnerSub(account);
                    temp.setFtthAccount(rs1.getString("ftth_account"));
                    temp.setMoHisId(rs1.getLong("mo_his_id"));
                    if (rs1.getString("member_sub") != null) {
                        temp.setMemberSub(rs1.getString("member_sub"));
                    } else {
                        temp.setMemberSub(account);
                    }
                    result.add(temp);
                }
            }
            logTimeDb("Time to getListBundle owner " + owner + " , size of member in group " + result.size(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getListBundle owner " + owner);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public int[] insertBundleLog(List<BundleHis> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        int[] res = new int[0];
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                BundleHis sd = (BundleHis) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("mo_his_id", sd.getMoHisId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("owner_sub", sd.getOwnerSub(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("member_sub", sd.getMemberSub(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("ftth_account", sd.getFtthAccount(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("start_time", sd.getStartTime(), Param.DataType.TIMESTAMP, Param.IN));
                paramList.add(new Param("end_reason", sd.getEndReason(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("description", sd.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("log_time", "sysdate", Param.DataType.CONST, Param.IN));
                listParam.add(paramList);
            }
            if (listParam.size() > 0) {
                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "bundle_log");
                logTimeDb("Time to insertBundleLog, total result: " + res.length, timeSt);
            } else {
                logTimeDb("List Record to insert Queue Output is empty, batchid " + batchId, timeSt);
            }
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertBundleLog batchid " + batchId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        }
    }

    public ArrayList<String> getListBundleToRecover(long moHisId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        ArrayList<String> result = new ArrayList();
        String sqlMo = "select distinct member_sub  from bundle_log "
                + " where mo_his_id = ? and end_reason = 3 and RECOVER_STATUS is null";
        PreparedStatement psMo = null;
        String account = "";
        try {
            connection = getConnection("dbnetcombo");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, moHisId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                account = rs1.getString("member_sub");
                if (account != null && account.trim().length() > 0) {
                    result.add(account);
                }
            }
            logTimeDb("Time to getListBundleToRecover moHisId " + moHisId + " , size of member in group " + result.size(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getListBundleToRecover moHisId " + moHisId);
            logger.error(AppManager.logException(timeSt, ex));
            result = null;
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public int recoverBundle(String ownerSub) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update bundle_his set end_time = null, end_reason = null where owner_sub = ? and end_reason = 3";
            ps = connection.prepareStatement(sql);
            ps.setString(1, ownerSub);
            result = ps.executeUpdate();
            logger.info("End recoverBundle ownerSub " + ownerSub + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR recoverBundle: ").
                    append(sql).append("\n")
                    .append(" ownerSub ")
                    .append(ownerSub)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateBundleLog(String ownerSub, String member) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update bundle_log set recover_status = 1,RECOVER_TIME = sysdate where owner_sub = ? "
                    + " and member_sub = ? and end_reason = 3 and RECOVER_STATUS is null";
            ps = connection.prepareStatement(sql);
            ps.setString(1, ownerSub);
            ps.setString(2, member);
            result = ps.executeUpdate();
            logger.info("End updateBundleLog ownerSub " + ownerSub + " member " + member + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateBundleLog: ").
                    append(sql).append("\n")
                    .append(" ownerSub ")
                    .append(ownerSub)
                    .append(" member ")
                    .append(member)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
}
