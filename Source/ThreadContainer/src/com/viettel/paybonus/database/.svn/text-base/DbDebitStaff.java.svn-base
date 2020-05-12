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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
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
public class DbDebitStaff extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbDebitStaff.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;

    public DbDebitStaff() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbDebitStaff(String sessionName, Logger logger) throws SQLException, Exception {
        this.logger = logger;
        dbNameCofig = sessionName;
        poolStore = new PoolStore(dbNameCofig, logger);
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
        StaffInfo record = new StaffInfo();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("staff_id"));
            record.setStaffId(rs.getLong("staff_id"));
            record.setStaffCode(rs.getString("staff_code"));
            record.setShopId(rs.getLong("shop_id"));
            record.setLimitDay(rs.getString("limit_day"));
            record.setLimitMoney(rs.getString("limit_money"));
            record.setLimitCreateUser(rs.getString("limit_create_user"));
            record.setLimitApproveUser(rs.getString("limit_approve_user"));
            record.setLimitOverStatus(rs.getString("limit_over_status"));
            record.setLimitOverLastTime(rs.getTimestamp("limit_over_last_time"));
            record.setResultCode("0");
            record.setDescription("Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse MoRecord");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    public int lockRoleUser(String staffCode, String roleCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbVsa");
            sql = "update vsa_v3.role_user set is_active = 0 "
                    + "where user_id = (select user_id from users where status = 1 "
                    + "and user_name = ?) "
                    + "and role_id = (select role_id from vsa_v3.roles where status = 1 and upper(role_code) = ?) and is_active = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode.trim().toLowerCase());
            ps.setString(2, roleCode.trim().toUpperCase());
            result = ps.executeUpdate();
            logger.info("End lockRoleUser staffCode " + staffCode + " roleCode "
                    + roleCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR lockRoleUser: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" roleCode ")
                    .append(roleCode)
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

    public int openRoleUser(String staffCode, String roleCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbVsa");
            sql = "update vsa_v3.role_user set is_active = 1 "
                    + "where user_id = (select user_id from users where status = 1 "
                    + "and user_name = ?) "
                    + "and role_id = (select role_id from vsa_v3.roles where status = 1 and upper(role_code) = ?) and is_active = 0 ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode.trim().toLowerCase());
            ps.setString(2, roleCode.trim().toUpperCase());
            result = ps.executeUpdate();
            logger.info("End openRoleUser staffCode " + staffCode + " roleCode "
                    + roleCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR openRoleUser: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" roleCode ")
                    .append(roleCode)
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

    public int updateLockStaff(String staffCode, long staffId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update sm.staff set limit_over_status = 1, limit_over_last_time = sysdate "
                    + "where staff_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, staffId);
            result = ps.executeUpdate();
            logger.info("End updateLockStaff staffCode " + staffCode + " limit_over_status = 1, limit_over_last_time = sysdate "
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateLockStaff: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
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

    public int updateOpenStaff(String staffCode, long staffId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "update sm.staff set limit_over_status = 0"
                    + "where staff_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, staffId);
            result = ps.executeUpdate();
            logger.info("End updateOpenStaff staffCode " + staffCode + " limit_over_status = 1 "
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateOpenStaff: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
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

    public int saveActionLog(String staffCode, long staffId, String desc, String actionType) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "insert into sm.action_log(action_id, action_type, description, action_user, action_date, action_ip, object_id) "
                    + " values (action_log_seq.nextval, ?, ?, 'SYSTEM', sysdate, '127.0.0.1', ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, actionType);
            ps.setString(2, desc);
            ps.setLong(3, staffId);
            result = ps.executeUpdate();
            logger.info("End saveActionLog staffCode " + staffCode
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR saveActionLog: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
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

    public String getTelByStaffCode(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String tel = null;
        String sqlMo = " select cellphone from vsa_v3.users where user_name = ? and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
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

    public String getBranchId(String staffCode, long shopId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String parentShopId = "";
        String sqlMo = " select shop_path from sm.shop where shop_id = ? and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, shopId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String shopPath = rs1.getString("shop_path");
                String[] listShop = shopPath.split("\\_");
                if (listShop.length > 2) {
                    parentShopId = listShop[2];
                } else if (listShop.length > 1) {
                    parentShopId = listShop[1];
                }
            }
            logTimeDb("Time to getBranchId: " + staffCode + " BranchId " + parentShopId, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getBranchId " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return parentShopId;
        }
    }

    public ArrayList<String> getListBodFinanceOfBranch(String staffCode, long shopId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        ArrayList<String> listManager = new ArrayList<String>();
        String sqlMo = " select staff_code from sm.staff "
                + " where channel_type_id = 14 and type in (1,3) and status = 1 "
                + "and shop_id = ?";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbNameCofig);
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, shopId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String staff = rs1.getString("staff_code");
                listManager.add(staff);
            }
            logTimeDb("Time to getListBodFinanceOfBranch: " + staffCode + " BranchId " + shopId, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getListBodFinanceOfBranch " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return listManager;
        }
    }

    public boolean checkHaveSaleTransOverTime(String staffCode, long staffId, String limitDay) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        long transId = 0;
        try {
            connection = getConnection(dbNameCofig);
//            Start queue from 01/06/2018 because it's start time to manage sale limit
            sql = "select * from sm.sale_trans a where a.sale_trans_date > '17-jul-2018' and a.staff_id = ? and "
                    + " (a.clear_debit_status is null or a.clear_debit_status <> '1') "
                    + " and a.sale_trans_date < trunc(sysdate) - ? and a.status not in (4,6) and a.amount_tax > 0"
                    + " and not exists (select * from sm.sale_trans_order where sale_trans_id = a.sale_trans_id and is_check = 3)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, staffId);
            ps.setString(2, limitDay);
            rs = ps.executeQuery();
            while (rs.next()) {
                transId = rs.getLong("sale_trans_id");
                if (transId > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkHaveSaleTransOverTime staffCode " + staffCode + " limitDay "
                    + limitDay + " result " + result + " transId " + transId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkHaveSaleTransOverTime: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" limitDay ")
                    .append(limitDay)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkHaveEmolaTransOverTime(String staffCode, long staffId, String limitDay) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        long transId = 0;
        try {
            connection = getConnection(dbNameCofig);
//            Start queue from 01/06/2018 because it's start time to manage sale limit
            sql = "select * from sm.sale_emola_float a where a.sale_trans_date > '17-jul-2018' and a.staff_id = ? and "
                    + " (a.clear_debit_status is null or a.clear_debit_status <> '1') "
                    + " and a.sale_trans_date < trunc(sysdate) - ? and a.status not in (4,6) and a.amount_tax > 0";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, staffId);
            ps.setString(2, limitDay);
            rs = ps.executeQuery();
            while (rs.next()) {
                transId = rs.getLong("sale_trans_id");
                if (transId > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkHaveEmolaTransOverTime staffCode " + staffCode + " limitDay "
                    + limitDay + " result " + result + " transId " + transId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkHaveEmolaTransOverTime: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" limitDay ")
                    .append(limitDay)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkHaveDepositTransOverTime(String staffCode, long staffId, String limitDay) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        long transId = 0;
        try {
            connection = getConnection(dbNameCofig);
//            Start queue from 01/06/2018 because it's start time to manage sale limit
            sql = "select * from sm.deposit where create_date > '17-jul-2018' and staff_id = ? and "
                    + " (clear_debit_status is null or clear_debit_status <> '1') "
                    + " and create_date < trunc(sysdate) - ? and status in (0,1) and type = 1 and amount > 0";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, staffId);
            ps.setString(2, limitDay);
            rs = ps.executeQuery();
            while (rs.next()) {
                transId = rs.getLong("deposit_id");
                if (transId > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkHaveDepositTransOverTime staffCode " + staffCode + " limitDay "
                    + limitDay + " result " + result + " deposit_id " + transId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkHaveDepositTransOverTime: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" limitDay ")
                    .append(limitDay)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkHavePaymentTransOverTime(String staffCode, long staffId, String limitDay) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        long transId = 0;
        try {
            connection = getConnection("db_payment");
//            Start queue from 01/06/2018 because it's start time to manage sale limit
            sql = "select * from payment.payment_contract a where a.create_date > '17-jul-2018' and a.collection_staff_id = ? and "
                    + " (a.clear_debit_status is null or a.clear_debit_status <> '1') "
                    + " and a.create_date < trunc(sysdate) - ? and a.status = 1 and a.payment_type = '00' and a.payment_amount > 0"
                    + " and not exists (select * from payment.payment_bank_slip where payment_id = a.payment_id and status = 3)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, staffId);
            ps.setString(2, limitDay);
            rs = ps.executeQuery();
            while (rs.next()) {
                transId = rs.getLong("payment_id");
                if (transId > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkHavePaymentTransOverTime staffCode " + staffCode + " limitDay "
                    + limitDay + " result " + result + " transId " + transId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkHavePaymentTransOverTime: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" limitDay ")
                    .append(limitDay)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public double getMoneySaleTrans(String staffCode, long staffId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        double result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
//            Start queue from 01/06/2018 because it's start time to manage sale limit
            sql = "select sum(a.amount_tax) total_money from sm.sale_trans a "
                    + " where a.sale_trans_date > '17-jul-2018' and a.staff_id = ? "
                    + " and (a.clear_debit_status is null or a.clear_debit_status <> '1') and a.status not in (4,6)"
                    + " and not exists (select * from sm.sale_trans_order where sale_trans_id = a.sale_trans_id and is_check = 3)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, staffId);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getDouble("total_money");
            }
            logger.info("End getMoneySaleTrans staffCode " + staffCode
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getMoneySaleTrans: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public double getMoneyEmola(String staffCode, long staffId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        double result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
//            Start queue from 01/06/2018 because it's start time to manage sale limit
            sql = "select sum(a.amount_tax) total_money from sm.sale_emola_float a "
                    + " where a.sale_trans_date > '17-jul-2018' and a.staff_id = ? "
                    + " and (a.clear_debit_status is null or a.clear_debit_status <> '1') and a.status not in (4,6)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, staffId);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getDouble("total_money");
            }
            logger.info("End getMoneyEmola staffCode " + staffCode
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getMoneyEmola: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public double getMoneyDepositTrans(String staffCode, long staffId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        double result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
//            Start queue from 01/06/2018 because it's start time to manage sale limit
            sql = "select sum(amount) total_money from sm.deposit "
                    + " where create_date > '17-jul-2018' and staff_id = ? "
                    + " and (clear_debit_status is null or clear_debit_status <> '1') "
                    + " and status in (0,1) and type = 1";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, staffId);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getDouble("total_money");
            }
            logger.info("End getMoneyDepositTrans staffCode " + staffCode
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getMoneyDepositTrans: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public double getMoneyPaymentTrans(String staffCode, long staffId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        double result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("db_payment");
//            Start queue from 01/06/2018 because it's start time to manage sale limit
            sql = "select sum(payment_amount) total_money from payment.payment_contract a where a.create_date > '17-jul-2018' "
                    + " and a.collection_staff_id = ? and "
                    + " (a.clear_debit_status is null or a.clear_debit_status <> '1') "
                    + " and a.status = 1 and a.payment_type = '00'"
                    + " and not exists (select * from payment.payment_bank_slip where payment_id = a.payment_id and status = 3)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, staffId);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getDouble("total_money");
            }
            logger.info("End getMoneyPaymentTrans staffCode " + staffCode
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getMoneyPaymentTrans: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkAlreadyWarningInDay(String isdn, String msg) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("db_payment");
            sql = "select msisdn from mt_his where sent_time > trunc(sysdate) and msisdn = ? and message = ?";
            ps = connection.prepareStatement(sql);
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            ps.setString(1, isdn.trim());
            ps.setString(2, msg.trim());
            rs = ps.executeQuery();
            while (rs.next()) {
                String id = rs.getString("msisdn");
                if (id != null && id.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkAlreadyWarningInDay isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkAlreadyWarningInDay: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" message ")
                    .append(msg)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkAlreadyWarningInDayQueue(String isdn, String msg) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbNameCofig);
            sql = "select msisdn from mt where msisdn = ? and message = ?";
            ps = connection.prepareStatement(sql);
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            ps.setString(1, isdn.trim());
            ps.setString(2, msg.trim());
            rs = ps.executeQuery();
            while (rs.next()) {
                String id = rs.getString("msisdn");
                if (id != null && id.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkAlreadyWarningInDayQueue isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkAlreadyWarningInDayQueue: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" message ")
                    .append(msg)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        int[] res = new int[0];
        return res;
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
        int[] res = new int[0];
        return res;
    }

    public int sendSms(String msisdn, String message, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("db_payment");
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
}
