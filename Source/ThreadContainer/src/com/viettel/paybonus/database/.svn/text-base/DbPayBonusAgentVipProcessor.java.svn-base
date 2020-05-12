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
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
import java.sql.PreparedStatement;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbPayBonusAgentVipProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbPayBonusAgentVipProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDeleteMo = "update bonus_agent_vip set last_process_time = sysdate where bonus_agent_vip_id = ?";//"update cm_pre.sub_profile_info set bonus_status = 1 where sub_profile_id = ?";

    public DbPayBonusAgentVipProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
//        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        dbNameCofig = "dbapp1";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbPayBonusAgentVipProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        BonusAgentVip record = new BonusAgentVip();
        long timeSt = System.currentTimeMillis();
        try {
            record.setBonusAgentVipId(rs.getLong("bonus_agent_vip_id"));
            record.setStaffCode(rs.getString("staff_code"));
            record.setStaffOwnerId(rs.getLong("staff_owner_code"));
            record.setLastModifyTime(rs.getTimestamp("last_modify_time"));
            record.setTarget(rs.getLong("target"));
            record.setResultCode("0");
            record.setDescription("Start Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse BonusAgentVip");
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
                BonusAgentVip sd = (BonusAgentVip) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getBonusAgentVipId());
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
                BonusAgentVip sd = (BonusAgentVip) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("BONUS_AGENT_VIP_PROCESS_ID", sd.getBonusAgentVipProcessId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("STAFF_CODE", sd.getStaffCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("BONUS_VALUE", sd.getBonusValue(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("BONUS_TIME", "sysdate", Param.DataType.CONST, Param.IN));
//                paramList.add(new Param("BONUS_STATUS", sd.getBonusValue(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("BONUS_AGENT_VIP_ID", sd.getBonusAgentVipId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param(Bonus.RESULT_CODE, sd.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.DESCRIPTION, sd.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param(Bonus.DURATION, sd.getDuration(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("SOLD_LV1", sd.getSoldLv1(), Param.DataType.DOUBLE, Param.IN));
                paramList.add(new Param("SOLD_LV2", sd.getSoldLv2(), Param.DataType.DOUBLE, Param.IN));
                paramList.add(new Param("SOLD_LV3", sd.getSoldLv3(), Param.DataType.DOUBLE, Param.IN));
                paramList.add(new Param("OVER_TARGET_LV1_BR", sd.getOverTargetLv1Br(), Param.DataType.DOUBLE, Param.IN));
                paramList.add(new Param("OVER_TARGET_LV2_BR", sd.getOverTargetLv2Br(), Param.DataType.DOUBLE, Param.IN));
                paramList.add(new Param("OVER_TARGET_LV3_BR", sd.getOverTargetLv3Br(), Param.DataType.DOUBLE, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "BONUS_AGENT_VIP_PROCESS");
            logTimeDb("Time to insertQueueHis BONUS_AGENT_VIP_PROCESS, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis BONUS_AGENT_VIP_PROCESS batchid " + batchId, ex);
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
//        List<ParamList> listParam = new ArrayList<ParamList>();
//        String batchId = "";
//        int[] res = new int[0];
//        long timeSt = System.currentTimeMillis();
//        try {
//            String countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
//            String sms_code = ResourceBundle.getBundle("configPayBonus").getString("sms_code");
//            for (Record rc : listRecords) {
//                Bonus sd = (Bonus) rc;
//                if (sd.getMessage() == null || sd.getIsdn() == null || sd.getMessage().trim().length() <= 0
//                        || sd.getIsdn().trim().length() <= 0) {
//                    continue;
//                }
//                batchId = sd.getBatchId();
//                ParamList paramList = new ParamList();
//                paramList.add(new Param("MO_HIS_ID", sd.getActionAuditId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param("MT_ID", "MT_SEQ.NEXTVAL", Param.DataType.CONST, Param.IN));
//                paramList.add(new Param("MSISDN", countryCode + sd.getIsdn(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("MESSAGE", sd.getMessage(), Param.DataType.STRING, Param.IN));
//                paramList.add(new Param("CHANNEL", sms_code, Param.DataType.CONST, Param.IN));
//                listParam.add(paramList);
//            }
//            if (listParam.size() > 0) {
//                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "MT");
//                logTimeDb("Time to insertQueueOutput MT, batchid " + batchId + " total result: " + res.length, timeSt);
//            } else {
//                logTimeDb("List Record to insert Queue Output is empty, batchid " + batchId, timeSt);
//            }
//            return res;
//        } catch (Exception ex) {
//            logger.error("ERROR insertQueueOutput batchid " + batchId, ex);
//            logger.error(AppManager.logException(timeSt, ex));
//            return null;
//        }
        logger.info("insertQueueOutput, do nothing");
        int[] res = new int[0];
        return res;
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
                paramList.add(new Param("BONUS_AGENT_VIP_ID", Long.valueOf(sd), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("RESULT_CODE", "FW_99", Param.DataType.STRING, Param.IN));
                paramList.add(new Param("DESCRIPTION", "FW_Timeout", Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "BONUS_AGENT_VIP_PROCESS");
            logTimeDb("Time to processTimeoutRecord, insert BONUS_AGENT_VIP_PROCESS, total result: " + res.length, timeSt);
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
            try {
                int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "BONUS_AGENT_VIP_PROCESS");
                logTimeDb("Time to retry processTimeoutRecord, insert BONUS_AGENT_VIP_PROCESS, total result: " + res.length, timeSt);
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
            paramList.add(new Param("tel", "", Param.OperatorType.IS_NOT_NULL, Param.DataType.CONST, Param.IN));
            paramList.add(new Param("STAFF_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("STAFF_ID", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("tel", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("isdn_wallet", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("channel_type_id", null, Param.DataType.INT, Param.OUT));
            paramList.add(new Param("shop_id", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = poolStore.selectTable(paramList, "sm.staff");
            agent = new Agent();
            while (rs.next()) {
                String code = rs.getString("STAFF_CODE");
                String id = rs.getString("STAFF_ID");
                String isdn = rs.getString("tel");
                String isdnEmola = rs.getString("isdn_wallet");
                int channelTypeId = rs.getInt("channel_type_id");
                if (isdn != null && isdn.trim().length() > 0) {
                    agent.setStaffCode(code);
                    agent.setStaffId(Long.valueOf(id));
                    agent.setIsdnWallet(isdnEmola);
                    agent.setChannelTypeId(channelTypeId);
                    agent.setShopId(rs.getLong("shop_id"));
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

    public List<RateExchange> getListRateExchange(long productType) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String sqlMo = "select * from profile.BONUS_AGENT_VIP_RATE where product_type = ? and status = 1";
        PreparedStatement psMo = null;
        List<RateExchange> listRateExchange = new ArrayList<RateExchange>();
        try {
            connection = getConnection("dbapp1");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, productType);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String productCode = rs1.getString("product_code");
                double rate = rs1.getDouble("rate");
                long status = rs1.getLong("status");

                RateExchange rateEx = new RateExchange();
                rateEx.setProductCode(productCode);
                rateEx.setRate(rate);
                rateEx.setStatus(status);
                rateEx.setProductType(productType);

                listRateExchange.add(rateEx);

            }
            logTimeDb("Time to getListRateExchange: productType" + productType, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getListRateExchange ex: " + ex.getMessage());
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return listRateExchange;
    }

    public List<BonusAgentVip> getListStaffLevel2(String staffOwnerCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String sqlMo = "select * from bonus_agent_vip where staff_owner_code = ?";
        PreparedStatement psMo = null;
        List<BonusAgentVip> listStaff = new ArrayList<BonusAgentVip>();
        try {
            connection = getConnection("dbapp1");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, staffOwnerCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String staffCode = rs1.getString("staff_code");
                Long target = rs1.getLong("target");
                BonusAgentVip obj = new BonusAgentVip();
                obj.setStaffCode(staffCode);
                obj.setTarget(target);

                listStaff.add(obj);

            }
            logTimeDb("Time to getListStaffLevel2: staffOwnerCode " + staffOwnerCode + " result size " + listStaff.size(), timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getListStaffLevel2 staffOwnerCode " + staffOwnerCode + " " + ex.getMessage());
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return listStaff;
    }

    public List<String> getListStaffLevel3(String staffOwnerCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String sqlMo = "select * from sm.staff where staff_owner_id = (select staff_id from staff where staff_code = upper(?)) and status = 1";
        PreparedStatement psMo = null;
        List<String> listStaff = new ArrayList<String>();
        try {
            connection = getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, staffOwnerCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                String staffCode = rs1.getString("staff_code");
                listStaff.add(staffCode);
            }
            logTimeDb("Time to getListStaffLevel3: staffOwnerCode" + staffOwnerCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getListStaffLevel3 ex: " + ex.getMessage());
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return listStaff;
    }

    public long sumTargetStaffLevel2(String staffOwnerCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String sqlMo = "select sum(target) as total from bonus_agent_vip where staff_owner_code = ?";
        PreparedStatement psMo = null;
        long totalTarget = 0;
        try {
            connection = getConnection("dbapp1");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, staffOwnerCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                totalTarget = rs1.getLong("total");

            }
            logTimeDb("Time to sumTargetStaffLevel2: staffOwnerCode" + staffOwnerCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR sumTargetStaffLevel2 ex: " + ex.getMessage());
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return totalTarget;
    }

    public double getTotalSubMb(String productCode, String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        double totalSubMb = 0;
        String sqlMo = "select (select rate from profile.BONUS_AGENT_VIP_RATE where product_code = ?) * (select count(1) as total from cm_pre.sub_profile_info a, cm_pre.sub_mb b where a.sub_id = b.sub_id and a.cust_id = b.cust_id and \n"
                + "b.status = 2 and b.act_status = '00' and a.create_time > trunc(sysdate, 'mm') and a.sim_serial is not null and product_code = ?\n"
                + "and lower(a.create_staff) = lower(?)) as target from dual";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, productCode);
            psMo.setString(2, productCode);
            psMo.setString(3, staffCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                totalSubMb = rs1.getDouble("target");
            }
            logTimeDb("Time to getTotalSubMb productCode " + productCode + " staffCode " + staffCode + " result " + totalSubMb, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTotalSubMb productCode: " + productCode + " staffCode " + staffCode + ex.getMessage());
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return totalSubMb;
    }

    public double getTotalSubAdslLl(Long staffId, String productCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        double totalSubAdslLl = 0;
        String sqlMo = "select (select rate from profile.BONUS_AGENT_VIP_RATE@dbl_cus where product_code = ?) * (select count(1) as total from cm_pos.sub_adsl_ll where first_connect > trunc(sysdate, 'mm') "
                + "and status = 2 and act_status = 000 and staff_id = ? and product_code = ?) as target from dual";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("cm_pos");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, productCode);
            psMo.setLong(2, staffId);
            psMo.setString(3, productCode);

            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                totalSubAdslLl = rs1.getDouble("target");
            }
            logTimeDb("Time to getTotalSubAdslLl staffId " + staffId + " productCode " + productCode + totalSubAdslLl, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTotalSubAdslLl staffId " + staffId + " productCode " + productCode + ex.getMessage());
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return totalSubAdslLl;
    }

    public double getTotalSubCUG(String staffCode, String productCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        double totalSubAdslLl = 0;
        String sqlMo = "select (select rate from profile.BONUS_AGENT_VIP_RATE where product_code = ?) * (select count(1) as total from cug.vip_sub_process_log where log_time > trunc(sysdate,'mm') \n"
                + "and result_code = '0' and upper(create_staff) = upper(?) and policy_name = ?) as target from dual";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbapp1");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, productCode);
            psMo.setString(2, staffCode);
            psMo.setString(3, productCode);

            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                totalSubAdslLl = rs1.getDouble("target");
            }
            logTimeDb("Time to getTotalSubCUG staffCode " + staffCode + " productCode " + productCode + " result " + totalSubAdslLl, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getTotalSubCUG staffCode " + staffCode + " productCode " + productCode + ex.getMessage());
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return totalSubAdslLl;
    }

    public long sumBonusValueAgentVipInMonth(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String sqlMo = "select sum(bonus_agent_value) as total from bonus_agent_vip_process_detail where process_time > trunc(sysdate,'mm') "
                + "and result_code = '0' and upper(staff_code) = upper(?)";
        PreparedStatement psMo = null;
        long total = 0;
        try {
            connection = getConnection("dbapp1");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, staffCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                total = rs1.getLong("total");
            }
            logTimeDb("Time to sumBonusValueAgentVipInMonth: staffCode " + staffCode + " total lastest Bonus " + total, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR sumBonusValueAgentVipInMonth staffCode " + staffCode + " " + ex.getMessage());
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return total;
    }

    public long sumBonusValueBrDiriectorInMonth(String staffCode, String bonusObject) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String sqlMo = "select sum(bonus_value) as total from bonus_agent_vip_process where bonus_time > trunc(sysdate,'mm') "
                + "and result_code = ? and upper(staff_code) = upper(?)";
        PreparedStatement psMo = null;
        long total = 0;
        try {
            connection = getConnection("dbapp1");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
//            bonusObject: E01: Paybonus for AgentVip, E00: Paybonus for BR Director
            psMo.setString(1, bonusObject);
            psMo.setString(2, staffCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                total = rs1.getLong("total");
            }
            logTimeDb("Time to sumBonusValueInMonth: staffCode" + staffCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR sumBonusValueInMonth ex: " + ex.getMessage());
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return total;
    }

    public int insertBonusAgentVipProcessDetail(Long bonusAgentVipProcessDetailId, Long bonusAgentVipProcessId, String staffCode, String resultCode, String description, Long bonusAmount,
            double soldLv2, double overTargetLv2, double overTargetLv3Lv2, double soldLv3) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp1");
            sql = "insert into bonus_agent_vip_process_detail (id, bonus_agent_vip_process_id, staff_code, result_code, description, bonus_agent_value, process_time,"
                    + "sold_lv2,over_target_lv2,over_target_lv3_lv2,sold_lv3)\n"
                    + "values (?, ?,?,?,?,?,sysdate,?,?,?,?)";

            ps = connection.prepareStatement(sql);
            ps.setLong(1, bonusAgentVipProcessDetailId);
            ps.setLong(2, bonusAgentVipProcessId);
            ps.setString(3, staffCode);
            ps.setString(4, resultCode);
            ps.setString(5, description);
            ps.setLong(6, bonusAmount);
            ps.setDouble(7, soldLv2);
            ps.setDouble(8, overTargetLv2);
            ps.setDouble(9, overTargetLv3Lv2);
            ps.setDouble(10, soldLv3);
            result = ps.executeUpdate();
            logger.info("End insertBonusAgentVipProcessDetail staff " + staffCode + " resultCode " + resultCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertBonusAgentVipProcessDetail: ").
                    append(sql).append("\n")
                    .append(" staff ")
                    .append(staffCode)
                    .append(" resultCode ")
                    .append(resultCode)
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
            paramList.add(new Param("BONUS_TYPE", 5L, Param.DataType.LONG, Param.IN));
            PoolStore.PoolResult prs = poolStore.insertTable(paramList, "EWALLET_LOG");
            logTimeDb("Time to insertEwalletLog isdn " + log.getMobile(), timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getMobile());
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public Long getSequence(String sequenceName, String dbName) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Long sequenceValue = null;
        String sqlMo = "select " + sequenceName + ".nextval as sequence from dual";
        PreparedStatement psMo = null;
        try {
            connection = getConnection(dbName);
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
//    public int updateLastProcessTime(Long bonusAgentVipId) {
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder brBuilder = new StringBuilder();
//        String sql = "";
//        int result = 0;
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("dbapp1");
//            sql = "update bonus_agent_vip set last_process_time = sysdate where bonus_agent_vip_id = ?";
//            ps = connection.prepareStatement(sql);
//            ps.setLong(1, bonusAgentVipId);
//            result = ps.executeUpdate();
//            logger.info("End updateLastProcessTime bonusAgentVipId " + bonusAgentVipId + " result " + result + " time "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            brBuilder.setLength(0);
//            brBuilder.append(loggerLabel).append(new Date()).
//                    append("\nERROR updateLastProcessTime: ").
//                    append(sql).append("\n")
//                    .append(" bonusAgentVipId ")
//                    .append(bonusAgentVipId)
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
}
