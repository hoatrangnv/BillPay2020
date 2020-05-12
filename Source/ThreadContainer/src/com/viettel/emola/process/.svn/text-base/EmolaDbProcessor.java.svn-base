/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.emola.process;

import com.viettel.paybonus.database.*;
import com.viettel.cluster.agent.integration.Record;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.*;
import com.viettel.threadfw.database.DbProcessorAbstract;
import static com.viettel.threadfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class EmolaDbProcessor extends DbProcessorAbstract {

    private String loggerLabel = DbOpenFlagRegisterInfo.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String dbNameCofig;

    public EmolaDbProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbEmolaMsSql";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public EmolaDbProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        EmolaPromotion record = new EmolaPromotion();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("InvoiceID"));
            record.setInvoiceId(rs.getLong("InvoiceID"));
            record.setCusId(rs.getLong("CustomerID"));
            record.setQuantity(rs.getInt("Quantity"));
            record.setCreateTime(rs.getTimestamp("CreatedDate"));
            record.setServiceId(rs.getInt("ServiceId"));
            record.setMobile(rs.getString("Mobile"));
            record.setCusName(rs.getString("CustomerName"));
            record.setResultCode("0");
            record.setDescription("Processing");
            record.setAgentWallet(rs.getString("AgentWallet"));
            record.setAgentName(rs.getString("AgentName"));
            record.setAgentChannelCode(rs.getString("AgentChannelCode"));
            record.setTransRef(rs.getString("TransRef"));
        } catch (Exception ex) {
            logger.error("ERROR parse MoRecord");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return record;
    }

    @Override
    public int[] deleteQueue(List<Record> listRecords) {
        return new int[0];
//        long timeStart = System.currentTimeMillis();
//        PreparedStatement ps = null;
//        Connection connection = null;
//        String batchId = "";
//        try {
//            dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbBocNameConfig");
//            connection = ConnectionPoolManager.getConnection(dbNameCofig);
//            ps = connection.prepareStatement(sqlDeleteMo);
//            for (Record rc : listRecords) {
//                LogRegisterInfo sd = (LogRegisterInfo) rc;
//                batchId = sd.getBatchId();
//                ps.setLong(1, sd.getLogRegisterInfoId());
//                ps.addBatch();
//            }
//            return ps.executeBatch();
//        } catch (Exception ex) {
//            logger.error("ERROR deleteQueue LOG_REGISTER_INFO batchid " + batchId, ex);
//            logger.error(AppManager.logException(timeStart, ex));
//            return null;
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            logTimeDb("Time to deleteQueue LOG_REGISTER_INFO, batchid " + batchId, timeStart);
//        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                EmolaPromotion sd = (EmolaPromotion) rc;
                batchId = sd.getBatchId();
                ParamList paramList = new ParamList();
                paramList.add(new Param("emola_promotion_id", sd.getInvoiceId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("customer_id", sd.getCusId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("invoice_id", sd.getInvoiceId(), Param.DataType.LONG, Param.IN));
                paramList.add(new Param("quantity", sd.getQuantity(), Param.DataType.INT, Param.IN));
                paramList.add(new Param("service_id", sd.getServiceId(), Param.DataType.INT, Param.IN));
                paramList.add(new Param("cus_name", sd.getCusName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("mobile", sd.getMobile(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("created_date", sd.getCreateTime(), Param.DataType.TIMESTAMP, Param.IN));                
                paramList.add(new Param("err_code", sd.getResultCode(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("description", sd.getDescription(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("cluster_name", sd.getClusterName(), Param.DataType.STRING, Param.IN));
                paramList.add(new Param("note_name", sd.getNodeName(), Param.DataType.STRING, Param.IN));
                listParam.add(paramList);
            }
            poolStore = new PoolStore("dbEmolaPromotion", logger);
            int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "emola_promotion_his");
            logTimeDb("Time to insertQueueHis emola_promotion_his, batchid " + batchId + " total result: " + res.length, timeSt);
            return res;
        } catch (Exception ex) {
            logger.error("ERROR retry insertQueueHis emola_promotion_his batchid " + batchId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        }
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        return new int[0];
//        List<ParamList> listParam = new ArrayList<ParamList>();
//        String batchId = "";
//        int[] res = new int[0];
//        long timeSt = System.currentTimeMillis();
//        try {
//            String countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
//            String sms_code = ResourceBundle.getBundle("configPayBonus").getString("sms_code");
//            for (Record rc : listRecords) {
//                LogRegisterInfo sd = (LogRegisterInfo) rc;
//                if (sd.getMessage() == null || sd.getIsdn() == null || sd.getMessage().trim().length() <= 0
//                        || sd.getIsdn().trim().length() <= 0) {
//                    continue;
//                }
//                batchId = sd.getBatchId();
//                ParamList paramList = new ParamList();
//                paramList.add(new Param("MO_HIS_ID", sd.getLogRegisterInfoId(), Param.DataType.LONG, Param.IN));
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
//            try {
//                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "MT");
//                logTimeDb("Time to retry insertQueueOutput MT, batchid " + batchId + " total result: " + res.length, timeSt);
//                return res;
//            } catch (Exception ex1) {
//                logger.error("ERROR retry insertQueueOutput MT, batchid " + batchId, ex1);
//                logger.error(AppManager.logException(timeSt, ex));
//                return null;
//            }
//        }
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
                sb.append(": ").append(sd);
            }
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString() + " " + ex.toString());
        }
    }

    public int[] deleteQueueTimeout(List<String> listId) {
        return new int[0];
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int insertEmolaPromotion(EmolaPromotion obj) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        int index = 1;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbEmolaPromotion");
            sql = "INSERT INTO emola_promotion_info (EMOLA_PROMOTION_ID,CUSTOMER_ID,INVOICE_ID,QUANTITY,CREATED_DATE,SERVICE_ID,MOBILE,CUS_NAME,PROMOTION_CODE,PROMOTION_TYPE,LOG_TIME,BTS_REG,AGENT_WALLET,AGENT_NAME,AGENT_CHANNEL_CODE,AGENT_PROM_TYPE) \n"
                    //+ "VALUES(?,?,?,?,?,?,?,?,?,?,sysdate,?)";
                    + "VALUES(emola_promotion_seq.nextval,?,?,?,?,?,?,?,?,?,sysdate,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            //ps.setLong(index++, obj.getId());
            ps.setLong(index++, obj.getCusId());
            ps.setLong(index++, obj.getInvoiceId());
            ps.setInt(index++, obj.getQuantity());
            ps.setTimestamp(index++, new Timestamp(obj.getCreateTime().getTime()));
            ps.setInt(index++, obj.getServiceId());
            ps.setString(index++, obj.getMobile());
            ps.setString(index++, obj.getCusName());
            ps.setString(index++, obj.getPromotionCode());
            ps.setInt(index++, obj.getPromotionType());
            ps.setString(index++, obj.getBtsReg());
            ps.setString(index++, obj.getAgentWallet());
            ps.setString(index++, obj.getAgentName());
            ps.setString(index++, obj.getAgentChannelCode());
            ps.setInt(index++, obj.getAgentPromType());
            result = ps.executeUpdate();
            logger.info("End insertEmolaPromotion isdn " + obj.getMobile() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR insertEmolaPromotion: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(obj.getMobile())
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
  
    public String getCell(String staffCode, String cellId, String lacId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement psGetCell = null;
        String cell = "";
        String sqlGetCell = "select cell from cell where ci = TO_NUMBER (util.convert_cell_id (?)) "
                + " and lac = TO_NUMBER (util.convert_lac (?)) and is_delete = 0 and ROWNUM < 2";
        try {
            connection = ConnectionPoolManager.getConnection("dbEmolaPromotion");
            psGetCell = connection.prepareStatement(sqlGetCell);
            if (QUERY_TIMEOUT > 0) {
                psGetCell.setQueryTimeout(QUERY_TIMEOUT);
            }
            psGetCell.setString(1, cellId);
            psGetCell.setString(2, lacId);
            rs = psGetCell.executeQuery();
            while (rs.next()) {
                String cellValue = rs.getString("cell");
                if (cellValue != null && cellValue.length() > 0) {
                    cell = cellValue;
                    break;
                }
            }
            logTimeDb("Time to getCell staffcode " + staffCode + " cellId " + cellId
                    + " lacId" + lacId + " result: " + cell, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getCell staffcode " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(psGetCell);
            closeConnection(connection);
        }
        return cell;
    }
    
    public String getBts(String staffCode, String cellId, String lacId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement psGetBts = null;
        String bts = "";
        String sqlGetBts = "select bts_code from bts where bts_id in (select bts_id from cell where ci = TO_NUMBER (util.convert_cell_id (?)) "
                + " and lac = TO_NUMBER (util.convert_lac (?)) and is_delete = 0 and ROWNUM < 2)";
        try {
            connection = ConnectionPoolManager.getConnection("dbEmolaPromotion");
            psGetBts = connection.prepareStatement(sqlGetBts);
            if (QUERY_TIMEOUT > 0) {
                psGetBts.setQueryTimeout(QUERY_TIMEOUT);
            }
            psGetBts.setString(1, cellId);
            psGetBts.setString(2, lacId);
            rs = psGetBts.executeQuery();
            while (rs.next()) {
                String btsValue = rs.getString("bts_code");
                if (btsValue != null && btsValue.length() > 0) {
                    bts = btsValue;
                    break;
                }
            }
            logTimeDb("Time to getCell staffcode " + staffCode + " cellId " + cellId
                    + " lacId" + lacId + " result: " + bts, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getCell staffcode " + staffCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(psGetBts);
            closeConnection(connection);
        }
        return bts;
    }

    public int sendSms(String msisdn, String message, String channel) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbSentSMS");
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

    public HashMap<String, Config> getEmolaConfig() {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs1 = null;
        String sqlMo = "SELECT * FROM emola_promotion_config where status = 1";
        PreparedStatement psMo = null;
        HashMap<String, Config> map = new HashMap<String, Config>();
        try {
            connection = ConnectionPoolManager.getConnection("dbEmolaPromotion");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Config conf = new Config();
                conf.setConfigId(rs1.getLong("CONFIG_ID"));
                conf.setValue(rs1.getString("VALUE"));
                conf.setCode(rs1.getString("CODE"));
                map.put(rs1.getString("CODE").toLowerCase(), conf);
                continue;
            }
        } catch (Exception ex) {
            logger.error("ERROR getEmolaConfig ");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        return map;
    }

    public boolean checkAlreadyHaveCode(String mobile, long invoiceId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();        
        try {
            connection = getConnection("dbEmolaPromotion");
            sql = "select * from emola_promotion_info where invoice_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, invoiceId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String code = rs.getString("promotion_code");
                if (code != null && !code.isEmpty()) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkAlreadyHaveCode mobile " + mobile + " invoiceId "
                    + invoiceId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkAlreadyHaveCode: ").
                    append(sql).append("\n")
                    .append(" mobile ")
                    .append(mobile)
                    .append(" invoiceId ")
                    .append(invoiceId)
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
        
    public boolean checkIsdnAlreadyHaveCode(String mobile) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();        
        try {
            connection = getConnection("dbEmolaPromotion");
            sql = "select * from emola_promotion_info where mobile = ? and service_id in (1, 87, 170)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, mobile);
            rs = ps.executeQuery();
            while (rs.next()) {
                String code = rs.getString("mobile");
                if (code != null && !code.isEmpty()) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkIsdnAlreadyHaveCode mobile " + mobile +" result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkIsdnAlreadyHaveCode: ").
                    append(sql).append("\n")
                    .append(" mobile ")
                    .append(mobile)
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
    
    public Staff getChannelInfo(String mobile) {
        Staff staff = null;
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String targetChannel = Config.getConfig(Config.targetChannelCode, logger);
        String staffCode = "";
        long startTime = System.currentTimeMillis();        
        try {
            connection = getConnection("dbSm");
            sql = "select * from sm.staff where isdn_wallet = ? and status = 1";
//            sql = "select * from sm.staff where isdn_wallet = ? and status = 1 and channel_type_id in(" + targetChannel + ")";
            ps = connection.prepareStatement(sql);
            ps.setString(1, mobile);
            rs = ps.executeQuery();
            while (rs.next()) {
                staff = new Staff();
                staffCode = rs.getString("staff_code");
                staff.setStaffCode(staffCode);
                staff.setChannelTypeId(rs.getString("channel_type_id"));
                if (staffCode != null && !staffCode.isEmpty()) {
                    break;
                }
            }
            logger.info("End getChannelInfo mobile " + mobile + "\n target channel" + targetChannel + " result " + staffCode + " time "
                    + (System.currentTimeMillis() - startTime));
            return staff;
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getChannelInfo: ").
                    append(sql).append("\n")
                    .append(" mobile ")
                    .append(mobile)
                    .append(" result ")
                    .append(staffCode);
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return staff;
        }
    }
    
    public Long getSequence(String sequenceName, String dbName) {
            ResultSet rs1 = null;
            Connection connection = null;
            Long sequenceValue = null;
            String sqlMo = "select " + sequenceName + ".nextval as sequence from dual";
            PreparedStatement psMo = null;
            try {
                    connection = getConnection(dbName);
                    psMo = connection.prepareStatement(sqlMo);
                    rs1 = psMo.executeQuery();
                    while (rs1.next()) {
                            sequenceValue = rs1.getLong("sequence");
                    }
                    logger.info("End getSequence sequenceName " + sequenceName);
            } catch (Exception ex) {
                    logger.error("ERROR getSequence " + sequenceName);
            } finally {
                    closeResultSet(rs1);
                    closeStatement(psMo);
                    closeConnection(connection);
            }
            return sequenceValue;
    }
    
    public Long getPromRankingOfTrans(int type) {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs = null;
        String sqlMo = "SELECT * FROM emola_promotion_config WHERE code = ? and status = 1";
        PreparedStatement psMo = null;
        long startTime = System.currentTimeMillis();
        Config conf = null;
        int result = 0;
        Long ranking = null;
        try {            
            connection = ConnectionPoolManager.getConnection("dbEmolaPromotion");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setString(1, Config.currentRankingProm + type);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs = psMo.executeQuery();
            while (rs.next()) {
                conf = new Config();
                conf.setConfigId(rs.getLong("CONFIG_ID"));
                conf.setValue(rs.getString("VALUE"));
                conf.setCode(rs.getString("CODE"));
                break;
            }
            
            if (conf == null) {
                logger.info("End getPromRankingOfTrans code " + Config.currentRankingProm + type + ": result not found. Time "
                    + (System.currentTimeMillis() - startTime));
                return ranking;
            }
            //Increase current ranking 
            ranking = Long.parseLong(conf.getValue());
            Long maxRanking = Config.getLongValue(Config.maxRankingProm + type, logger);
            if (maxRanking != null && ranking >= maxRanking) {
                logger.info("End getPromRankingOfTrans code " + Config.currentRankingProm + type + ": Ranking reached to max number. Time "
                    + (System.currentTimeMillis() - startTime));
                return null;
            }
            
            ranking++;
            psMo = connection.prepareStatement("UPDATE emola_promotion_config SET value = ? WHERE code = ? and status = 1");
            psMo.setString(1, ranking.toString());
            psMo.setString(2, Config.currentRankingProm + type);
            result = psMo.executeUpdate();

            if (result <= 0) {
                logger.info("End getPromRankingOfTrans code " + Config.currentRankingProm + type + ": Can not update ranking. Time "
                    + (System.currentTimeMillis() - startTime));
                return ranking;
            }
            
            logger.info("End getPromRankingOfTrans code " + Config.currentRankingProm + type + ": result " + ranking + ". Time "
                    + (System.currentTimeMillis() - startTime));

            return ranking;
            
        } catch (Exception ex) {
            logger.error("ERROR getPromRankingOfTrans code " + Config.currentRankingProm + type + " result " + ranking + " time "
                    + (System.currentTimeMillis() - startTime));
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
            closeResultSet(rs);
        }
        
        return ranking;
    }
    
    public String getPromotionCode(long ranking, String emolaPromPrefix) {
        String promotionCode = null;
        StringBuilder brBuilder = new StringBuilder();
        long startTime = System.currentTimeMillis();
        try {
            String prefixLuckyNumber = Config.getConfig(Config.prefixLuckyNumber, logger);
            String[] arrPromPrefix = prefixLuckyNumber.split(",");
            for (String pps : arrPromPrefix) {
                String[] detailProm = pps.split("\\:");
                
                if (emolaPromPrefix.equals(detailProm[0])) {
                    String promotionPrefix = detailProm[1];
                    promotionCode = String.format(promotionPrefix, ranking);
                    break;
                }
            }       
            
            logger.info("End getPromotionCode ranking " + ranking + " result " + promotionCode);
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR getPromotionCode: ").
                    append(ranking).append("\n")
                    .append(" result ")
                    .append(promotionCode);
            logger.error(brBuilder + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        }
        return promotionCode;
    }
    
    public int updateEmolaConfig(String code, String value) {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        String sqlMo = "UPDATE emola_promotion_config set VALUE = ? where CODE = ?";
        PreparedStatement psMo = null;
        long startTime = System.currentTimeMillis();
        int result = 0;
        try {
            connection = ConnectionPoolManager.getConnection("dbEmolaPromotion");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setString(1, value);
            psMo.setString(2, code);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            result = psMo.executeUpdate();
            logger.info("End updateEmolaConfig code " + code + " value " + value + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            logger.error("ERROR updateEmolaConfig " + code + " value " + value + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
        }
        
        return result;
    }
    
    public String getEmolaConfig(String code) {
        long timeSt = System.currentTimeMillis();
        Connection connection = null;
        ResultSet rs = null;
        String sqlMo = "SELECT * FROM emola_promotion_config where CODE = ? AND status = 1";
        PreparedStatement psMo = null;
        long startTime = System.currentTimeMillis();
        String value = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbEmolaPromotion");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setString(1, code);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            rs = psMo.executeQuery();
            while (rs.next()) {
                value = rs.getString("value");
                if (value != null && !value.isEmpty()) {
                    break;
                }
            }
            logger.info("End getEmolaConfig code " + code + " result " + value + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            logger.error("ERROR getEmolaConfig " + code + " result " + value + " time "
                    + (System.currentTimeMillis() - startTime));
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
            closeResultSet(rs);
        }
        
        return value;
    }
}
