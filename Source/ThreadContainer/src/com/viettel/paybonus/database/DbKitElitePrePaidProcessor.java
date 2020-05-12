/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.KitBatch;
import com.viettel.paybonus.obj.KitElitePrepaid;
import com.viettel.paybonus.obj.ProductConnectKit;
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
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
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
public class DbKitElitePrePaidProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbKitElitePrePaidProcessor.class.getSimpleName() + ": ";
    private String dbNameCofig;
    private PoolStore poolStore;
    private String sqlDeleteMo = "update kit_elite_prepaid set process_time = sysdate, count_process = nvl(count_process,0) + 1 where id = ?";

    public DbKitElitePrePaidProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "cm_pre";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbKitElitePrePaidProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        KitElitePrepaid record = new KitElitePrepaid();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("id"));
            record.setIsdn(rs.getString("isdn"));
            record.setPrepaidMonth(rs.getInt("prepaid_month"));
            record.setRemainMonth(rs.getInt("remain_month"));
            record.setPrepaidType(rs.getInt("prepaid_type"));
            record.setKitBatchId(rs.getLong("kit_batch_id"));
            record.setStatus(rs.getLong("status"));
            record.setCreateTime(rs.getTimestamp("create_time"));
            record.setRemainTime(rs.getTimestamp("remain_time"));
            record.setCreateUser(rs.getString("create_user"));
            record.setProductCode(rs.getString("product_code"));
            record.setResultCode("0");
            record.setDescription("Start Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse KitElitePrepaid");
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
                KitElitePrepaid sd = (KitElitePrepaid) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR updateQueue KitElitePrepaid batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to updateQueue KitElitePrepaid, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        List<ParamList> listParam = new ArrayList<ParamList>();
        String batchId = "";
        long timeSt = System.currentTimeMillis();
        try {
            for (Record rc : listRecords) {
                KitElitePrepaid sd = (KitElitePrepaid) rc;
                if (sd.getKitBatchId() != null && sd.getKitBatchId() > 0) {
                    logger.info("Add Money by Batch, so insertQueueHis using another method.");
                } else {
                    if ("0".equals(sd.getResultCode())) {
                        batchId = sd.getBatchId();
                        ParamList paramList = new ParamList();
                        paramList.add(new Param("ID", sd.getId(), Param.DataType.LONG, Param.IN));
                        paramList.add(new Param("ISDN", sd.getIsdn() != null ? sd.getIsdn() : "", Param.DataType.STRING, Param.IN));
                        paramList.add(new Param("PROCESS_TIME", "sysdate", Param.DataType.CONST, Param.IN));
                        paramList.add(new Param("RESULT_CODE", sd.getResultCode(), Param.DataType.STRING, Param.IN));
                        paramList.add(new Param("DESCRIPTION", sd.getDescription(), Param.DataType.STRING, Param.IN));
                        paramList.add(new Param("NODE_NAME", sd.getNodeName(), Param.DataType.STRING, Param.IN));
                        paramList.add(new Param("RESULT_TOPUP", sd.getResultTopup() != null ? sd.getResultTopup() : "", Param.DataType.STRING, Param.IN));
                        paramList.add(new Param("ACTION_AUDIT_ID", (sd.getActionAuditId() != null && sd.getActionAuditId() > 0) ? sd.getActionAuditId().toString() : "", Param.DataType.STRING, Param.IN));
                        paramList.add(new Param("KIT_BATCH_ID", (sd.getKitBatchId() != null && sd.getKitBatchId() > 0) ? sd.getKitBatchId().toString() : "", Param.DataType.STRING, Param.IN));
                        paramList.add(new Param("PRODUCT_CODE", sd.getProductCode(), Param.DataType.STRING, Param.IN));
                        paramList.add(new Param("DURATION", sd.getDuration(), Param.DataType.LONG, Param.IN));
                        if (sd.getMoId() != null && sd.getMoId() > 0) {
                            paramList.add(new Param("MO_ID", sd.getMoId(), Param.DataType.LONG, Param.IN));
                        }
                        listParam.add(paramList);
                    } else {
                        logger.info("No need insertQueueHis, because not yet add price plan buy more.");
                    }

                }
            }
            int[] res;
            if (listParam.size() > 0) {
                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "KIT_ELITE_PREPAID_HIS");
                logTimeDb("Time to insertQueueHis KitElitePrepaid, batchid " + batchId + " total result: " + res.length, timeSt);
            } else {
                res = new int[0];
            }
            return res;
        } catch (Exception ex) {
            logger.error("ERROR insertQueueHis KitElitePrepaid batchid " + batchId, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return null;
        }
    }

    @Override
    public int[] insertQueueOutput(List<Record> listRecords) {
        logger.info("KitBatchConnect No need to insertQueueOutput");
        int[] res = new int[0];
        return res;
    }

    @Override
    public int[] updateQueueInput(List<Record> listRecords) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateSqlMoParam(List<Record> lrc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
        StringBuilder sb = new StringBuilder();
        try {
            deleteQueueTimeout(ids);
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
        }
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
            logger.error("ERROR deleteQueueTimeout KitBatchConnect listId " + sf.toString(), ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueueTimeout KitBatchConnect, listId " + sf.toString(), timeStart);
        }
    }

    public String getProductCode(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String productCode = null;
        String sqlMo = "select * from sub_mb where isdn = ? and status = 2";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                productCode = rs1.getString("product_code");
            }
            if (productCode == null) {
                productCode = "";
                logger.info("productCode is null - isdn: " + isdn);
            }
            logTimeDb("Time to getProductCode: isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getProductCode isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return productCode;
        }
    }

    public boolean checkEliteProduct(String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from product.product_connect_kit where vip_product = 1 and status = 1 and upper(product_code) = upper(?) and pp_buy_more is not null";
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
            logger.info("End checkEliteProduct productCode " + productCode
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkEliteProduct: ").
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

    public Long getSequence(String sequenceName, String dbName) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Long sequenceValue = null;
        String sqlMo = "select " + sequenceName + ".nextval as sequence from dual";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection(dbName);
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

    public int insertActionAudit(Long actionAuditId, Long pkId, String des) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "INSERT INTO action_audit (ACTION_AUDIT_ID,ISSUE_DATETIME,ACTION_CODE,REASON_ID,SHOP_CODE,USER_NAME,"
                    + " PK_TYPE,PK_ID,IP,DESCRIPTION) "
                    + " VALUES(?,sysdate,'','','PROCESS','PROCESS', "
                    + "'3',?,'127.0.0.1',?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            ps.setLong(2, pkId);
            ps.setString(3, des);
            result = ps.executeUpdate();
            logger.info("End insertActionAudit actionAuditId " + actionAuditId
                    + " pkId " + pkId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionAudit: ").
                    append(sql).append("\n")
                    .append(" actionAuditId ")
                    .append(actionAuditId)
                    .append(" pkId ")
                    .append(pkId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertQueueHisForBatch(Long id, String isdn, String resultCode, String description, String nodeName,
            long duration, String requestId, String resultTopup, Long actionAuditId, Long kitBatchId, String productCode, String moneyProduct) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "INSERT INTO KIT_ELITE_PREPAID_HIS (ID,ISDN,PROCESS_TIME,RESULT_CODE,DESCRIPTION,NODE_NAME,DURATION,REQUEST_ID,"
                    + "RESULT_TOPUP,ACTION_AUDIT_ID,KIT_BATCH_ID,PRODUCT_CODE,MONEY_PRODUCT) "
                    + " VALUES(?,?,sysdate,?,?,?,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, id);
            ps.setString(2, isdn);
            ps.setString(3, resultCode);
            ps.setString(4, description);
            ps.setString(5, nodeName);
            ps.setLong(6, duration);
            ps.setString(7, requestId);
            ps.setString(8, resultTopup);
            if (actionAuditId > 0) {
                ps.setLong(9, actionAuditId);
            } else {
                ps.setString(9, "");
            }
            if (kitBatchId > 0) {
                ps.setLong(10, kitBatchId);
            } else {
                ps.setString(10, "");
            }
            ps.setString(11, productCode != null ? productCode : "");
            ps.setString(12, moneyProduct != null ? moneyProduct : "");
            result = ps.executeUpdate();
            logger.info("End insertQueueHisForBatch id " + id
                    + " isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionAudit: ").
                    append(sql).append("\n")
                    .append(" id ")
                    .append(id)
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public List<KitBatch> getListKitBatchDetail(Long kitBatchId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from kit_batch_detail where kit_batch_id = ? and result_code = '0' and (upper(product_code) in \n"
                    + " (select upper(product_code) from product.product_connect_kit where vip_product = 1 and status = 1) "
                    + "or product_code in (select vas_code from product.product_add_on where status = 1) or addon is not null)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String addOn = rs.getString("addon");
                int inputType = rs.getInt("input_type");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(kitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setCustId(rs.getLong("cust_id"));
                kitBatch.setProductAddOn(addOn);
                kitBatch.setInputType(inputType);
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchDetail: kitBatchId" + kitBatchId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getListKitBatchDetail ").
                    append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return lstKitBatch;
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

    public String getSubMbInfo(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String subInfo = "";
        String sqlMo = "select sub_id||'|'||product_code as sub_info from cm_pre.sub_mb where isdn = ? and status = 2";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                subInfo = rs1.getString("sub_info");
                break;
            }
            logTimeDb("Time to getSubMbInfo: isdn " + isdn + ", subInfo: " + subInfo, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSubMbInfo isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return subInfo;
        }
    }

    public ProductConnectKit getProductInfo(String isdn, String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        ProductConnectKit result = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from product.product_connect_kit where upper(product_code) = upper(?) and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode.trim().toUpperCase());
            rs = ps.executeQuery();
            if (rs.next()) {
                result = new ProductConnectKit();
                result.setMoneyFee(rs.getString("money_fee"));
                result.setPricePlan(rs.getString("price_plan_id"));
                result.setOfferId(rs.getLong("offer_id"));
                result.setPpBuyMore(rs.getString("pp_buy_more"));
                result.setSmsPrepaid(rs.getString("sms_prepaid"));
                result.setVasPricePlain(rs.getString("vas_price_plan"));
                result.setVasConnection(rs.getString("vas_connection"));
                result.setVasParam(rs.getString("vas_param"));
                result.setVasActionType(rs.getString("vas_action_type"));
                result.setVasChannel(rs.getString("vas_channel"));
            }
            logger.info("End getProductInfo: " + isdn + " productCode " + productCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getProductInfo ").append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertSubRenewElite(String isdn, String productCode, String inputType) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbElite");
            sql = "insert into sub_renew_elite(isdn,sta_datetime,product_code,id,input_type) "
                    + " values (?,sysdate,?,sub_renew_elite_seq.nextval,?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setString(2, productCode);
            ps.setString(3, inputType);
            result = ps.executeUpdate();
            logger.info("End insertSubRenewElite isdn: " + isdn + " productCode " + productCode + " id " + inputType + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSubRenewElite: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" productCode ")
                    .append(productCode)
                    .append(" inputType ")
                    .append(inputType)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateKitElitePrepaid(Long kitElitePrepaidId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "update kit_elite_prepaid set prepaid_month = (prepaid_month - 1), remain_month = (remain_month - 1) where id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitElitePrepaidId);
            result = ps.executeUpdate();
            logger.info("End updateKitElitePrepaid kitElitePrepaidId: " + kitElitePrepaidId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateKitElitePrepaid.");
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertMoHis(String dbName, String msisdn, Long subId, String productCode, String param, Long actionType, Long fee, Long moId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(dbName);
            sql = "INSERT INTO mo_his (msisdn, sub_id, product_code, sub_type, channel, command, param, "
                    + "receive_time, action_type, process_time, err_code, fee, node_name, cluster_name, channel_type, mo_his_id) \n"
                    + "VALUES(?,?,?,1,'155',?,?,sysdate,?,sysdate,'0',?,'KitElitePrepaid_Node1',\n"
                    + "'KitElitePrepaid','PROCESS_PREPAID',?)";
            ps = connection.prepareStatement(sql);
            if (!msisdn.startsWith("258")) {
                msisdn = "258" + msisdn;
            }
            ps.setString(1, msisdn);
            ps.setLong(2, subId);
            ps.setString(3, productCode);
            ps.setString(4, productCode);
            ps.setString(5, param);
            ps.setLong(6, actionType);
            ps.setLong(7, fee);
            ps.setLong(8, moId);
            result = ps.executeUpdate();
            logger.info("End insertMoHis msisdn " + msisdn
                    + " productCode " + productCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertMoHis: ").
                    append(sql).append("\n")
                    .append(" msisdn ")
                    .append(msisdn)
                    .append(" productCode ")
                    .append(productCode);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertKitElitePrepaid(String isdn, int addMonth, String createUser, String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "INSERT INTO kit_elite_prepaid (id,prepaid_month,remain_month,prepaid_type,status,"
                    + "create_time,remain_time,create_user,product_code,isdn,process_time) \n"
                    + "VALUES(kit_elite_prepaid_seq.nextval,?,?,1,1,sysdate,sysdate,?,?,?,sysdate)";
            ps = connection.prepareStatement(sql);
            ps.setInt(1, addMonth);
            ps.setInt(2, addMonth);
            ps.setString(3, createUser);
            ps.setString(4, productCode);
            ps.setString(5, isdn);
            result = ps.executeUpdate();
            logger.info("End insertKitElitePrepaid isdn " + isdn + " createUser " + createUser + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertKitElitePrepaid: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" createUser ")
                    .append(createUser)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateKitElitePrepaid(int prepaidMonth, String isdn, String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "update kit_elite_prepaid set prepaid_month = (prepaid_month + ?), remain_month = (remain_month + ?), process_time = sysdate where isdn = ? and status = 1 and upper(product_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setInt(1, prepaidMonth);
            ps.setInt(2, prepaidMonth);
            ps.setString(3, isdn);
            ps.setString(4, productCode);
            result = ps.executeUpdate();
            logger.info("End updateKitElitePrepaid isdn: " + isdn + ", prepaidMonth: " + prepaidMonth + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateKitElitePrepaid.");
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int disablePrepaidBatch(Long kitElitePrepaidId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "update kit_elite_prepaid set status = 0 where id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitElitePrepaidId);
            result = ps.executeUpdate();
            logger.info("End disablePrepaidBatch kitElitePrepaidId: " + kitElitePrepaidId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR disablePrepaidBatch.");
            logger.error(br + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkKitElitePrepaid(String isdn, String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from kit_elite_prepaid where status = 1 and isdn = ? and upper(product_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setString(2, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkKitElitePrepaid isdn " + isdn
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkKitElitePrepaid: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public long countSubPrepaid(String isdn, String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select count(1) as total from kit_elite_prepaid where status = 1 and isdn = ? and upper(product_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setString(2, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getLong("total");
                break;
            }
            logger.info("End countSubPrepaid isdn " + isdn
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR countSubPrepaid: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkBranchPromotionProduct(String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbElite");
            sql = "select * from vas_elite.branch_promotion_config where upper(promotion_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkBranchPromotionProduct productCode " + productCode
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkBranchPromotionProduct: ").
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

    public boolean checkVasProduct(String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from product.product_connect_kit where upper(product_code) = upper(?) and status = 1 and is_product = 0";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkVasProduct productCode " + productCode
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkVasProduct: ").
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

    public String getExpireTimeBranchPromotionProduct(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String expireTime = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbElite");
            sql = "select to_char(expire_time,'yyyymmddhh24miss') as expire_time from branch_promotion_sub where isdn = ?";
            ps = connection.prepareStatement(sql);
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                expireTime = rs.getString("expire_time");
                break;
            }
            logger.info("End getExpireTimeBranchPromotionProduct isdn " + isdn
                    + " expireTime " + expireTime + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getExpireTimeBranchPromotionProduct: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return expireTime;
        }
    }

    public int insertMO(String msisdn, String vasCode, String connName, String param, String actionType, String channel, Long moId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        if (!msisdn.startsWith("258")) {
            msisdn = "258" + msisdn;
        }
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection(connName);
            sql = "INSERT INTO mo (MO_ID,MSISDN,COMMAND,PARAM,RECEIVE_TIME,ACTION_TYPE,CHANNEL,CHANNEL_TYPE) \n"
                    + "VALUES(?,?,?,?,sysdate,?,?,'MBCCS')";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, moId);
            ps.setString(2, msisdn);
            ps.setString(3, vasCode);
            if (param != null && !param.isEmpty()) {
                ps.setString(4, param);
            } else {
                ps.setString(4, "");
            }
            ps.setString(5, actionType);
            ps.setString(6, channel);
            result = ps.executeUpdate();
            logger.info("End insertMO msisdn " + msisdn + " vasCode " + vasCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertMO: ").
                    append(sql).append("\n")
                    .append(" msisdn ")
                    .append(msisdn)
                    .append(" vasCode ")
                    .append(vasCode)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkPromotionSub(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        try {
            connection = getConnection("dbElite");
            ps = connection.prepareStatement(
                    //LinhNBV start modified on April 12 2018: Add condition to check profile of channel correct or not
                    "select * from branch_promotion_sub where isdn = ?");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                String channel = rs.getString("create_user");
                if (channel != null && channel.trim().length() > 0) {
                    result = true;
                }
                break;
            }
            logTimeDb("Time to checkPromotionSub " + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkPromotionSub default return false");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateBranchPromotionSub(String isdn, String expireTime) {
        long timeSt = System.currentTimeMillis();
        int rs1 = 0;
        Connection connection = null;
        String sqlMo = "update branch_promotion_sub set expire_time = to_date(?,'yyyyMMddHH24miss'), update_time = sysdate where isdn = ?";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("dbElite");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            if (!isdn.startsWith("258")) {
                isdn = "258" + isdn;
            }
            psMo.setString(1, expireTime);
            psMo.setString(2, isdn);
            rs1 = psMo.executeUpdate();
            logTimeDb("Time to updateBranchPromotionSub msisdn " + isdn
                    + " result: " + rs1, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR updateBranchPromotionSub msisdn " + isdn
                    + " message error " + ex.getLocalizedMessage());
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
            return rs1;
        }
    }

    public int insertBranchPromotionSub(Long actionAuditId, String isdn, String createUser, String productCode, String expireTime) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbElite");
            sql = "insert into branch_promotion_sub (action_audit_id, isdn, create_user, create_time, product_code, expire_time, update_time)\n"
                    + "values (?,?,?,sysdate,?,to_date(?,'yyyyMMddHH24miss'), sysdate)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            ps.setString(2, isdn);
            ps.setString(3, createUser);
            ps.setString(4, productCode);
            ps.setString(5, expireTime);
            result = ps.executeUpdate();
            logger.info("End insertBranchPromotionSub mo_id " + actionAuditId + " isdn: " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertBranchPromotionSub: ").
                    append(sql).append("\n")
                    .append(" mo_id ")
                    .append(actionAuditId)
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertKitElitePrepaidHis(KitElitePrepaid record) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into kit_elite_prepaid_his (id, process_time, result_code, description, node_name, kit_batch_id, "
                    + "product_code, mo_id, isdn)\n"
                    + "values (?,sysdate,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, record.getId());
            ps.setString(2, record.getResultCode());
            ps.setString(3, record.getDescription());
            ps.setString(4, record.getNodeName());
            ps.setLong(5, record.getKitBatchId());
            ps.setString(6, record.getProductCode());
            ps.setLong(7, record.getMoId());
            ps.setString(8, record.getIsdn());
            result = ps.executeUpdate();
            logger.info("End insertKitElitePrepaidHis mo_id " + record.getMoId() + " kitBatchId: " + record.getKitBatchId() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertKitElitePrepaidHis: ").
                    append(sql).append("\n")
                    .append(" mo_id ")
                    .append(record.getMoId())
                    .append(" kitBatchId ")
                    .append(record.getKitBatchId())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateCreateTimeKitElitePrepaid(String isdn, String productCode) {
        long timeSt = System.currentTimeMillis();
        int rs1 = 0;
        Connection connection = null;
        String sql = "update kit_elite_prepaid set create_time = create_time + 30 where isdn = ? and status = 1 and upper(product_code) = upper(?)";
        PreparedStatement psMo = null;
        try {
            connection = getConnection("cm_pre");
            psMo = connection.prepareStatement(sql);
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            psMo.setString(1, isdn);
            psMo.setString(2, productCode);
            rs1 = psMo.executeUpdate();
            logTimeDb("Time to updateCreateTimeKitElitePrepaid isdn " + isdn
                    + " result: " + rs1, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR updateCreateTimeKitElitePrepaid isdn " + isdn
                    + " message error " + ex.getLocalizedMessage());
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
            return rs1;
        }
    }
}
