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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbKitChangeToStudentProcessor extends DbProcessorAbstract {

    private String loggerLabel = DbKitChangeToStudentProcessor.class.getSimpleName() + ": ";
    private PoolStore poolStore;
    private String sqlDeleteMo = "update cm_pre.request_change_product set status = 1, date_process = sysdate, result_code = ?, description = ? where sub_profile_id = ? and request_id = ?";

    public DbKitChangeToStudentProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        poolStore = new PoolStore("cm_pre", logger);
    }

    public DbKitChangeToStudentProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        Bonus record = new Bonus();
        long timeSt = System.currentTimeMillis();
        try {
            record.setAgentId(rs.getLong("request_id"));
            record.setId(rs.getLong("sub_profile_id"));
            record.setActionAuditId(rs.getLong("action_audit_id"));
            record.setIsdnCustomer(rs.getString("isdn"));
            record.setProductCode(rs.getString("old_product"));
            record.setUserName(rs.getString("create_staff"));
            record.setPkId(rs.getLong("sub_id"));
            record.setCustId(rs.getLong("cust_id"));
            record.setShopCode(rs.getString("create_shop"));
            record.setStudentCardCode(rs.getString("student_card_code"));
            record.setVoucherCode(rs.getString("voucher_code"));
            record.setVasCode(rs.getString("vas_code"));
            record.setProductCode(rs.getString("add_on"));
            record.setHandsetModel(rs.getString("handset_serial"));
            record.setPayMethod(rs.getInt("pay_method"));
            record.setBankName(rs.getString("bank_name"));
            record.setBankTranCode(rs.getString("bank_tran_code"));
            record.setBankTranAmount(rs.getString("bank_tran_amount"));
            record.setReferenceId(rs.getString("reference_id"));
            record.setMainProduct(rs.getString("main_product"));
            record.setIssueDateTime(rs.getTimestamp("create_time"));
            record.setResultCode("0");
            record.setDescription("Start Processing");
        } catch (Exception ex) {
            logger.error("ERROR parse SubProfileInfo");
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
            connection = ConnectionPoolManager.getConnection("cm_pre");
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                Bonus sd = (Bonus) rc;
                batchId = sd.getBatchId();
                ps.setString(1, sd.getResultCode());
                ps.setString(2, sd.getDescription());
                ps.setLong(3, sd.getId());
                ps.setLong(4, sd.getAgentId());
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
        int[] res = new int[0];
        return res;
    }

    @Override
    public void processTimeoutRecord(List<String> ids) {
        StringBuilder sb = new StringBuilder();
        long timeSt = System.currentTimeMillis();
        try {
            //            The first delete queue timeout
            deleteQueueTimeout(ids);
            for (String sd : ids) {
                sb.append(":" + sd);
            }
            logger.warn("Dispatcher not get reponse from agent, so processTimeoutRecord ID " + sb.toString());
        } catch (Exception ex) {
            logger.error("ERROR processTimeoutRecord ID " + sb.toString());
            try {
                logTimeDb("Time to retry processTimeoutRecord", timeSt);
            } catch (Exception ex1) {
                logger.error("ERROR retry processTimeoutRecord ", ex1);
                logger.error(AppManager.logException(timeSt, ex));
            }
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
            connection = ConnectionPoolManager.getConnection("cm_pre");
            ps = connection.prepareStatement(sqlDeleteMo);
            sf.setLength(0);
            for (String id : listId) {
                ps.setString(1, "FW_99");
                ps.setString(2, "FW_Timeout");
                ps.setLong(3, Long.valueOf(id));
                ps.setLong(4, Long.valueOf(id));
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

    public String getIsdnWalletByStaffCode(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String isdnWallet = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select isdn_wallet from staff where lower(staff_code) = lower(?) and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                isdnWallet = rs.getString("isdn_wallet");
                break;
            }
            logger.info("End getIsdnWalletByStaffCode: " + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getIsdnWalletByStaffCode ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            isdnWallet = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return isdnWallet;
        }
    }

    public int insertEwalletConnectKitLog(Long transactionType, String requestId, String amount,
            String voucherCode, String staffCode, String request, String response, String errCode, String description, String orgRequestId,
            String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into EWALLET_CONNECT_KIT_LOG values (EWALLET_CONNECT_KIT_LOG_SEQ.nextval,?,?,?,?,?,sysdate,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, transactionType);
            ps.setString(2, requestId);
            ps.setString(3, amount);
            ps.setString(4, voucherCode);
            ps.setString(5, staffCode);
            ps.setString(6, request);
            ps.setString(7, response);
            ps.setString(8, errCode);
            ps.setString(9, description);
            ps.setString(10, orgRequestId);
            result = ps.executeUpdate();
            logger.info("End insertEwalletConnectKitLog staffCode " + staffCode + " isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertEwalletConnectKitLog: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
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

    public int insertSaleTransOrder(String bankName, String bankTranAmount, String bankTranCode, Long saleTransId, String note) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "insert into sale_trans_order(sale_trans_order_id,bank_name,amount,is_check,order_code,sale_trans_date,sale_trans_id,status,sale_trans_type,note)\n"
                    + "values(sale_trans_order_seq.nextval,?,?,3,?,sysdate,?,5,4,?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, bankName);
            ps.setString(2, bankTranAmount);
            ps.setString(3, bankTranCode);
            ps.setLong(4, saleTransId);
            ps.setString(5, note);
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

    public int insertSaleTrans(Long saleTransId, Long shopId, Long staffId, Long saleServiceId, Long saleServicePriceId, Double amountTax,
            Long subId, String isdn, Long reasonId, String ewalletRequestId, int payMethod, String bankTransCode, double discount, double vasAmount, Long subProfileId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO sm.sale_trans (SALE_TRANS_ID,SALE_TRANS_DATE,SALE_TRANS_TYPE,STATUS,CHECK_STOCK,INVOICE_USED_ID,"
                    + "INVOICE_CREATE_DATE,SHOP_ID,STAFF_ID,PAY_METHOD,SALE_SERVICE_ID,SALE_SERVICE_PRICE_ID,AMOUNT_SERVICE,"
                    + "AMOUNT_MODEL,DISCOUNT,PROMOTION,AMOUNT_TAX,AMOUNT_NOT_TAX,VAT,TAX,SUB_ID,ISDN,CUST_NAME,CONTRACT_NO,"
                    + "TEL_NUMBER,COMPANY,ADDRESS,TIN,NOTE,DESTROY_USER,DESTROY_DATE,APPROVER_USER,APPROVER_DATE,REASON_ID,"
                    + "TELECOM_SERVICE_ID,TRANSFER_GOODS,SALE_TRANS_CODE,STOCK_TRANS_ID,CREATE_STAFF_ID,RECEIVER_ID,SYN_STATUS,"
                    + "RECEIVER_TYPE,FROM_SALE_TRANS_ID,DAILY_SYN_STATUS,CURRENCY,CHANNEL,SALE_TRANS,SERIAL_STATUS,"
                    + "INVOICE_DESTROY_ID,SALE_PROGRAM,SALE_PROGRAM_NAME,PARENT_MASTER_AGENT_ID,PAYMENT_PAPERS_CODE,AMOUNT_PAYMENT,"
                    + "LAST_UPDATE,CLEAR_DEBIT_STATUS,CLEAR_DEBIT_TIME,CLEAR_DEBIT_USER,CLEAR_DEBIT_REQUEST_ID,IN_TRANS_ID)  "
                    + " VALUES(?,sysdate,'4','2',NULL,NULL,NULL,?,?,'1',?,?,NULL,NULL,?,NULL, "
                    + "?,?,17,?,?,?,NULL,NULL,?,NULL,NULL,\n"
                    + "NULL,NULL,NULL,NULL,NULL,NULL,?,1,NULL,?,NULL,NULL,NULL,'0',NULL,NULL,0,'MT',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,\n"
                    + "sysdate,?,?,?,?,?)";//'SYSTEM_AUTO_CN_KIT'
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransId);
            ps.setLong(2, shopId);
            ps.setLong(3, staffId);
            if (saleServiceId > 0) {
                ps.setLong(4, saleServiceId);
            } else {
                ps.setString(4, "");
            }
            if (saleServicePriceId > 0) {
                ps.setLong(5, saleServicePriceId);
            } else {
                ps.setString(5, "");
            }
            if (discount > 0) {
                ps.setDouble(6, discount / 1.17);
            } else {
                ps.setString(6, "");
            }
            ps.setDouble(7, amountTax);
            Double amountNotTax = amountTax / 1.17;
            ps.setDouble(8, amountNotTax);
            Double tax = amountTax - amountNotTax;
            ps.setDouble(9, tax);
            ps.setLong(10, subId);
            ps.setString(11, isdn);
            ps.setString(12, isdn);
            ps.setLong(13, reasonId);
            String prefix = "";
            if (vasAmount > 0 && amountTax == 0) {
                prefix = "EMOLA0000";
            } else {
                prefix = "SS0000";
            }
            String saleTransCode = prefix + String.format("%0" + 9 + "d", saleTransId);
            ps.setString(14, saleTransCode);
            if ((payMethod == 1 && ewalletRequestId != null && ewalletRequestId.length() > 0) || amountTax == 0) {
                ps.setLong(15, 1L);
                ps.setTimestamp(16, new Timestamp(new Date().getTime()));
                ps.setString(17, "SYSTEM_AUTO_CN_KIT");
                ps.setString(18, ewalletRequestId);
            } else {
                ps.setString(15, "");
                ps.setNull(16, java.sql.Types.DATE);
                ps.setString(17, "");
                ps.setString(18, "");

            }
            ps.setLong(19, subProfileId);
            result = ps.executeUpdate();
            logger.info("End insertSaleTrans saleTransId " + saleTransId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTrans: ").
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

    public int insertSaleTransDetail(Long saleTransDetailId, Long saleTransId, String stockModelId, String priceId, String saleServiceId, String saleServicePriceId,
            String stockTypeId, String stockTypeName, String stockModelCode, String stockModelName, String saleServicesCode, String saleServicesName, String accountModelCode,
            String accountModelName, String saleServicesPriceVat, String priceVat, String price, String saleServicesPrice, Double amountTax, Double discountAmout, Long quantity) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO sm.sale_trans_detail (SALE_TRANS_DETAIL_ID,SALE_TRANS_ID,SALE_TRANS_DATE,STOCK_MODEL_ID,STATE_ID,PRICE_ID,QUANTITY,DISCOUNT_ID,"
                    + "TRANSFER_GOOD,PROMOTION_ID,PROMOTION_AMOUNT,NOTE,UPDATE_STOCK_TYPE,USER_DELIVER,DELIVER_DATE,USER_UPDATE,DELIVER_STATUS,SALE_SERVICES_ID,"
                    + "SALE_SERVICES_PRICE_ID,STOCK_TYPE_ID,STOCK_TYPE_CODE,STOCK_TYPE_NAME,STOCK_MODEL_CODE,STOCK_MODEL_NAME,SALE_SERVICES_CODE,SALE_SERVICES_NAME,"
                    + "ACCOUNTING_MODEL_CODE,ACCOUNTING_MODEL_NAME,CURRENCY,VAT_AMOUNT,SALE_SERVICES_PRICE_VAT,PRICE_VAT,PRICE,SALE_SERVICES_PRICE,AMOUNT,"
                    + "DISCOUNT_AMOUNT,AMOUNT_BEFORE_TAX,AMOUNT_TAX,AMOUNT_AFTER_TAX)\n"
                    + "VALUES(?,?,sysdate,?,1,?,?,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,"
                    + "?,?,?,NULL,?,?,?,?,?,?,?,'MT',?,?,?,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransDetailId);
            ps.setLong(2, saleTransId);
            ps.setString(3, stockModelId);
            ps.setString(4, priceId);
            ps.setLong(5, quantity);
            ps.setString(6, saleServiceId);
            ps.setString(7, saleServicePriceId);
            ps.setString(8, stockTypeId);
            ps.setString(9, stockTypeName);
            ps.setString(10, stockModelCode);
            ps.setString(11, stockModelName);
            ps.setString(12, saleServicesCode);
            ps.setString(13, saleServicesName);
            ps.setString(14, accountModelCode);
            ps.setString(15, accountModelName);
            Double amountNotTax = amountTax / 1.17;
            Double tax = amountTax - amountNotTax;
            ps.setDouble(16, tax);
            ps.setString(17, saleServicesPriceVat);
            ps.setString(18, priceVat);
            ps.setString(19, price);
            ps.setString(20, saleServicesPrice);
            ps.setDouble(21, amountTax);
            if (discountAmout > 0) {
                ps.setDouble(22, discountAmout / 1.17);
                ps.setDouble(23, (amountTax - discountAmout) / 1.17);
                ps.setDouble(24, (amountTax - discountAmout) - (amountTax - discountAmout) / 1.17);
                ps.setDouble(25, amountTax - discountAmout);
            } else {
                ps.setString(22, "");
                ps.setDouble(23, amountNotTax);
                ps.setDouble(24, tax);
                ps.setDouble(25, amountTax);
            }



            result = ps.executeUpdate();
            logger.info("End insertSaleTrans saleTransId " + saleTransId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTrans: ").
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

    public int insertSaleTransSerial(Long saleTransSerialId, Long saleTransDetailId, Long stockModelId, String isdn) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO sm.sale_trans_serial \n"
                    + "VALUES(?,?,?,sysdate,NULL,NULL,NULL,?,?,1)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransSerialId);
            ps.setLong(2, saleTransDetailId);
            ps.setLong(3, stockModelId);
            ps.setString(4, isdn);
            ps.setString(5, isdn);

            result = ps.executeUpdate();
            logger.info("End insertSaleTransSerial saleTransDetailId " + saleTransDetailId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSaleTransSerial: ").
                    append(sql).append("\n")
                    .append(" saleTransDetailId ")
                    .append(saleTransDetailId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public StockModel findStockModelById(Long stockModelId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        StockModel stockModel = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from sm.stock_model where stock_model_id = ? and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, stockModelId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String stockModelCode = rs.getString("stock_model_code");
                String name = rs.getString("name");
                String accountModelCode = rs.getString("accounting_model_code");
                String accountModelName = rs.getString("accounting_model_name");
                stockModel = new StockModel();
                stockModel.setStockModelCode(stockModelCode);
                stockModel.setName(name);
                stockModel.setAccountingModelCode(accountModelCode);
                stockModel.setAccountingModelName(accountModelName);
                break;
            }
            logger.info("End findStockModelById: " + stockModelId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR findStockModelById ").append(stockModelId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            stockModel = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return stockModel;
        }
    }

    public int insertActionAudit(String isdn, String serial, String des, long subId) {
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
                    + " VALUES(seq_action_audit.nextval,sysdate,'00',4432,'ITBILLING','ITBILLING', "
                    + "'3',?,'127.0.0.1',?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, subId);
            ps.setString(2, des);
            result = ps.executeUpdate();
            logger.info("End insertActionAudit isdn " + isdn + " serial " + serial
                    + " subId " + subId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionAudit: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" serial ")
                    .append(serial)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertLogMakeSaleTransFail(Long saleTransId, String isdn, String serial, Long shopId, Long staffId, Long saleServicesId, Long saleServicesPriceId,
            Long reasonId, Long saleTransDetailId, Long saleTransSerialId, String tableName) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into kit_make_sale_trans_fail values (kit_make_sale_trans_fail_seq.nextval,?,?,?,?,?,?,?,?,?,?,?,sysdate)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, saleTransId);
            ps.setString(2, isdn);
            ps.setString(3, serial);
            ps.setLong(4, shopId);
            ps.setLong(5, staffId);
            ps.setLong(6, saleServicesId);
            ps.setLong(7, saleServicesPriceId);
            ps.setLong(8, reasonId);
            ps.setLong(9, saleTransDetailId);
            ps.setLong(10, saleTransSerialId);
            ps.setString(11, tableName);
            result = ps.executeUpdate();
            logger.info("End insertLogMakeSaleTransFail isdn " + isdn + " serial " + serial
                    + " saleTransId " + saleTransId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertLogMakeSaleTransFail: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" serial ")
                    .append(serial)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getMainProduct(String productCode, String isdn) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String mainProduct = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            String sql = "select * from product.Price_Plan where status = 1 \n"
                    + "and price_Plan_Id in (Select price_Plan_Id from product.Product_Offer_Pp \n"
                    + "where product_Offer_Id = (select offer_id from product.product_offer where product_id = (select product_id from product.product \n"
                    + "where status = 1 and product_type = 'P' and upper(product_code) = upper(?))) \n"
                    + "and (expire_Date is null or expire_Date >= trunc(sysdate)))";

            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                mainProduct = rs.getString("price_plan_code");
                logger.info("price_plan_code " + mainProduct + " isdn " + isdn);
                break;
            }
            logger.info("End getMainProduct: productCode" + productCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getMainProduct productCode").append(productCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString() + " isdn " + isdn);
            mainProduct = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return mainProduct;
        }
    }

    public Long getStockModelId(String stockModelCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Long stockModelId = 0L;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from sm.stock_model where stock_model_code = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, stockModelCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                stockModelId = rs.getLong("stock_model_id");
                break;
            }
            logger.info("End getStockModelId stockModelCode: " + stockModelCode + ", stockModelId: " + stockModelId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getStockModelId stockModelCode: ").append(stockModelCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return stockModelId;
        }
    }

    public long getDiscountForHandset(Long stockModelId, String mainProduct, String shopId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        long discountAmount = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            String sql = "select * from product.product_connect_kit_handset where stock_model_id = ? and main_product like ? and for_branch like ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, stockModelId);
            ps.setString(2, "%" + mainProduct + "%");
            ps.setString(3, "%" + shopId + "%");
            rs = ps.executeQuery();
            while (rs.next()) {
                discountAmount = rs.getLong("discount");
                break;
            }
            logger.info("End getDiscountForHandset stockModelId: " + stockModelId + ", discount: " + discountAmount + ", time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getDiscountForHandset stockModelId: ").
                    append(stockModelId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return discountAmount;
        }
    }

    public int expStockTotal(String staffCode, Long stockModelId, Long quantity) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update stock_total set quantity = quantity - ?, "
                    + " quantity_issue = quantity_issue - ?, modified_date = sysdate "
                    + " where owner_id = (select (case when staff_owner_id is not null then staff_owner_id else staff_id end) as staff_id from sm.staff where status = 1  and lower(staff_code) = lower(?)) and owner_type = 2 and stock_model_id = ? and state_id = 1 and quantity >= ? "
                    + " and quantity_issue >= ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, quantity);
            ps.setLong(2, quantity);
            ps.setString(3, staffCode);
            ps.setLong(4, stockModelId);
            ps.setLong(5, quantity);
            ps.setLong(6, quantity);
            result = ps.executeUpdate();
            logger.info("End expStockTotal staffCode " + staffCode + "stockModelId: " + stockModelId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR expStockTotal: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" stockModelId ")
                    .append(stockModelId)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateSeialExp(String staffCode, Long stockModelId, String serial) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update sm.stock_handset set status = 0 where stock_model_id = ? "
                    + " and owner_type = 2 and owner_id = (select (case when staff_owner_id is not null then staff_owner_id else staff_id end) as staff_id from sm.staff where status = 1  and lower(staff_code) = lower(?)) and  to_number(serial) = to_number(?)";//last time lock on mBCCS
            ps = connection.prepareStatement(sql);
            ps.setLong(1, stockModelId);
            ps.setString(2, staffCode);
            ps.setString(3, serial);
            result = ps.executeUpdate();
            logger.info("End updateSeialExp staffCode " + staffCode + "stockModelId: " + stockModelId + ", serial: " + serial
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSeialExp: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" stockModelId ")
                    .append(stockModelId)
                    .append(" serial ")
                    .append(serial)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public Price getPriceByStockModelCode(String stockModelCode, String priceType, String pricePolicy) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        Price priceObj = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.price where status = 1 and stock_model_id = (select stock_model_id from sm.stock_model where stock_model_code = ?)\n"
                    + "and status = 1 and sta_date <= sysdate and (end_date > trunc(sysdate) or end_date is null)\n"
                    + "and type = ? and price_policy = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, stockModelCode);
            ps.setString(2, priceType);
            ps.setString(3, pricePolicy);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long priceId = rs.getLong("price_id");
                Double price = rs.getDouble("price");
                Double vat = rs.getDouble("vat");
                String currency = rs.getString("CURRENCY");

                priceObj = new Price();
                priceObj.setPriceId(priceId);
                priceObj.setVat(vat);
                priceObj.setCurrency(currency);
                priceObj.setPrice(price);
                break;
            }
            logger.info("End getPriceByStockModelCode stockModelCode " + stockModelCode + ", priceType: "
                    + priceType + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getPriceByStockModelCode: ").
                    append(sql).append("\n")
                    .append(" stockModelCode ")
                    .append(stockModelCode)
                    .append(" priceType ")
                    .append(priceType);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return priceObj;
        }
    }

    public String getSerialOfHandset(String stockModelCode, String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String handsetSerial = "";
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from sm.stock_handset where status = 1 "
                    + "and stock_model_id = (select stock_model_id from sm.stock_model where status = 1 and stock_model_code = ?) and owner_type = 2 \n"
                    + "and owner_id = (select (case when staff_owner_id is not null then staff_owner_id else staff_id end) as staff_id from sm.staff where status =1  "
                    + "and lower(staff_code) = lower(?)) and rownum < 2";
            ps = connection.prepareStatement(sql);
            ps.setString(1, stockModelCode);
            ps.setString(2, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                handsetSerial = rs.getString("serial");
                break;
            }
            logger.info("End getSerialOfHandset, stockModelCode: " + stockModelCode + ", staffCode: " + staffCode + ", serial: " + handsetSerial
                    + ", time: " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getSerialOfHandset: stockModelCode").append(stockModelCode).
                    append("staffCode: ").append(staffCode).
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return handsetSerial;
        }
    }

    public String getMainProductConnectKit(String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String mainProduct = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            String sql = "select * from product.main_product_connect_kit where product_code = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                mainProduct = rs.getString("main_product");
                break;
            }
            logger.info("End getMainProductConnectKit productCode: " + productCode + ", mainProduct: " + mainProduct + ", time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getMainProductConnectKit productCode: ").
                    append(productCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return mainProduct;
        }
    }

    public String getBasedConfigConnectKit(String staffCode, String columnName) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String basedConfig = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
//            sql = "select distinct main_product_name from product_connect_kit where main_product_name is not null";
//            LinhNBV 20200303: filter main product by staff_code
            sql = "SELECT listagg(" + columnName + ",'|') within group (order by order_by_main_product) as based_config\n"
                    + "from (select distinct " + columnName + ", order_by_main_product from main_product_connect_kit where (for_user is null or for_user like ?) \n"
                    + "and status = 1 order by order_by_main_product)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, "%" + staffCode.toUpperCase() + "%");
            rs = ps.executeQuery();
            while (rs.next()) {
                basedConfig = rs.getString("based_config");
            }
            logger.info("End getBasedConfigConnectKit: " + basedConfig + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getBasedConfigConnectKit: ");
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return basedConfig;
        }
    }

//    public long getDiscountForHandset(String stockModelCode, String mainProduct) {
//        ResultSet rs = null;
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder br = new StringBuilder();
//        long discountAmount = 0;
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("cm_pre");
//            String sql = "select * from product.product_connect_kit_handset where stock_model_id = (select stock_model_id from sm.stock_model where status = 1 and stock_model_code = ?) and main_product like ?";
//            ps = connection.prepareStatement(sql);
//            ps.setString(1, stockModelCode);
//            ps.setString(2, "%" + mainProduct + "%");
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                discountAmount = rs.getLong("discount");
//                break;
//            }
//            logger.info("End getDiscountForHandset stockModelCode: " + stockModelCode + ", discount: " + discountAmount + ", time: "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).append("\nERROR getDiscountForHandset stockModelCode: ").
//                    append(stockModelCode).append(" Message: ").
//                    append(ex.getMessage());
//            logger.error(br + ex.toString());
//        } finally {
//            closeResultSet(rs);
//            closeStatement(ps);
//            closeConnection(connection);
//            return discountAmount;
//        }
//    }
//    public String getShopIdStaffIdOfManager(String staffCode) {
//        ResultSet rs = null;
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder br = new StringBuilder();
//        StringBuilder result = null;
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("dbsm");
//            String sql = "select * from staff where staff_id = (select (case when staff_owner_id is not null then staff_owner_id else staff_id end) as staff_id from sm.staff where status =1 \n"
//                    + "and lower(staff_code) = lower(?))";
//            ps = connection.prepareStatement(sql);
//            ps.setString(1, staffCode);
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                Long shopId = rs.getLong("shop_id");
//                Long staffId = rs.getLong("staff_id");
//                result = new StringBuilder();
//                result.append(shopId).append("|").append(staffId);
//                break;
//            }
//            logger.info("End getIsdnWalletByStaffCode: " + staffCode + " time: "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).append("\nERROR getIsdnWalletByStaffCode ").append(staffCode).append(" Message: ").
//                    append(ex.getMessage());
//            logger.error(br + ex.toString());
//            result = null;
//        } finally {
//            closeResultSet(rs);
//            closeStatement(ps);
//            closeConnection(connection);
//            return result.toString();
//        }
//    }

    public String getShopIdStaffIdByStaffCode(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        StringBuilder result = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from staff where lower(staff_code) = lower(?) and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long shopId = rs.getLong("shop_id");
                Long staffId = rs.getLong("staff_id");
                result = new StringBuilder();
                result.append(shopId).append("|").append(staffId);
                break;
            }
            logger.info("End getIsdnWalletByStaffCode: " + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getIsdnWalletByStaffCode ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            result = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result.toString();
        }
    }

    public Long getPriceProductConnectKit(String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        Long price = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            sql = "select * from product.product_connect_kit where upper(product_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                price = rs.getLong("money_fee");;
                break;
            }
            logger.info("End getPriceProductConnectKit productCode " + productCode + ", price: " + price + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getPriceProductConnectKit: ").
                    append(sql).append("\n")
                    .append(" productCode ")
                    .append(productCode);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return price;
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

    public String getPricePolicyHandset(String mainProduct, String stockModelCode, String shopId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        String pricePolicy = "";
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            sql = "select * from product.product_connect_kit_handset where status = 1 and main_product like ? and stock_model_code = ? and (for_branch is null or for_branch like ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, "%" + mainProduct + "%");
            ps.setString(2, stockModelCode);
            ps.setString(3, "%" + shopId + "%");
            rs = ps.executeQuery();
            while (rs.next()) {
                pricePolicy = rs.getString("price_policy");;
                break;
            }
            logger.info("End getPricePolicyHandset mainProduct " + mainProduct + ", pricePolicy: " + pricePolicy + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            ex.printStackTrace();
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getPricePolicyHandset: ").
                    append(sql).append("\n")
                    .append(" mainProduct ")
                    .append(mainProduct);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return pricePolicy;
        }
    }

    public int updateSubMb(String isdn, String newProduct, long offerId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update sub_mb set product_code = ?, offer_id = ? where isdn = ? and status = 2";
            ps = connection.prepareStatement(sql);
            ps.setString(1, newProduct);
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            ps.setLong(2, offerId);
            ps.setString(3, isdn);
            result = ps.executeUpdate();
            logger.info("End updateSubMb isdn " + isdn
                    + " newProduct " + newProduct + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSubMb: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" newProduct ")
                    .append(newProduct)
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
            paramList.add(new Param("BONUS_TYPE", log.getBonusType(), Param.DataType.LONG, Param.IN));
            PoolStore mPoolStore = new PoolStore("dbapp1", logger);
            PoolStore.PoolResult prs = mPoolStore.insertTable(paramList, "EWALLET_LOG");
            logTimeDb("Time to insertEwalletLog isdn " + log.getMobile(), timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getMobile());
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public ProductConnectKit getProductConnectKit(String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        ProductConnectKit product = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from product.product_connect_kit where status = 1 and upper(product_code) = upper(?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {

                Long bonusCenter = rs.getLong("bonus_center");
                Long bonusChannel = rs.getLong("bonus_channel");
                Long bonusCtv = rs.getLong("bonus_ctv");

                long isProduct = rs.getLong("is_product");
                String mainProduct = rs.getString("main_product");
                String moneyFee = rs.getString("money_fee");
                String description = rs.getString("description");
                String vasConnection = rs.getString("vas_connection");
                String vasParam = rs.getString("vas_param");
                String vasActionType = rs.getString("vas_action_type");
                String vasChannel = rs.getString("vas_channel");

                product = new ProductConnectKit();
                product.setProductCode(productCode);
                product.setMoneyFee(moneyFee);
                product.setBonusCenter(bonusCenter);
                product.setBonusChannel(bonusChannel);
                product.setBonusCtv(bonusCtv);
                product.setIsProduct(isProduct);
                product.setMainProduct(mainProduct);
                product.setDescription(description);
                product.setVasConnection(vasConnection);
                product.setVasParam(vasParam);
                product.setVasActionType(vasActionType);
                product.setVasChannel(vasChannel);
                break;
            }
            logger.info("End getProductConnectKit productCode " + productCode
                    + ",time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR getProductConnectKit: ").
                    append(sql).append("\n")
                    .append(" productCode ")
                    .append(productCode);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return product;
        }
    }

    public int insertMO(String msisdn, String vasCode, String connName, String param, String actionType, String channel) {
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
                    + "VALUES(mo_seq.nextval,?,?,?,sysdate,?,?,'MBCCS')";
            ps = connection.prepareStatement(sql);
            ps.setString(1, msisdn);
            ps.setString(2, vasCode);
            if (param != null && !param.isEmpty()) {
                ps.setString(3, param);
            } else {
                ps.setString(3, "");
            }
            ps.setString(4, actionType);
            ps.setString(5, channel);
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

    public int rollbackMainProduct(Long requestId, Long subProfileId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update cm_pre.sub_profile_info set student_card_code = (select old_student_card_code from cm_pre.request_change_product where request_id  = ?),\n"
                    + "vas_code = (select old_vas_code from cm_pre.request_change_product where request_id  = ?),\n"
                    + "handset_serial = (select old_handset_serial from cm_pre.request_change_product where request_id  = ?),\n"
                    + "pay_method =(select old_pay_method from cm_pre.request_change_product where request_id  = ?),\n"
                    + "voucher_code =(select old_voucher_code from cm_pre.request_change_product where request_id  = ?),\n"
                    + "bank_tran_code = (select old_bank_tran_code from cm_pre.request_change_product where request_id  = ?),\n"
                    + "bank_name = (select old_bank_name from cm_pre.request_change_product where request_id  = ?),\n"
                    + "bank_tran_amount =(select old_bank_tran_amount from cm_pre.request_change_product where request_id  = ?),\n"
                    + "reference_id =(select old_reference_id from cm_pre.request_change_product where request_id  = ?),\n"
                    + "main_product = (select old_main_product from cm_pre.request_change_product where request_id  = ?)\n"
                    + "where sub_profile_id  = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, requestId);
            ps.setLong(2, requestId);
            ps.setLong(3, requestId);
            ps.setLong(4, requestId);
            ps.setLong(5, requestId);
            ps.setLong(6, requestId);
            ps.setLong(7, requestId);
            ps.setLong(8, requestId);
            ps.setLong(9, requestId);
            ps.setLong(10, requestId);
            ps.setLong(11, subProfileId);
            result = ps.executeUpdate();
            logger.info("End rollbackMainProduct subProfileId " + subProfileId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR rollbackMainProduct: ").
                    append(sql).append("\n")
                    .append(" subProfileId ")
                    .append(requestId);
            logger.error(brBuilder + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int updateSubIdNo(String studentCode, String idNo, Long subId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update cm_pre.sub_id_no set student_code = ? where lower(id_no) = lower(?) and status = 1 and sub_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, studentCode);
            ps.setString(2, idNo);
            ps.setLong(3, subId);
            result = ps.executeUpdate();
            logger.info("End updateSubIdNo idNo " + idNo + ", studentCode: " + studentCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSubIdNo: ").
                    append(sql).append("\n")
                    .append(" idNo ")
                    .append(idNo);
            logger.error(brBuilder + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getIdNo(String isdn) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String result = "";
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from cm_pre.customer where cust_id = (select cust_id from cm_pre.sub_mb where status = 2 and isdn = ?)";
            ps = connection.prepareStatement(sql);
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getString("id_no");
                break;
            }
            logger.info("End getIdNo, isdn: " + isdn + ", result:" + result + ", time:"
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getIdNo: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
}
