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
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
import com.viettel.vas.util.ConnectionPoolManager;
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
public class DbKitBatchConnectProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbKitBatchConnectProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDeleteMo = "update cm_pre.kit_batch_info set status = 1, process_time = sysdate, "
            + "result_code = ?, description = ?, duration = ?, total_success = ?, total_fail = ?, "
            + "node_name = ?, group_status = ?, expire_time_group = ?  where kit_batch_id = ?";

    public DbKitBatchConnectProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
//        dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
        dbNameCofig = "cm_pre";
        poolStore = new PoolStore(dbNameCofig, logger);
    }

    public DbKitBatchConnectProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        KitBatchInfo record = new KitBatchInfo();
        long timeSt = System.currentTimeMillis();
        try {
            record.setKitBatchId(rs.getLong("kit_batch_id"));
            record.setCreateUser(rs.getString("create_user"));
            record.setUnitCode(rs.getString("unit_code"));
            record.setCustId(rs.getLong("cust_id"));
            record.setCreateTime(rs.getTimestamp("create_time"));
            record.setPayType(rs.getString("pay_type"));
            record.setBankName(rs.getString("bank_name"));
            record.setBankTransCode(rs.getString("bank_tran_code"));
            record.setBankTransAmount(rs.getString("bank_tran_amount"));
            record.setEmolaAccount(rs.getString("emola_account"));
            record.setEmolaVoucherCode(rs.getString("emola_voucher_code"));
            record.setAddMonth(rs.getString("add_month"));
            record.setChannelType(rs.getString("channel_type"));
            record.setGroupName(rs.getString("group_name") != null ? rs.getString("group_name") : "N/A");
            record.setExtendFromKitBatchId(rs.getLong("extend_from_kit_batch_id"));
            record.setResultCode("0");
            record.setDescription("Start Processing");
            record.setEnterpriseWallet(rs.getString("enterprise_wallet"));
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
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(sqlDeleteMo);
            for (Record rc : listRecords) {
                KitBatchInfo sd = (KitBatchInfo) rc;
                batchId = sd.getBatchId();
                ps.setString(1, sd.getResultCode());
                ps.setString(2, sd.getDescription());
                ps.setLong(3, sd.getDuration() != null ? sd.getDuration() : 0L);
                ps.setLong(4, sd.getTotalSuccess());
                ps.setLong(5, sd.getTotalFail());
                //ps.setLong(6, sd.getTotalMoney());
                ps.setString(6, sd.getNodeName());
                ps.setString(7, sd.getGroupStatus());
                ps.setString(8, sd.getExpireTimeGroup() != null ? sd.getExpireTimeGroup() : "");
                ps.setLong(9, sd.getKitBatchId());
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR updateQueue KIT_BATCH_INFO batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to updateQueue KIT_BATCH_INFO, batchid " + batchId, timeStart);
        }
    }

    @Override
    public int[] insertQueueHis(List<Record> listRecords) {
        logger.info("KitBatchConnect No need to insertQueueHis ");
        int[] res = new int[0];
        return res;
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
                ps.setString(1, "FW_99");
                ps.setString(2, "FW_Timeout");
                ps.setLong(3, 0L);
                ps.setLong(4, 0);
                ps.setLong(5, 0);
                ps.setLong(6, 0);
                ps.setString(7, "FW_Timeout");
                ps.setString(8, "0");
                ps.setString(10, "");
                ps.setLong(9, Long.valueOf(id));
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

    public List<KitBatch> getListKitBatchDetail(Long kitBatchId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from kit_batch_detail where kit_batch_id = ? and result_code is null";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String handsetSerial = rs.getString("handset_serial");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(kitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setHandsetSerial(handsetSerial);
                kitBatch.setCustId(rs.getLong("cust_id"));
                kitBatch.setProductAddOn(rs.getString("addon"));
                kitBatch.setIsConnectNew(rs.getInt("input_type") == 1);
                kitBatch.setMoneyProduct(rs.getString("money_product"));
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchDetail: kitBatchId" + kitBatchId + " size " + lstKitBatch.size() + " time: "
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

    public int updateKitBatchDetail(Long kitBatchId, String serial, String isdn, String resultCode, String description,
            String nodeName, String moneyProduct, String moneyIsdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update kit_batch_detail set result_code = ?, description = ?, node_name = ?, process_time = sysdate \n"
                    + "where kit_batch_id = ? and serial = ? and isdn = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, resultCode);
            ps.setString(2, description);
            ps.setString(3, nodeName != null ? nodeName : "");
            ps.setLong(4, kitBatchId);
            ps.setString(5, serial);
            ps.setString(6, isdn);
            result = ps.executeUpdate();
            logger.info("End updateKitBatchDetail kitBatchId " + kitBatchId + ", serial: " + serial + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateKitBatchDetail: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
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

    public int updateKitBatchDetailWithVas(Long kitBatchId, String serial, String isdn, long moId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update kit_batch_detail set mo_id=? where kit_batch_id = ? and serial = ? and isdn = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, moId);
            ps.setLong(2, kitBatchId);
            ps.setString(3, serial);
            ps.setString(4, isdn);
            result = ps.executeUpdate();
            logger.info("End updateKitBatchDetailWithVas kitBatchId " + kitBatchId + ", serial: " + serial + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateKitBatchDetailWithVas: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
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

    public int updateGroupEliteKitBatchDetail(Long kitBatchId, String isdn, String resultCode,
            String cugId, String cugName, String ownerGroup) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update kit_batch_detail set cug_id = ?, cug_name = ?, owner = ?, result_add_group = ?\n"
                    + "where kit_batch_id = ? and isdn = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, cugId);
            ps.setString(2, cugName);
            ps.setString(3, ownerGroup != null ? ownerGroup : "");
            ps.setString(4, resultCode != null ? resultCode : "");
            ps.setLong(5, kitBatchId);
            ps.setString(6, isdn);
            result = ps.executeUpdate();
            logger.info("End updateGroupEliteKitBatchDetail kitBatchId " + kitBatchId + ", isdn: " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateGroupEliteKitBatchDetail: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
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

    public int insertSubMbCmPre(Long subId, Long custId, String isdn, String subName, String imsi, String serial, Long offerId, String productCode, String staffCode, String shopCode, boolean isActive) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        int result = 0;
        long startTime = System.currentTimeMillis();
        String sql = "";
        try {

            connection = getConnection("cm_pre");
            if (!isActive) {
                sql = "INSERT INTO cm_pre.sub_mb \n"
                        + "VALUES(?,?,?,2,'03',NULL,NULL,NULL,'CN_AUTO',NULL,?,?,?,?,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'0',?,?,NULL,sysdate,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?,?,?,NULL,NULL,NULL,NULL,NULL)";
            } else {
                sql = "INSERT INTO cm_pre.sub_mb \n"
                        + "VALUES(?,?,?,2,'00',sysdate,NULL,NULL,'CN_AUTO',NULL,?,?,?,?,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'0',?,?,NULL,sysdate,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?,?,?,NULL,NULL,NULL,NULL,NULL)";

            }
            ps = connection.prepareStatement(sql);
            ps.setLong(1, subId);
            ps.setLong(2, custId);
            ps.setString(3, isdn);
            ps.setString(4, staffCode);
            ps.setString(5, shopCode);
            ps.setLong(6, offerId);
            ps.setString(7, subName);
            ps.setString(8, imsi);
            ps.setString(9, serial);
            ps.setString(10, productCode);
            ps.setString(11, productCode);
            ps.setString(12, isdn.substring(isdn.length() - 1));//last_number
            result = ps.executeUpdate();
            logger.info("End insertSubMbCmPre " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR insertSubMbCmPre : exception: ")
                    .append(ex.getMessage())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public Customer getCustomerByCustIdCmPre(Long custId, String channelType) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Customer customer = null;
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = " select * from cm_pre.customer where cust_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, custId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String subName = rs.getString("name");
                String repreCustIdNo = rs.getString("repre_cust_id_no");
                String frontImageUrl = rs.getString("image_name_no1");
                String busType = rs.getString("bus_type");
                String backImageUrl = "";
                String formImageUrl = "";

                if ("mBCCS".equals(channelType)) {
                    if ("INDI".equals(busType)) {
                        backImageUrl = rs.getString("image_name_no2");
//                        formImageUrl = rs.getString("image_name_no2");
                    } else {
                        backImageUrl = rs.getString("image_name");
                        formImageUrl = rs.getString("image_name_no2");
                    }

                } else {
                    backImageUrl = rs.getString("image_name_no2");
                    formImageUrl = rs.getString("image_name");
                }

                customer = new Customer();
                customer.setSubName(subName);
                customer.setIdNo(repreCustIdNo);
                customer.setFrontImageUrl(frontImageUrl);
                customer.setBackImageUrl(backImageUrl);
                customer.setFormImageUrl(formImageUrl);
                customer.setBusType(rs.getString("bus_type"));
                break;
            }
            logger.info("End getCustomerByCustId with custId: " + custId + "time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getCustomerByCustId: ------ custId: ").append(custId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return customer;
        }
    }

    public String getShopCodeByStaffCode(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String shopCode = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from shop where shop_id = (select shop_id from staff where upper(staff_code) = upper(?) and status = 1)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                shopCode = rs.getString("shop_code");
            }
            logger.info("End getShopCodeByStaffCode with staffCode: " + staffCode + "time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getShopCodeByStaffCode: ------ staffCode: ").
                    append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return shopCode;
        }
    }

    public ProductConnectKit getProductConnectKit(String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        ProductConnectKit productConnectKit = null;
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            sql = "select * from product.product_connect_kit where status = 1 and lower(product_code) = lower(?) order by order_by_product";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long productId = rs.getLong("product_id");
                Long offerId = rs.getLong("offer_id");
                productConnectKit = new ProductConnectKit();
                productConnectKit.setProductId(productId);
                productConnectKit.setProductCode(productCode);
                productConnectKit.setOfferId(offerId);
                productConnectKit.setPrice(rs.getLong("money_fee"));
                productConnectKit.setPpBuyMore(rs.getString("pp_buy_more"));
                productConnectKit.setVasConnection(rs.getString("vas_connection"));
                productConnectKit.setVasParam(rs.getString("vas_param"));
                productConnectKit.setVasActionType(rs.getString("vas_action_type"));
                productConnectKit.setVasChannel(rs.getString("vas_channel"));
                productConnectKit.setIsProduct(rs.getLong("is_product"));
                productConnectKit.setVipProduct(rs.getLong("vip_product"));
                break;
            }
            logger.info("End getProductConnectKit: productCode: " + productCode
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getProductConnectKit: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return productConnectKit;
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

    //LinhNBV start modified on September 04 2017: Get isdn of channel
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

    public String getSerialByIsdnCustomer(String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String serial = null;
        String sqlMo = " select serial from cm_pre.sub_mb where status = 2 and isdn = ? ";
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
                serial = rs1.getString("serial");
            }
            logTimeDb("Time to getSerialByIsdnCustomer: " + isdn, timeSt);
            if (serial == null) {
                serial = "";
            }
        } catch (Exception ex) {
            logger.error("ERROR getSerialByIsdnCustomer " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            serial = "";
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return serial;
    }

    public boolean checkMovitelStaff(String staffCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        try {
            connection = ConnectionPoolManager.getConnection(dbNameCofig);
            ps = connection.prepareStatement(
                    //LinhNBV start modified on April 12 2018: Add condition to check profile of channel correct or not
                    "select staff_code from sm.staff where status = 1 and channel_type_id = 14 and lower(staff_code) = lower(?)");
            if (QUERY_TIMEOUT > 0) {
                ps.setQueryTimeout(QUERY_TIMEOUT);
            }
            ps.setString(1, staffCode.toUpperCase());
            rs = ps.executeQuery();
            while (rs.next()) {
                String channel = rs.getString("staff_code");
                if (channel != null && channel.trim().length() > 0) {
                    result = true;
                }
                break;
            }
            logTimeDb("Time to checkMovitelStaff " + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkMovitelStaff defaul return false");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertSendSms(String isdn, String isdnCustomer, long requetsId, long actionAuditId) {
        /*
         * INSERT INTO send_sms (send_sms_id,source_isdn,target_isdn,sms_type,content,status,create_date,process_time,schedule_date,time_from,
         time_to,source_import,num_process,app_name,params,key,request_id,action_audit_id)
         VALUES   (send_sms_seq.NEXTVAL,'86904',?,'1',NULL,'0',SYSDATE,NULL,NULL, NULL, NULL, NULL,0,NULL,?,?,?,?);
         */
        List<ParamList> listParam = new ArrayList<ParamList>();
        int[] res = new int[0];
        long timeSt = System.currentTimeMillis();
        String param = ";Register customer's info for Smartphone;";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
            String sms_code = ResourceBundle.getBundle("configPayBonus").getString("sms_code");
            String valueAnypayAdd = ResourceBundle.getBundle("configPayBonus").getString("valueAnypayBonus");
            String anypayKeyMsg = ResourceBundle.getBundle("configPayBonus").getString("anypayKeyMsg");
            param = valueAnypayAdd + param + isdnCustomer + ";" + sdf.format(new Date());
            ParamList paramList = new ParamList();
            paramList.add(new Param("send_sms_id", "send_sms_seq.NEXTVAL", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("source_isdn", sms_code, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("target_isdn", isdn, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("sms_type", "1", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("status", "0", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("create_date", "sysdate", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("num_process", 0, Param.DataType.INT, Param.IN));
            paramList.add(new Param("params", param, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("key", anypayKeyMsg, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("request_id", requetsId, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("action_audit_id", actionAuditId, Param.DataType.LONG, Param.IN));
            listParam.add(paramList);
            if (listParam.size() > 0) {
                res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "send_sms");
                logTimeDb("Time to insertSendSms , isdn " + isdn + " result: " + res[0], timeSt);
            } else {
                logTimeDb("List Record to insertSendSms is empty, isdn " + isdn, timeSt);
            }
            return res[0];
        } catch (Exception ex) {
            logger.error("ERROR insertSendSms isdn " + isdn, ex);
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
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
            paramList.add(new Param("BONUS_TYPE", 4L, Param.DataType.LONG, Param.IN));
            PoolStore.PoolResult prs = poolStore.insertTable(paramList, "PROFILE.EWALLET_LOG");
            logTimeDb("Time to insertEwalletLog isdn " + log.getMobile() + " result insert: " + (prs == PoolStore.PoolResult.SUCCESS ? 0 : -1), timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getMobile());
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    //LinhNBV start modified on Jun 06 2018: Add method using update or rollback status stock_isdn_mobile or stock_sim.
    public int destroySubMbRecord(String serial, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "delete sub_mb where serial = ? and isdn = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            ps.setString(2, isdn);
            result = ps.executeUpdate();
            logger.info("End destroySubMbRecord isdn " + isdn + " serial " + serial + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR destroySubMbRecord: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
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

    public int updateStockIsdn(Long status, String lastUpdateUser, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update stock_isdn_mobile set status = ?, last_update_user = ?, last_update_time = sysdate where to_number(isdn) = ? \n"
                    + "and (status = 1 or status = 3 or status = 5 or status = 6)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, status);
            ps.setString(2, lastUpdateUser);
            ps.setString(3, isdn);
            result = ps.executeUpdate();
            logger.info("End updateIsdnStatusToWaitingConnect isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateIsdnStatusToWaitingConnect: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
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

    public int updateStockSim(Long status, Long hlrStatus, String serial, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;

        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update stock_sim set status = ?, hlr_status = ?, hlr_reg_date = sysdate where to_number(serial) = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, status);
            ps.setLong(2, hlrStatus);
            ps.setString(3, serial);

            result = ps.executeUpdate();
            logger.info("End updateStockSimToWaitingConnect serial " + serial + " isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            StringBuilder brBuilder = new StringBuilder();
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateStockSimToWaitingConnect: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial)
                    .append(" isdn ")
                    .append(isdn)
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

    public int updateConnectKitStatus(Long subProfileId) {
        Connection connection = null;
        PreparedStatement ps = null;

        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update cm_pre.sub_profile_info set connect_kit_status = 1 where sub_profile_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, subProfileId);

            result = ps.executeUpdate();
            logger.info("End updateConnectKitStatus subProfileId " + subProfileId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            StringBuilder brBuilder = new StringBuilder();
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateConnectKitStatus: ").
                    append(sql).append("\n")
                    .append(" subProfileId ")
                    .append(subProfileId)
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

    public String getImsiEkiSimBySerial(String serial, String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder btBuilder = new StringBuilder();
        StringBuilder result = new StringBuilder();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select imsi, eki from stock_sim where to_number(serial) = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            rs = ps.executeQuery();
            while (rs.next()) {
                String imsi = rs.getString("imsi");
                result.append(imsi).append("|");
                String eki = rs.getString("eki");
                result.append(eki);
                logger.info("checkSerialSimIsSale " + imsi + " isdn " + isdn);
            }
            logger.info("End checkSerialSimIsSale serial " + serial + " isdn " + isdn
                    + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            btBuilder.setLength(0);
            btBuilder.append(loggerLabel).append(new Date()).append("\nERROR checkSerialSimIsSale ---- serial ").append(serial).append(" Message: ").
                    append(ex.getMessage());
            logger.error(btBuilder + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result.toString();
        }
    }

    public List<AssignedUser> getListAssignedUser() {
        List<AssignedUser> lstAssignedUser = new ArrayList<AssignedUser>();
        ResultSet rs = null;
        ResultSet rs2 = null;
        Connection connection = null;
        String sql = "select upper(assigned_user) assigned_user, count(*) as so_luong  from sub_profile_info where check_status = 0 and upper(assigned_user) in (select distinct upper(user_name) \n"
                + "from sub_profile_log where login_time < sysdate and logout_time is null) "
                + "and upper(assigned_user) in (select upper(user_name) \n"
                + "from sub_profile_role where role_id = 0)"
                + "group by upper(assigned_user) ";
        String sql2 = "select   distinct upper(user_name) assigned_user "
                + "                from sub_profile_log where login_time < sysdate and logout_time is null "
                + "                and upper(user_name) in (select upper(user_name) "
                + "                from sub_profile_role where role_id = 0)"
                + " and upper(user_name) not in (\n"
                + "select upper(assigned_user)  from sub_profile_info where check_status = 0 and upper(assigned_user) in (select distinct upper(user_name)\n"
                + "                from sub_profile_log where login_time < sysdate and logout_time is null) \n"
                + "                and upper(assigned_user) in (select upper(user_name) \n"
                + "                from sub_profile_role where role_id = 0)\n"
                + "                group by upper(assigned_user) having count(*) > 0\n"
                + ")    ";
        PreparedStatement ps = null;
        PreparedStatement ps2 = null;
        try {
            connection = getConnection("cm_pre");
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String assigned = rs.getString("assigned_user");
                Long count = rs.getLong("so_luong");
                AssignedUser assignedUser = new AssignedUser(assigned, count);
                lstAssignedUser.add(assignedUser);
            }
            ps2 = connection.prepareStatement(sql2);
            rs2 = ps2.executeQuery();
            while (rs2.next()) {
                String assigned = rs2.getString("assigned_user");
                AssignedUser assignedUser = new AssignedUser(assigned, 0l);
                lstAssignedUser.add(assignedUser);
            }
            logger.error("Finish to getListAssignedUser count of result " + lstAssignedUser.size());
        } catch (Exception ex) {
            logger.error("Cannot get getListAssignedUser" + ex.toString());
            return null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeResultSet(rs2);
            closeStatement(ps2);
            closeConnection(connection);
        }
        return lstAssignedUser;
    }

    public int insertSubProfileInfo(Long subProfileId, Long subId, Long custId, Long actionAuditId, String isdn, String createStaff, String shopCode,
            String frontImageUrl, String backImageUrl, String formImageUrl, String assignUser, String actionCode, long reasonId,
            String simSerial, String subName, String idNo, Long kitBatchId, String checkCommen, String vasCode, boolean isChecked) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String sqlChecked = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            if (isChecked) {
                sql = "insert into sub_profile_info (sub_profile_id, sub_id, cust_id, action_audit_id, isdn, create_time, "
                        + "create_staff, create_shop, front_image_url, back_image_url, form_image_url, student_card_code, assigned_user"
                        + ", action_code, reason_id, check_status, check_commend, original_id_no, original_cust_id, original_cus_name, bts_2g, "
                        + "sim_serial, voucher_code, request_emola, vas_code, kit_batch_id, connect_kit_status,check_time) "
                        + "values (?, ?,?,?,?,sysdate,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,2,sysdate)";
            } else {
                sql = "insert into sub_profile_info (sub_profile_id, sub_id, cust_id, action_audit_id, isdn, create_time, "
                        + "create_staff, create_shop, front_image_url, back_image_url, form_image_url, student_card_code, assigned_user"
                        + ", action_code, reason_id, check_status, check_commend, original_id_no, original_cust_id, original_cus_name, bts_2g, "
                        + "sim_serial, voucher_code, request_emola, vas_code, kit_batch_id, connect_kit_status) "
                        + "values (?, ?,?,?,?,sysdate,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,2)";
            }

            ps = connection.prepareStatement(sql);
            ps.setLong(1, subProfileId);
            ps.setLong(2, subId);
            ps.setLong(3, custId);
            ps.setLong(4, actionAuditId);
            ps.setString(5, isdn);
            ps.setString(6, createStaff);
            ps.setString(7, shopCode);
            ps.setString(8, frontImageUrl != null ? frontImageUrl : "");
            ps.setString(9, backImageUrl != null ? backImageUrl : "");
            ps.setString(10, formImageUrl != null ? formImageUrl : "");
            ps.setString(11, "");
            ps.setString(12, assignUser);
            ps.setString(13, actionCode);
            ps.setLong(14, reasonId);
            ps.setLong(15, isChecked ? 1 : 0);
            ps.setString(16, checkCommen);
            ps.setString(17, idNo);
            ps.setLong(18, custId);
            ps.setString(19, subName);
            ps.setString(20, "");
            ps.setString(21, simSerial);
            ps.setString(22, "");
            ps.setString(23, "1");
            ps.setString(24, vasCode != null ? vasCode : "");
            ps.setLong(25, kitBatchId);

            result = ps.executeUpdate();
            logger.info("End insertSubProfileInfo ---- isdn " + isdn + "---subId:" + subId + "-----custId: " + custId
                    + " assignUser " + assignUser + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertSubProfileInfo: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
            result = -1;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public String getProductCode(String serial, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String productCode = null;
        String sqlMo = "select * from sub_mb where serial = ? and isdn = ? and status = 2";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, serial);
            psMo.setString(2, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                productCode = rs1.getString("product_code");
            }
            if (productCode == null) {
                productCode = "";
                logger.info("productCode is null - serial: " + serial);
            }
            logTimeDb("Time to getProductCode: " + serial + " isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getProductCode " + serial + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return productCode;
        }
    }

    public Long getReasonIdByProductCode(String productCode, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Long reasonId = null;
        String sqlMo = "select * from Reason r where r.status = 1 and r.type = '00'\n"
                + "and r.reason_Id in (select m.reason_Id from Mapping m where (upper(m.product_Code) = upper(?) or m.product_Code is null) \n"
                + "and m.tel_Service_Id = 1 and m.status = 1 \n"
                + "and ( m.end_Date is null or m.end_Date >= trunc(sysdate))\n"
                + ")\n"
                + "order by NLSSORT(r.code,'NLS_SORT=vietnamese')";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, productCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                reasonId = rs1.getLong("reason_id");
                logger.info("Get reason_i " + reasonId + " base on productCode " + productCode + " for isdn " + isdn);
            }
            logTimeDb("Time to getReasonIdByProductCode: " + reasonId
                    + " base on productCode " + productCode + " for isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getReasonIdByProductCode " + reasonId);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return reasonId;
        }
    }

    public String getSaleServiceCode(Long reasonId, String productCode, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        String saleServiceCode = null;
        String sqlMo = "Select * from Mapping m, Reason r \n"
                + "where m.reason_Id = r.reason_Id and r.status = 1 and m.status = 1 \n"
                + "and m.channel is null and m.reason_Id = ? and (upper(m.product_Code) = upper(?) or m.product_Code is null)";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, reasonId);
            psMo.setString(2, productCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                saleServiceCode = rs1.getString("sale_service_code");
                logger.info("sale_service_code " + saleServiceCode
                        + " base on productCode " + productCode + " reasonId " + reasonId + " isdn " + isdn);
            }
            logTimeDb("Time to getSaleServiceCode: " + saleServiceCode
                    + " base on productCode " + productCode + " reasonId " + reasonId + " isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSaleServiceCode " + " base on productCode " + productCode + " reasonId " + reasonId + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return saleServiceCode;
        }
    }

    public SaleServices getSaleService(String saleServiceCode, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        SaleServices saleService = null;
        String sqlMo = "select * from sm.sale_services where code = ? and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, saleServiceCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Long saleServiceId = rs1.getLong("sale_services_id");
                String name = rs1.getString("name");
                String accountModelCode = rs1.getString("accounting_model_code");
                String accountModelName = rs1.getString("accounting_model_name");
                saleService = new SaleServices();
                saleService.setSaleServicesId(saleServiceId);
                saleService.setName(name);
                saleService.setAccountModelCode(accountModelCode);
                saleService.setAccountModelName(accountModelName);
                logger.info("Result getSaleService  " + saleServiceCode
                        + " saleServiceId " + saleServiceId + " isdn " + isdn);
                break;
            }
            logTimeDb("Time to getSaleService: " + saleServiceCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSaleService " + saleServiceCode + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return saleService;
        }
    }

    public SaleServicesPrice getSaleServicesPrice(Long saleServicesId, String isdn) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        SaleServicesPrice saleServicesPrice = null;
        String sqlMo = "select * from Sale_Services_Price where sale_Services_Id = ? \n"
                + "and sta_Date <= to_date(to_char(trunc(sysdate),'MM/DD/YYYY')||' 23:59:59','MM/DD/YYYY HH24:MI:SS')\n"
                + "and (((end_Date >= to_date(to_char(trunc(sysdate),'MM/DD/YYYY')||' 00:00:00','MM/DD/YYYY HH24:MI:SS')) and (end_Date is not null)) or (end_Date is null)) \n"
                + "and status = 1 and price_Policy = 1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, saleServicesId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Long saleServicesPriceId = rs1.getLong("sale_services_price_id");
                Double price = rs1.getDouble("price");
                Double vat = rs1.getDouble("vat");
                saleServicesPrice = new SaleServicesPrice();
                saleServicesPrice.setSaleServicesPriceId(saleServicesPriceId);
                saleServicesPrice.setPrice(price);
                saleServicesPrice.setVat(vat);
                logger.info("Result getSaleServicesPrice saleServicesId "
                        + saleServicesId + " saleServicesPriceId " + saleServicesPriceId + " isdn " + isdn);
                break;
            }
            logTimeDb("Time to getSaleServicesPrice: " + saleServicesId + " isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getSaleServicesPrice " + saleServicesId + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
            return saleServicesPrice;
        }
    }

    public Price getPrice(String isdn, Long saleServicesId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Price priceObj = null;
        String sqlMo = "select * from price where 1 = 1 and price_id = (select price_id from Sale_Services_Detail where stock_model_id = (select stock_model_id from stock_isdn_mobile where isdn = ?) and status= 1 \n"
                + "and sale_Services_Model_Id in (select sale_Services_Model_Id from Sale_Services_Model where sale_Services_Id = ? and stock_Type_Id = 1 )) \n"
                + "and status = 1 and trunc(sta_date) <= trunc(sysdate) and (end_Date is null or trunc(end_Date) >= trunc(sysdate))";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setString(1, isdn);
            psMo.setLong(2, saleServicesId);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Long priceId = rs1.getLong("price_id");
                Long stockModelId = rs1.getLong("stock_model_id");
                Double price = rs1.getDouble("price");
                priceObj = new Price();
                priceObj.setPriceId(priceId);
                priceObj.setStockModelId(stockModelId);
                priceObj.setPrice(price);
                logger.info("Result getPrice " + saleServicesId + " isdn " + isdn + " priceId " + priceId);
                break;
            }
            logTimeDb("Time to getPrice: " + saleServicesId + " isdn " + isdn, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getPrice " + saleServicesId + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return priceObj;
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
        String isdnWallet = "";
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
            isdnWallet = "";
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

    public int insertSaleTrans(Long saleTransId, Long shopId, Long staffId, Long saleServiceId, Long saleServicePriceId, Double amountTax,
            Long subId, String isdn, Long reasonId, String ewalletRequestId, int payMethod, String bankTransCode, double discount, double amountVas, long kitBatchId) {
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
                    + "RECEIVER_TYPE,IN_TRANS_ID,FROM_SALE_TRANS_ID,DAILY_SYN_STATUS,CURRENCY,CHANNEL,SALE_TRANS,SERIAL_STATUS,"
                    + "INVOICE_DESTROY_ID,SALE_PROGRAM,SALE_PROGRAM_NAME,PARENT_MASTER_AGENT_ID,PAYMENT_PAPERS_CODE,AMOUNT_PAYMENT,"
                    + "LAST_UPDATE,CLEAR_DEBIT_STATUS,CLEAR_DEBIT_TIME,CLEAR_DEBIT_USER,CLEAR_DEBIT_REQUEST_ID)  "
                    + " VALUES(?,sysdate,'4','2',NULL,NULL,NULL,?,?,'1',?,?,NULL,NULL,?,NULL, "
                    + "?,?,17,?,?,?,NULL,NULL,?,NULL,NULL,\n"
                    + "NULL,NULL,NULL,NULL,NULL,NULL,?,1,NULL,?,NULL,NULL,NULL,'0',NULL,NULL,NULL,0,'MT',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,\n"
                    + "sysdate,?,?,?,?)";//'SYSTEM_AUTO_CN_KIT'
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
            ps.setString(12, kitBatchId + "");
            ps.setLong(13, reasonId);
            String prefix = "";
            if (amountVas > 0 && amountTax == 0) {
                prefix = "EMOLA0000";
            } else {
                prefix = "SS0000";
            }
            String saleTransCode = prefix + String.format("%0" + 9 + "d", saleTransId);
            ps.setString(14, saleTransCode);
            if (((payMethod == 1 || payMethod == 2) && ewalletRequestId != null && ewalletRequestId.length() > 0) || amountTax == 0) {
                ps.setLong(15, 1L);
                ps.setTimestamp(16, new Timestamp(new Date().getTime()));
                ps.setString(17, "SYSTEM_AUTO_CN_KIT");
                ps.setString(18, ewalletRequestId);
            } //            else if (payMethod == 0) {
            //                ps.setLong(15, 1L);
            //                ps.setTimestamp(16, new Timestamp(new Date().getTime()));
            //                ps.setString(17, "CLEAR_BY_BANK_TRANSFER");
            //                ps.setString(18, bankTransCode);
            //            } 
            else {
                ps.setString(15, "");
                ps.setNull(16, java.sql.Types.DATE);
                ps.setString(17, "");
                ps.setString(18, "");

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
                    + "values(sale_trans_order_seq.nextval,?,?,3,?,sysdate,?,5,4,'Clear by bankTransfer from connectKitBatch.')";
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

//    public int insertSaleTrans(Long saleTransId, Long shopId, Long staffId, Long saleServiceId, Long saleServicePriceId, Double amountTax,
//            Long subId, String isdn, Long reasonId, String ewalletRequestId, String payMethod) {
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder br = new StringBuilder();
//        String sql = "";
//        int result = 0;
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("dbsm");
//            sql = "INSERT INTO sm.sale_trans (SALE_TRANS_ID,SALE_TRANS_DATE,SALE_TRANS_TYPE,STATUS,CHECK_STOCK,INVOICE_USED_ID,"
//                    + "INVOICE_CREATE_DATE,SHOP_ID,STAFF_ID,PAY_METHOD,SALE_SERVICE_ID,SALE_SERVICE_PRICE_ID,AMOUNT_SERVICE,"
//                    + "AMOUNT_MODEL,DISCOUNT,PROMOTION,AMOUNT_TAX,AMOUNT_NOT_TAX,VAT,TAX,SUB_ID,ISDN,CUST_NAME,CONTRACT_NO,"
//                    + "TEL_NUMBER,COMPANY,ADDRESS,TIN,NOTE,DESTROY_USER,DESTROY_DATE,APPROVER_USER,APPROVER_DATE,REASON_ID,"
//                    + "TELECOM_SERVICE_ID,TRANSFER_GOODS,SALE_TRANS_CODE,STOCK_TRANS_ID,CREATE_STAFF_ID,RECEIVER_ID,SYN_STATUS,"
//                    + "RECEIVER_TYPE,IN_TRANS_ID,FROM_SALE_TRANS_ID,DAILY_SYN_STATUS,CURRENCY,CHANNEL,SALE_TRANS,SERIAL_STATUS,"
//                    + "INVOICE_DESTROY_ID,SALE_PROGRAM,SALE_PROGRAM_NAME,PARENT_MASTER_AGENT_ID,PAYMENT_PAPERS_CODE,AMOUNT_PAYMENT,"
//                    + "LAST_UPDATE,CLEAR_DEBIT_STATUS,CLEAR_DEBIT_TIME,CLEAR_DEBIT_USER,CLEAR_DEBIT_REQUEST_ID)  "
//                    + " VALUES(?,sysdate,'4','2',NULL,NULL,NULL,?,?,?,?,?,NULL,NULL,NULL,NULL, "
//                    + "?,?,17,?,?,?,NULL,NULL,?,NULL,NULL,\n"
//                    + "NULL,NULL,NULL,NULL,NULL,NULL,?,1,NULL,?,NULL,NULL,NULL,'0',NULL,NULL,NULL,0,'MT',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,\n"
//                    + "sysdate,1,sysdate,'SYSTEM_AUTO_CN_KIT',?)";
//            ps = connection.prepareStatement(sql);
//            ps.setLong(1, saleTransId);
//            ps.setLong(2, shopId);
//            ps.setLong(3, staffId);
//            ps.setString(4, payMethod);
//            ps.setLong(5, saleServiceId);
//            ps.setLong(6, saleServicePriceId);
//            ps.setDouble(7, amountTax);
//            Double amountNotTax = amountTax / 1.17;
//            ps.setDouble(8, amountNotTax);
//            Double tax = amountTax - amountNotTax;
//            ps.setDouble(9, tax);
//            ps.setLong(10, subId);
//            ps.setString(11, isdn);
//            ps.setString(12, isdn);
//            ps.setLong(13, reasonId);
//            String saleTransCode = "SS0000" + String.format("%0" + 9 + "d", saleTransId);
//            ps.setString(14, saleTransCode);
//            ps.setString(15, ewalletRequestId);
//            result = ps.executeUpdate();
//            logger.info("End insertSaleTrans saleTransId " + saleTransId + " result " + result + " time "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).
//                    append("\nERROR insertSaleTrans: ").
//                    append(sql).append("\n")
//                    .append(" saleTransId ")
//                    .append(saleTransId)
//                    .append(" result ")
//                    .append(result);
//            logger.error(br + ex.toString());
//        } finally {
//            closeStatement(ps);
//            closeConnection(connection);
//            return result;
//        }
//    }
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

    public Long getStockModelIdByIsdn(String isdn) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Long stockModelId = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select stock_model_id from sm.stock_isdn_mobile where isdn = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                stockModelId = rs.getLong("stock_model_id");
                break;
            }
            logger.info("End getStockModelIdByIsdn: " + isdn + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getStockModelIdByIsdn ").append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            stockModelId = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return stockModelId;
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

    public int insertDataKitVas(String isdn, String serial, String userCreated, String productCode) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "insert into kit_vas(kit_vas_id, isdn, serial, status, create_user, create_date, product_code) "
                    + "values (kit_vas_seq.nextval,?,?,1,?,sysdate,?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setString(2, serial);
            ps.setString(3, userCreated);
            ps.setString(4, productCode);
            result = ps.executeUpdate();
            logger.info("End insertDataKitVas userCreated " + userCreated + " isdn: " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertDataKitVas: ").
                    append(sql).append("\n")
                    .append(" userCreated ")
                    .append(userCreated)
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

    public int insertActionAudit(Long actionAuditId, String des, long subId, String shopCode, String userName) {
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
                    + " VALUES(?,sysdate,'00',4432,?,?, "
                    + "'1',?,'127.0.0.1',?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            ps.setString(2, shopCode);
            ps.setString(3, userName);
            ps.setLong(4, subId);
            ps.setString(5, des);
            result = ps.executeUpdate();
            logger.info("End insertActionAudit actionAuditId " + actionAuditId
                    + " subId " + subId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionAudit: ").
                    append(sql).append("\n")
                    .append(" actionAuditId ")
                    .append(actionAuditId)
                    .append(" subId ")
                    .append(subId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public Comission getComissionStaff(String staffCode, String productCode, String isdn) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Comission comission = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            String sql = "select * from product.product_connect_kit where upper(product_code) = upper(?) and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                long bonusCenter = rs.getLong("bonus_center");
                long bonusCtv = rs.getLong("bonus_ctv");
                long bonusChannel = rs.getLong("bonus_channel");
                long bonusCtvCenter = rs.getLong("bonus_ctv_center");
                long bonusChannelCtv = rs.getLong("bonus_channel_ctv");
                long bonusChanelCenter = rs.getLong("bonus_channel_center");
                comission = new Comission();
                comission.setBonusCenter(bonusCenter);
                comission.setBonusCtv(bonusCtv);
                comission.setBonusChannel(bonusChannel);
                comission.setBonusCtvCenter(bonusCtvCenter);
                comission.setBonusChannelCtv(bonusChannelCtv);
                comission.setBonusChannelCenter(bonusChanelCenter);
                break;
            }
            logger.info("End getBonusDirectorCenter: staffCode " + staffCode + " isdn " + isdn + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getStaffType ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            comission = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return comission;
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

    public int insertActionLogPr(String isdn, String serial, String shopCode, String staffCode,
            String request, String response, String responseCode) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "INSERT INTO action_log_pr \n"
                    + "VALUES(seq_action_log_pr.nextval,NULL,?,?,NULL,NULL,sysdate,'127.0.0.1',?,?,?,?,?,NULL)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setString(2, serial);
            ps.setString(3, shopCode);
            ps.setString(4, staffCode);
            ps.setString(5, request);
            ps.setString(6, response);
            ps.setString(7, responseCode);

            result = ps.executeUpdate();
            logger.info("End insertActionLogPr serial " + serial + " isdn: " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionLogPr: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial)
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

    public Long getK4SNO(String imsi) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Long k4sno = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from k4sno_connect_kit where (to_number(?) between imsi_start and imsi_end)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, imsi);
            rs = ps.executeQuery();
            while (rs.next()) {
                k4sno = rs.getLong("k4sno");
                logger.info("k4sno " + k4sno + " imsi " + imsi);
                break;
            }
            logger.info("End getK4SNO: imsi" + imsi + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getK4SNO imsi").append(imsi).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString() + " imsi " + imsi);
            k4sno = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return k4sno;
        }
    }

    public String getTPLID(String isdn, boolean isSim4G) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String tplId = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from tplid_connect_kit where prefix = substr(?,0,4)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                if (isSim4G) {
                    tplId = rs.getString("tplid_4g");
                } else {
                    tplId = rs.getString("tplid");
                }

                logger.info("tplid " + tplId + " isdn " + isdn);
                break;
            }
            logger.info("End getTPLID: isdn" + isdn + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getTPLID isdn").append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString() + " isdn " + isdn);
            tplId = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return tplId;
        }
    }

    public boolean checkVipProductConnectKit(String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbProduct");
            sql = "select * from product.product_connect_kit where vip_product = 1 and status = 1 and upper(product_code) = upper(?)";
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
            logger.info("End checkVipProductConnectKit productCode " + productCode
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkVipProductConnectKit: ").
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

    public int insertEwalletDeductRequest(long actionAuditId, String isdn, String staffCode, double money,
            String serial, String product, Date createTime, String client, String fundCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp1");
            sql = "INSERT INTO ewallet_deduct_request (EWALLET_DEDUCT_REQUEST_ID,ACTION_AUDIT_ID,ISDN,CREATE_STAFF,MONEY,SIM_SERIAL,"
                    + "PRODUCT_CODE,CREATE_TIME,CLIENT,FUND_CODE) \n"
                    + "VALUES(ewallet_deduct_request_seq.nextval,?,?,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            ps.setString(2, isdn);
            ps.setString(3, staffCode);
            ps.setDouble(4, money);
            ps.setString(5, serial);
            ps.setString(6, product);
            ps.setTimestamp(7, new Timestamp(createTime.getTime()));
            ps.setString(8, client);
            ps.setString(9, fundCode);
            result = ps.executeUpdate();
            logger.info("End insertEwalletDeductRequest msisdn " + isdn + " actionAuditId " + actionAuditId
                    + " money " + money + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertEwalletDeductRequest: ").
                    append(sql).append("\n")
                    .append(" msisdn ")
                    .append(isdn)
                    .append(" actionAuditId ")
                    .append(actionAuditId)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int insertBonusConnectKit(String staffCode, String productCode, String isdn, long actionAudit,
            int channelType, String actionCode, String serial, Date createTime) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp1");
            sql = "INSERT INTO bonus_connect_kit (action_audit_id,isdn,create_time,create_staff,productcode,serial,"
                    + "count_process,last_process,channel_type_id,action_code) \n"
                    + "VALUES(?,?,?,?,?,?,0,null,?, ?) ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAudit);
            ps.setString(2, isdn);
            ps.setTimestamp(3, new Timestamp(createTime.getTime()));
            ps.setString(4, staffCode);
            ps.setString(5, productCode);
            ps.setString(6, serial);
            ps.setInt(7, channelType);
            ps.setString(8, actionCode);
            result = ps.executeUpdate();
            logger.info("End insertBonusConnectKit isdn " + isdn + " actionAuditId " + actionAudit
                    + " staffCode " + staffCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertBonusConnectKit: ").
                    append(sql).append("\n")
                    .append(" msisdn ")
                    .append(isdn)
                    .append(" actionAuditId ")
                    .append(actionAudit)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkVipAgentHasDebit(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp1");
            sql = "select * from ewallet_deduct_daily where bill_cycle_date = trunc(sysdate -1) and result_code = '0'";
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            if (rs.next()) {
                Long transId = rs.getLong("ewallet_deduct_daily_id");
                if (transId > 0) {
                    result = true;
                }
            }
            logger.info("End checkVipAgentHasDebit isdn " + isdn
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR checkVipAgentHasDebit: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
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

    public boolean isSim4G(String serial, Long stockModelId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        long transId = 0;
        try {
            connection = getConnection("dbsm");
            sql = "select * from sm.stock_sim where serial = to_number(?) and stock_model_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            ps.setLong(2, stockModelId);
            rs = ps.executeQuery();
            while (rs.next()) {
                transId = rs.getLong("stock_model_id");
                if (transId > 0) {
                    result = true;
                    break;
                }
            }
            logger.info("End isSim4G serial " + serial
                    + " result " + result + " transId " + transId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR isSim4G: ").
                    append(sql).append("\n")
                    .append(" serial ")
                    .append(serial)
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

    public String getOwnerStaffOfChannel(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String result = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select staff_code from staff where status = 1 and staff_id = (select staff_owner_id from staff where upper(staff_code) = upper(?))";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getString("staff_code");
                break;
            }
            logger.info("End getOwnerStaffOfChannel: staffCode" + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getOwnerStaffOfChannel ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            result = null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int changeBatchProcessing(long kitBatchId) {
        Connection connection = null;
        PreparedStatement ps = null;
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update cm_pre.kit_batch_info set status = 9 where kit_batch_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            result = ps.executeUpdate();
            logger.info("End changeBatchProcessing kitBatchId " + kitBatchId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            StringBuilder brBuilder = new StringBuilder();
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR changeBatchProcessing: ").
                    append(sql).append("\n")
                    .append(" kitBatchId ")
                    .append(kitBatchId)
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

    public List<KitBatch> getListKitBatchDetailElite(Long kitBatchId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from kit_batch_detail where result_code = '0' and kit_batch_id in (select kit_batch_id from kit_batch_info "
                    + "where kit_batch_id = ? or extend_from_kit_batch_id = ? and result_code = '0') and (product_code in \n"
                    + " (select product_code from product.product_connect_kit where vip_product = 1 and status = 1) "
                    + " ) ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            ps.setLong(2, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long tmpKitBatchId = rs.getLong("kit_batch_id");
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String stateOfRecord = rs.getString("state_of_record");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(tmpKitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setStateOfRecord(stateOfRecord);
                kitBatch.setCustId(rs.getLong("cust_id"));
                kitBatch.setIsConnectNew(rs.getInt("input_type") == 1);
                kitBatch.setProductAddOn(rs.getString("addon"));
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchDetailElite: kitBatchId" + kitBatchId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getListKitBatchDetailElite ").
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

    public String getCUGInformation(Long kitBatchId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String cugName = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select distinct nvl(cug_name,'N/A') as cug_name from cm_pre.kit_batch_detail where kit_batch_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                cugName = rs.getString("cug_name");
                break;
            }
            logger.info("End getCUGInformation kitBatchId " + kitBatchId
                    + " cugName " + cugName + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkElitePackage ---- kitBatchId ").
                    append(kitBatchId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return cugName;
        }
    }

    public String getExpireTimeCUG(Long kitBatchId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String expireTime = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from cm_pre.kit_batch_info where kit_batch_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                expireTime = rs.getString("expire_time_group");
                break;
            }
            logger.info("End getExpireTimeCUG kitBatchId " + kitBatchId
                    + " expireTime " + expireTime + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getExpireTimeCUG ---- kitBatchId ").
                    append(kitBatchId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return expireTime;
        }
    }

    public int updateExpireTimeKitBatchInfo(Long kitBatchId, String expireTime) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "update kit_batch_info set expire_time_group = ?, group_status = 1 \n"
                    + "where kit_batch_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, expireTime);
            ps.setLong(2, kitBatchId);
            result = ps.executeUpdate();
            logger.info("End updateExpireTimeKitBatchInfo kitBatchId " + kitBatchId + ", expireTime: " + expireTime + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateExpireTimeKitBatchInfo: ").
                    append(sql).append("\n")
                    .append(" kitBatchId ")
                    .append(kitBatchId)
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

    public int insertMOWithMoId(String msisdn, String vasCode, String connName, String param, String actionType, String channel, long moID) {
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
            ps.setLong(1, moID);
            ps.setString(2, msisdn);
            ps.setString(3, vasCode);
            ps.setString(4, param);
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
            ps.setString(3, param);
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

    public int insertKitElitePrepaid(String addMonth, Long kitBatchId, String createUser) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "INSERT INTO kit_elite_prepaid (id,prepaid_month,remain_month,prepaid_type,kit_batch_id,status,create_time,remain_time,create_user) \n"
                    + "VALUES(kit_elite_prepaid_seq.nextval,?,?,0,?,1,sysdate,sysdate,?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, addMonth);
            ps.setString(2, addMonth);
            ps.setLong(3, kitBatchId);
            ps.setString(4, createUser);
            result = ps.executeUpdate();
            logger.info("End insertKitElitePrepaid kitBatchId " + kitBatchId + " createUser " + createUser + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertKitElitePrepaid: ").
                    append(sql).append("\n")
                    .append(" kitBatchId ")
                    .append(kitBatchId)
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

//    public double getDiscountAmountForHandset(String productCode) {
//        ResultSet rs = null;
//        Connection connection = null;
//        PreparedStatement ps = null;
//        StringBuilder br = new StringBuilder();
//        double discountAmount = 0;
//        long startTime = System.currentTimeMillis();
//        try {
//            connection = getConnection("cm_pre");
//            String sql = "select * from kit_prepaid_promotion where product_code = ? and status = 1";
//            ps = connection.prepareStatement(sql);
//            ps.setString(1, productCode);
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                discountAmount = rs.getDouble("discount_amount");
//                break;
//            }
//            logger.info("End getDiscountAmountForHandset, productCode: " + productCode
//                    + ", discountAmount" + discountAmount + " time: "
//                    + (System.currentTimeMillis() - startTime));
//        } catch (Exception ex) {
//            br.setLength(0);
//            br.append(loggerLabel).append(new Date()).append("\nERROR getDiscountAmountForHandset productCode").append(productCode).
//                    append(" Message: ").
//                    append(ex.getMessage());
//            logger.error(br + ex.toString() + " productCode " + productCode);
//        } finally {
//            closeResultSet(rs);
//            closeStatement(ps);
//            closeConnection(connection);
//            return discountAmount;
//        }
//    }
    public Long getStockModelIdHandset(String serial) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Long stockModelId = 0L;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select stock_model_id from sm.stock_handset where serial = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, serial);
            rs = ps.executeQuery();
            while (rs.next()) {
                stockModelId = rs.getLong("stock_model_id");
                break;
            }
            logger.info("End getStockModelIdHandset serial: " + serial + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getStockModelIdHandset serial: ").append(serial).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return stockModelId;
        }
    }

    public Long getPricePolicyByStaffCode(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Long pricePolicy = 1L;//pricePolicy Default...
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from shop where shop_id = (select shop_id from staff "
                    + "where upper(staff_code) = upper(?) and status = 1)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                pricePolicy = rs.getLong("price_policy");
                break;
            }
            logger.info("End getPricePolicyByStaffCode staffCode: " + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getPricePolicyByStaffCode staffCode: ").
                    append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return pricePolicy;
        }
    }

    public Price getPriceForSaleRetail(Long stockModelId, Long pricePolicy) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        Price priceObj = null;
        String sqlMo = "select * from price where stock_model_id = ? and \n"
                + "type = 1 and price_policy = ? \n"
                + "and sta_date <= sysdate and ((end_date >= sysdate and end_date is not null) or end_date is null)\n"
                + "and status = 1";
        PreparedStatement psMo = null;
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
            psMo = connection.prepareStatement(sqlMo);
            if (QUERY_TIMEOUT > 0) {
                psMo.setQueryTimeout(QUERY_TIMEOUT);
            }
            psMo.setLong(1, stockModelId);
            psMo.setLong(2, pricePolicy);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                Long priceId = rs1.getLong("price_id");
                Double price = rs1.getDouble("price");
                Double vat = rs1.getDouble("vat");
                String currency = rs1.getString("CURRENCY");

                priceObj = new Price();
                priceObj.setPriceId(priceId);
                priceObj.setStockModelId(stockModelId);
                priceObj.setVat(vat);
                priceObj.setCurrency(currency);
                priceObj.setPrice(price);
                break;
            }
            logTimeDb("Time to getPriceForSaleRetail, stockModelId: " + stockModelId + " pricePolicy " + pricePolicy, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getPriceForSaleRetail, stockModelId: " + stockModelId + " pricePolicy " + pricePolicy);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return priceObj;
    }

    public double getDiscountForHandset(String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        double discountAmount = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from cm_pre.kit_prepaid_promotion where product_code = ? and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setString(1, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                discountAmount = rs.getDouble("discount_amount");
                break;
            }
            logger.info("End getDiscountForHandset productCode: " + productCode + ", discountAmount: " + discountAmount + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getDiscountForHandset productCode: ").
                    append(productCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return discountAmount;
        }
    }

    public int expStockTotal(Long staffId, Long stockModelId, Long quantity) {
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
                    + " where owner_id = ? and owner_type = 2 and stock_model_id = ? and state_id = 1 and quantity >= ? "
                    + " and quantity_issue >= ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, quantity);
            ps.setLong(2, quantity);
            ps.setLong(3, staffId);
            ps.setLong(4, stockModelId);
            ps.setLong(5, quantity);
            ps.setLong(6, quantity);
            result = ps.executeUpdate();
            logger.info("End expStockTotal staffId " + staffId + "stockModelId: " + stockModelId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR expStockTotal: ").
                    append(sql).append("\n")
                    .append(" staffId ")
                    .append(staffId)
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

    public int updateSeialExp(Long staffId, Long stockModelId, String serial) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update sm.stock_handset set status = 0 where stock_model_id = ? "
                    + " and owner_type = 2 and owner_id = ? and  serial = ? and status = 7 ";//status = 7 because last time already lock status
            ps = connection.prepareStatement(sql);
            ps.setLong(1, stockModelId);
            ps.setLong(2, staffId);
            ps.setString(3, serial);
            result = ps.executeUpdate();
            logger.info("End updateSeialExp staffId " + staffId + "stockModelId: " + stockModelId + ", serial: " + serial
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateSeialExp: ").
                    append(sql).append("\n")
                    .append(" staffId ")
                    .append(staffId)
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

    public boolean checkBIOfDocument(Long custId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from cm_pre.customer where cust_id = ? and image_name_no1 is not null and image_name is not null and image_name_no2 is not null";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, custId != null ? custId.longValue() : 0L);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkBIOfDocument custId " + custId + " result " + result + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(this.loggerLabel).append(new Date()).append("\nERROR checkBIOfDocument: ").append(sql).append("\n").append(" custId ").append(custId);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
        }
        return result;
    }

    public boolean checkValidProfileCorporeate(Long custId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pos");
            sql = "select * from corporate_info where cust_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, custId != null ? custId.longValue() : 0L);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkBIOfDocument custId " + custId + " result " + result + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(this.loggerLabel).append(new Date()).append("\nERROR checkBIOfDocument: ").append(sql).append("\n").append(" custId ").append(custId);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
        }
        return result;
    }

    public String getEnterpriseWallet(long kitBatchId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        String isdnWallet = "";
        try {
            connection = getConnection("cm_pre");
            sql = "select enterprise_wallet from kit_batch_info where kit_batch_id=?";
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                isdnWallet = rs.getString("enterprise_wallet");
                break;
            }
            logger.info("End getEnterpriseWallet  kitBatchId" + kitBatchId + " result " + isdnWallet + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(this.loggerLabel).append(new Date()).append("\nERROR getEnterpriseWallet: ")
                    .append(sql).append("\n")
                    .append(" kitBatchId ")
                    .append(kitBatchId)
                    .append(" isdnWallet ")
                    .append(isdnWallet);
            logger.error(br + ex.toString());
            return "";
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
        }
        return isdnWallet;
    }

    public int updateEnterpriseIsdnForBatch(long kitBatchId, String newIsdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        int result = 0;
        try {
            connection = getConnection("cm_pre");
            sql = "update kit_batch_info set enterprise_wallet =? where kit_batch_id in (\n"
                    + "select kit_batch_id from kit_batch_info where extend_from_kit_batch_id in (\n"
                    + "select extend_from_kit_batch_id from kit_batch_info where kit_batch_id =?)\n"
                    + "union\n"
                    + "select extend_from_kit_batch_id from kit_batch_info where kit_batch_id =?\n"
                    + ") and (enterprise_wallet  is null or enterprise_wallet <> ? )";
            ps = connection.prepareStatement(sql);
            ps.setString(1, newIsdn);
            ps.setLong(2, kitBatchId);
            ps.setLong(3, kitBatchId);
            ps.setString(4, newIsdn);
            result = ps.executeUpdate();
            logger.info("End updateEnterpriseIsdnForBatch kitBatchId " + kitBatchId + " newIsdn " + newIsdn + " result " + result + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(this.loggerLabel).append(new Date()).append("\nERROR updateEnterpriseIsdnForBatch: ").append(sql).append("\n").append(" kitBatchId ").append(kitBatchId);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
        }
        return result;
    }

    public String[] getIsdnWallet(String staffCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String[] isdnWallet = null;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "select a.isdn_wallet isdn_wallet_staff,b.isdn_wallet isdn_wallet_owner from sm.staff a, sm.staff  b \n"
                    + "where a.staff_owner_id = b.staff_id(+) and a.staff_code=?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffCode);
            rs = ps.executeQuery();
            isdnWallet = new String[2];
            while (rs.next()) {
                isdnWallet[0] = rs.getString("isdn_wallet_staff");
                isdnWallet[1] = rs.getString("isdn_wallet_owner");
                break;
            }
            logger.info("End getIsdnWallet: " + staffCode + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getIsdnWallet ").append(staffCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            return null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return isdnWallet;
        }
    }

    public String getParamValue(String paramCode) {
        ResultSet rs1 = null;
        Connection connection = null;
        String sqlMo = " select param_value from AP_PARAM  where param_code = ? and status =1 ";
        PreparedStatement psMo = null;
        String values = "";
        try {
            connection = getConnection("cm_pre");
            psMo = connection.prepareStatement(sqlMo);
            psMo.setString(1, paramCode);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                values = rs1.getString("param_value");
            }
            logger.info("Time to getParamValue paramCode " + paramCode);
        } catch (Exception ex) {
            logger.error("ERROR getParamValue paramCode " + paramCode + " error " + ex.getMessage());
            values = "";
            ex.printStackTrace();
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return values;
    }

    public int insertBonusConnectKit(String staffCode, String productCode, String isdn, long actionAudit, int channelType, String actionCode,
            String serial, Date createTime, int prepaidMonth, double bonusForCustomer, double bonusForPrepaid, Long subProfileId) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp1");
            sql = "INSERT INTO bonus_connect_kit (action_audit_id,isdn,create_time,create_staff,productcode,serial,"
                    + "count_process,last_process,channel_type_id,action_code,prepaid_month,bonus_for_customer,bonus_for_prepaid, sub_profile_id) \n"
                    + "VALUES(?,?,?,?,?,?,0,null,?,?,?,?,?,?) ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAudit);
            ps.setString(2, isdn);
            ps.setTimestamp(3, new Timestamp(createTime.getTime()));
            ps.setString(4, staffCode);
            ps.setString(5, productCode);
            ps.setString(6, serial);
            ps.setInt(7, channelType);
            ps.setString(8, actionCode);
            if (prepaidMonth > 0) {
                ps.setInt(9, prepaidMonth);
            } else {
                ps.setString(9, "");
            }
            if (bonusForCustomer > 0) {
                ps.setDouble(10, bonusForCustomer);
            } else {
                ps.setString(10, "");
            }
            if (bonusForPrepaid > 0) {
                ps.setDouble(11, bonusForPrepaid);
            } else {
                ps.setString(11, "");
            }
            ps.setLong(12, subProfileId);
            result = ps.executeUpdate();
            logger.info("End insertBonusConnectKit isdn " + isdn + " actionAuditId " + actionAudit
                    + " staffCode " + staffCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertBonusConnectKit: ").
                    append(sql).append("\n")
                    .append(" msisdn ")
                    .append(isdn)
                    .append(" actionAuditId ")
                    .append(actionAudit)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public List<KitBatch> getListKitBatchAddon(Long kitBatchId, double limitFee) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "SSELECT   a.*  FROM   kit_batch_detail a ,  product.product_connect_kit b WHERE  a.kit_batch_id =? and  a.result_code = '0'\n"
                    + "and a.addon =b.product_code\n"
                    + "and b.money_fee >= ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            ps.setDouble(2, limitFee);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long tmpKitBatchId = rs.getLong("kit_batch_id");
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String stateOfRecord = rs.getString("state_of_record");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(tmpKitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setStateOfRecord(stateOfRecord);
                kitBatch.setCustId(rs.getLong("cust_id"));
                kitBatch.setProductAddOn(rs.getString("addon"));
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchDetailElite: kitBatchId" + kitBatchId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getListKitBatchDetailElite ").
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

    public List<KitBatch> getListKitBatchAddonAllWithOutElite(Long kitBatchId, double limitFee) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "SELECT   *\n"
                    + "  FROM   kit_batch_detail a, product.product_connect_kit b\n"
                    + " WHERE   a.result_code = '0'\n"
                    + "         AND a.kit_batch_id IN\n"
                    + "                    (SELECT   kit_batch_id\n"
                    + "                       FROM   kit_batch_info\n"
                    + "                      WHERE   kit_batch_id = ?\n"
                    + "                              OR extend_from_kit_batch_id = ?\n"
                    + "                                AND result_code = '0')\n"
                    + "         AND a.addon = b.product_code\n"
                    + "         AND b.money_fee >= ?\n"
                    + "         AND a.product_code NOT IN\n"
                    + "                    (SELECT   product_code\n"
                    + "                       FROM   product.product_connect_kit\n"
                    + "                      WHERE   vip_product = 1 AND status = 1\n"
                    + "                              ) OR product_code = 'DATA_SIM' ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            ps.setLong(2, kitBatchId);
            ps.setDouble(3, limitFee);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long tmpKitBatchId = rs.getLong("kit_batch_id");
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String stateOfRecord = rs.getString("state_of_record");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(tmpKitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setStateOfRecord(stateOfRecord);
                kitBatch.setCustId(rs.getLong("cust_id"));
                kitBatch.setProductAddOn(rs.getString("addon"));
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchDetailElite: kitBatchId" + kitBatchId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getListKitBatchDetailElite ").
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

    public Customer getCorpCustomerByCustId(Long custId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Customer customer = null;
        String sql = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pos");
            sql = " select * from corporate_info where cust_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, custId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String subName = rs.getString("corp_name");
                String repreCustIdNo = rs.getString("repre_id_no");
                String frontImageUrl = rs.getString("img_repre_front_bi");
                String backImageUrl = rs.getString("img_repre_backside_bi");
                String formImageUrl = rs.getString("img_contract");
                customer = new Customer();
                customer.setSubName(subName);
                customer.setIdNo(repreCustIdNo);
                customer.setFrontImageUrl(frontImageUrl);
                customer.setBackImageUrl(backImageUrl);
                customer.setFormImageUrl(formImageUrl);
                customer.setBusType("CAMP");
                break;
            }
            logger.info("End getCorpCustomerByCustId with custId: " + custId + "time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getCorpCustomerByCustId: ------ custId: ").append(custId).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return customer;
        }
    }

    public boolean checkValidOla(String isdn) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        isdn = isdn.startsWith("258") ? isdn : "258" + isdn;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbElite");
            sql = " select  * from branch_promotion_sub where  isdn = ? and expire_time > sysdate";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = true;
                break;
            }
            logger.info("End checkValidOla with custId: " + isdn + "time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkValidOla: ------ custId: ").append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            return false;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public List<String> getValidPricePlan(double limitFee) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long startTime = System.currentTimeMillis();
        List<String> listRs = new ArrayList<String>();
        try {
            connection = getConnection("cm_pre");
            sql = " SELECT   CASE\n"
                    + "             WHEN is_product = 1 THEN pp_buy_more\n"
                    + "             WHEN is_product = 0 THEN vas_price_plan\n"
                    + "         END\n"
                    + "             pp_code\n"
                    + "  FROM   product.product_connect_kit\n"
                    + " WHERE   (is_product = 1 AND  vip_product = 1)\n"
                    + "         OR (is_product = 0 AND vas_priority IS NOT NULL)\n"
                    + "           AND money_fee >= ?\n"
                    + "UNION\n"
                    + "SELECT   vas_pp_auto AS pp_code\n"
                    + "  FROM   product.product_connect_kit\n"
                    + " WHERE   is_product = 0 AND vas_priority IS NOT NULL AND money_fee >= ? ";
            ps = connection.prepareStatement(sql);
            ps.setDouble(1, limitFee);
            ps.setDouble(2, limitFee);
            rs = ps.executeQuery();
            while (rs.next()) {
                if (rs.getString("pp_Code") != null) {
                    listRs.add(rs.getString("pp_Code"));
                }
            }

            logger.info("End getValidPricePlan with custId: " + listRs.size() + "time: "
                    + (System.currentTimeMillis() - startTime));
            return listRs;
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getValidPricePlan").append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
            return null;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
        }
    }

    public List<KitBatch> getListKitBatchDetailNOTElite(Long kitBatchId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "select * from kit_batch_detail where result_code = '0' and kit_batch_id in (select kit_batch_id from kit_batch_info "
                    + "where kit_batch_id = ? or extend_from_kit_batch_id = ? and result_code = '0') and (product_code not in \n"
                    + " (select product_code from product.product_connect_kit where vip_product = 1 and status = 1) "
                    + "or product_code in (select vas_code from product.product_add_on where status = 1)) ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            ps.setLong(2, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long tmpKitBatchId = rs.getLong("kit_batch_id");
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String stateOfRecord = rs.getString("state_of_record");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(tmpKitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setStateOfRecord(stateOfRecord);
                kitBatch.setCustId(rs.getLong("cust_id"));
                kitBatch.setIsConnectNew(rs.getInt("input_type") == 1);
                kitBatch.setProductAddOn(rs.getString("addon"));

                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchDetailElite: kitBatchId" + kitBatchId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getListKitBatchDetailElite ").
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

    public List<KitBatch> getListKitBatchDetailAddCUG(Long kitBatchId, double limitMOneyAddon) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "SELECT   *\n"
                    + "  FROM   kit_batch_detail\n"
                    + " WHERE   result_code = '0' AND kit_batch_id = ?\n"
                    + "         AND (product_code IN\n"
                    + "                      (SELECT   product_code\n"
                    + "                         FROM   product.product_connect_kit\n"
                    + "                        WHERE       vip_product = 1\n"
                    + "                                AND status = 1\n"
                    + "                                AND product_code <> 'DATA_SIM'\n"
                    + "                                AND input_type = 1)\n"
                    + "              OR addon IN\n"
                    + "                        (SELECT   product_code\n"
                    + "                           FROM   product.product_connect_kit\n"
                    + "                          WHERE       status = 1\n"
                    + "                                  AND money_fee >= ?\n"
                    + "                                  AND vas_priority IS NOT NULL))";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            ps.setDouble(2, limitMOneyAddon);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long tmpKitBatchId = rs.getLong("kit_batch_id");
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String stateOfRecord = rs.getString("state_of_record");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(tmpKitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setStateOfRecord(stateOfRecord);
                kitBatch.setCustId(rs.getLong("cust_id"));
                kitBatch.setIsConnectNew(rs.getInt("input_type") == 1);
                kitBatch.setProductAddOn(rs.getString("addon"));
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchDetailElite: kitBatchId" + kitBatchId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getListKitBatchDetailElite ").
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

    public List<KitBatch> getListKitBatchDetailCheckOCS(Long kitBatchId, double limitMOneyAddon) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "SELECT   *\n"
                    + "  FROM   kit_batch_detail\n"
                    + " WHERE       result_code = '0'\n"
                    + "         AND kit_batch_id = ?\n"
                    + "         AND input_type = 0\n"
                    + "         AND (addon IS NULL\n"
                    + "              OR addon NOT IN (SELECT   product_code\n"
                    + "                                 FROM   product.product_connect_kit\n"
                    + "                                WHERE   status = 1 AND money_fee >= ?))";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            ps.setDouble(2, limitMOneyAddon);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long tmpKitBatchId = rs.getLong("kit_batch_id");
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String stateOfRecord = rs.getString("state_of_record");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(tmpKitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setStateOfRecord(stateOfRecord);
                kitBatch.setCustId(rs.getLong("cust_id"));
                kitBatch.setIsConnectNew(rs.getInt("input_type") == 1);
                kitBatch.setProductAddOn(rs.getString("addon"));
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchDetailElite: kitBatchId" + kitBatchId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getListKitBatchDetailElite ").
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

    public List<KitBatch> getAllKitBatchDetailBeLongCustomer(Long kitBatchId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "SELECT   *\n"
                    + "  FROM   kit_batch_detail\n"
                    + " WHERE   result_code = '0'\n"
                    + "         AND kit_batch_id IN\n"
                    + "                    (SELECT   kit_batch_id\n"
                    + "                       FROM   kit_batch_info\n"
                    + "                      WHERE   (extend_from_kit_batch_id IN\n"
                    + "                                       (SELECT   extend_from_kit_batch_id\n"
                    + "                                          FROM   kit_batch_info\n"
                    + "                                         WHERE   kit_batch_id = ?)\n"
                    + "                               OR (kit_batch_id IN\n"
                    + "                                           (SELECT   extend_from_kit_batch_id\n"
                    + "                                              FROM   kit_batch_info\n"
                    + "                                             WHERE   kit_batch_id = ?))\n"
                    + "                                 AND status = 1\n"
                    + "                                 AND total_success > 0))\n"
                    + "UNION\n"
                    + "SELECT   *\n"
                    + "  FROM   kit_batch_detail\n"
                    + " WHERE   result_code = '0' AND kit_batch_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            ps.setLong(2, kitBatchId);
            ps.setLong(3, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long tmpKitBatchId = rs.getLong("kit_batch_id");
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String stateOfRecord = rs.getString("state_of_record");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(tmpKitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setStateOfRecord(stateOfRecord);
                kitBatch.setCustId(rs.getLong("cust_id"));
                kitBatch.setIsConnectNew(rs.getInt("input_type") == 1);
                kitBatch.setProductAddOn(rs.getString("addon"));
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getListKitBatchDetailElite: kitBatchId" + kitBatchId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getListKitBatchDetailElite ").
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

    public List<KitBatch> getAllKitBatchExtendBeLongCustomer(Long kitBatchId) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        List<KitBatch> lstKitBatch = new ArrayList<KitBatch>();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            String sql = "SELECT   isdn, kit_batch_id,'' serial, '' product_code, '' state_of_record,'' cust_id, 0 as input_type , null as addon \n"
                    + "  FROM   kit_batch_extend\n"
                    + " WHERE   kit_batch_id IN (SELECT   extend_from_kit_batch_id\n"
                    + "                            FROM   kit_batch_info\n"
                    + "                           WHERE   kit_batch_id = ?)\n"
                    + "         AND result_code = '0' ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long tmpKitBatchId = rs.getLong("kit_batch_id");
                String serial = rs.getString("serial");
                String isdn = rs.getString("isdn");
                String productCode = rs.getString("product_code");
                String stateOfRecord = rs.getString("state_of_record");
                KitBatch kitBatch = new KitBatch();
                kitBatch.setSerial(serial);
                kitBatch.setKitBatchId(tmpKitBatchId);
                kitBatch.setIsdn(isdn);
                kitBatch.setProductCode(productCode);
                kitBatch.setStateOfRecord(stateOfRecord);
                kitBatch.setCustId(rs.getLong("cust_id"));
                kitBatch.setIsConnectNew(rs.getInt("input_type") == 1);
                kitBatch.setProductAddOn(rs.getString("addon"));
                lstKitBatch.add(kitBatch);

            }
            logger.info("End getAllKitBatchExtendBeLongCustomer: kitBatchId" + kitBatchId + " time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getAllKitBatchExtendBeLongCustomer ").
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

    public int deleteSubprofileInfo(Long subProfileId) {
        Connection connection = null;
        PreparedStatement ps = null;

        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "delete cm_pre.sub_profile_info where sub_profile_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, subProfileId);

            result = ps.executeUpdate();
            logger.info("End deleteSubprofileInfo subProfileId " + subProfileId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            StringBuilder brBuilder = new StringBuilder();
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR deleteSubprofileInfo: ").
                    append(sql).append("\n")
                    .append(" subProfileId ")
                    .append(subProfileId)
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

    public double getTotalKitBatchMount(long kitBatchId) {
        Connection connection = null;
        PreparedStatement ps = null;
        String sql = "";
        ResultSet rs = null;
        double result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pre");
            sql = "select * from kit_batch_info where kit_batch_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                result = rs.getDouble("total_money");
            }
            logger.info("End getTotalKitBatchMount kitBatchId " + kitBatchId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            StringBuilder brBuilder = new StringBuilder();
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR getTotalKitBatchMount: ").
                    append(sql).append("\n")
                    .append(" kitBatchId ")
                    .append(kitBatchId)
                    .append(" result ")
                    .append(result);
            logger.error(brBuilder + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
}
