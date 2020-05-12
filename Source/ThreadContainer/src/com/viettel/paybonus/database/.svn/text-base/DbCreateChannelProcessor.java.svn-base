/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.RequestChannel;
import com.viettel.paybonus.obj.Staff;
import com.viettel.paybonus.obj.SubscriberInfo;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.threadfw.database.DbProcessorAbstract;
import static com.viettel.threadfw.database.DbProcessorAbstract.QUERY_TIMEOUT;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.PoolStore;
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
public class DbCreateChannelProcessor extends DbProcessorAbstract {

//    private Logger logger;
    private String loggerLabel = DbCreateChannelProcessor.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
    private PoolStore poolStore;
    private String dbNameCofig;
    private String sqlDeleteMo = "update sm.request_channel set process_date = sysdate, status = ?, contract_status = ?, description = ? where id = ?";
    
    public DbCreateChannelProcessor() throws SQLException, Exception {
        this.logger = Logger.getLogger(loggerLabel);
        dbNameCofig = "dbsm";
        poolStore = new PoolStore(dbNameCofig, logger);
    }
    
    public DbCreateChannelProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
        RequestChannel record = new RequestChannel();
        long timeSt = System.currentTimeMillis();
        try {
            record.setId(rs.getLong("id"));
            record.setRequestUserIsdn(rs.getString("request_user_isdn"));
            record.setStaffId(rs.getLong("staff_id"));
            record.setChannelName(rs.getString("channel_name"));
            record.setChannelIsdn(rs.getString("channel_isdn"));
            record.setCreateDate(rs.getTimestamp("create_date"));
            record.setCreateObject(rs.getString("create_object"));
            record.setStatus(rs.getLong("status"));
            record.setChannelPrefix(rs.getString("channel_prefix"));
            record.setChannelWallet(rs.getString("channel_wallet"));
            record.setIsdnWallet(rs.getString("isdn_wallet"));
            record.setParentIdWallet(rs.getLong("parent_id_wallet"));
            record.setTypeAction(rs.getString("type_action"));
            record.setX(rs.getString("x"));
            record.setY(rs.getString("y"));
            record.setImgUrl(rs.getString("img_url"));
            record.setImgUrl1(rs.getString("img_url1"));
            record.setImgUrl2(rs.getString("img_url2"));
            record.setImgPath(rs.getString("img_path"));
            record.setBtsCode(rs.getString("bts_code"));
            record.setImei(rs.getString("imei"));
            record.setExistIsdnWallet(rs.getString("exist_isdn_wallet"));
            record.setAnotherPhone(rs.getString("another_phone"));
            record.setDistrict(rs.getString("district"));
            record.setPrecint(rs.getString("precinct"));
            record.setStreet(rs.getString("street_name"));
            record.setSerial(rs.getString("serial"));
            record.setLastUpdateKey(rs.getString("last_update_key"));
            record.setAreaManageId(rs.getLong("area_manage_id"));
            record.setImgUrl3(rs.getString("img_url3"));
            record.setbINumber(rs.getString("bi_number"));
            record.setNuitNumber(rs.getString("nuit_number"));
            record.setEquipmentInfo(rs.getString("equipment_info"));
            record.setDescription("Start Processing");
            record.setResultCode("0");
        } catch (Exception ex) {
            logger.error("ERROR parse MoRecord");
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
                RequestChannel sd = (RequestChannel) rc;
                batchId = sd.getBatchId();
                ps.setLong(1, sd.getStatus());
                ps.setLong(2, sd.getContractStatus());
                ps.setString(3, sd.getDescription());
                ps.setLong(4, sd.getId());
                
                ps.addBatch();
            }
            return ps.executeBatch();
        } catch (Exception ex) {
            logger.error("ERROR deleteQueue REQUEST_CHANNEL batchid " + batchId, ex);
            logger.error(AppManager.logException(timeStart, ex));
            return null;
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            logTimeDb("Time to deleteQueue REQUEST_CHANNEL, batchid " + batchId, timeStart);
        }
    }
    
    public String getChannelType(String prefixObjectCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder btBuilder = new StringBuilder();
        StringBuilder result = new StringBuilder();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from channel_type where prefix_object_code = ? and object_type = '2'  and is_vt_unit = '2'";
            ps = connection.prepareStatement(sql);
            ps.setString(1, prefixObjectCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long channelTypeId = rs.getLong("channel_type_id");
                result.append(channelTypeId).append("|");
                String discountPolicyDefault = rs.getString("discount_policy_default");
                result.append(discountPolicyDefault).append("|");
                String pricePolicyDefault = rs.getString("price_policy_default");
                result.append(pricePolicyDefault);
            }
            logger.info("End getChannelType result:  " + result + " prefix: " + prefixObjectCode
                    + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            btBuilder.setLength(0);
            btBuilder.append(loggerLabel).append(new Date()).append("\nERROR getChannelType ---- result ").
                    append(result).append(" Message: ").
                    append(ex.getMessage());
            logger.error(btBuilder + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result.toString();
        }
    }
    
    public String getStaffInfo(Long staffId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder btBuilder = new StringBuilder();
        StringBuilder result = new StringBuilder();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from Staff where staff_Id = ? and status = 1 ";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, staffId);
            rs = ps.executeQuery();
            while (rs.next()) {
                Long shopId = rs.getLong("shop_id");
                result.append(shopId).append("|");
                String staffCode = rs.getString("staff_code");
                result.append(staffCode);
            }
            logger.info("End getStaffInfo result:  " + result + " staffId: " + staffId
                    + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            btBuilder.setLength(0);
            btBuilder.append(loggerLabel).append(new Date()).append("\nERROR getStaffInfo ---- result ").
                    append(result).append(" Message: ").
                    append(ex.getMessage());
            logger.error(btBuilder + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result.toString();
        }
    }
    
    public String getShopInfo(Long shopId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder btBuilder = new StringBuilder();
        StringBuilder result = new StringBuilder();
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from Shop where shop_Id = ? and status = 1";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, shopId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String province = rs.getString("province");
                result.append(province);
            }
            logger.info("End getShopInfo result:  " + result + " shopId: " + shopId
                    + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            btBuilder.setLength(0);
            btBuilder.append(loggerLabel).append(new Date()).append("\nERROR getShopInfo ---- result ").
                    append(result).append(" Message: ").
                    append(ex.getMessage());
            logger.error(btBuilder + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result.toString();
        }
    }
    
    public String getProvinceReference(String areaCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder btBuilder = new StringBuilder();
        String provinceReference = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from area where area_code = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, areaCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                provinceReference = rs.getString("province_refefence");
            }
            logger.info("End getProvinceReference provinceReference:  " + provinceReference + " areaCode: " + areaCode
                    + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            btBuilder.setLength(0);
            btBuilder.append(loggerLabel).append(new Date()).append("\nERROR getProvinceReference ---- areaCode ").
                    append(areaCode).append(" Message: ").
                    append(ex.getMessage());
            logger.error(btBuilder + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return provinceReference;
        }
    }
    
    public String getStaffCodeSeqIsNotVt(String prefixObjectCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder btBuilder = new StringBuilder();
        String staffCode = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select to_char(max(to_number(substr(staff_code, ?)))) as staff_code from staff where lower(staff_code) like ?";
            ps = connection.prepareStatement(sql);
            ps.setInt(1, (prefixObjectCode.trim().length() + 1));
            ps.setString(2, "" + prefixObjectCode.trim().toLowerCase() + "%");
            rs = ps.executeQuery();
            while (rs.next()) {
                staffCode = rs.getString("staff_code");
            }
            Long seq = 1L;
            if (staffCode == null || staffCode.trim().equals("")) {
                staffCode = prefixObjectCode.trim() + String.format("%05d", 1L);
            } else {
                seq = Long.parseLong(staffCode) + 1;
                staffCode = prefixObjectCode.trim() + String.format("%05d", seq);
            }
            logger.info("End getStaffCodeSeqIsNotVt prefixObjectCode:  " + prefixObjectCode + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            btBuilder.setLength(0);
            btBuilder.append(loggerLabel).append(new Date()).append("\nERROR getStaffCodeSeqIsNotVt ---- prefixObjectCode ").
                    append(prefixObjectCode).
                    append(" Message: ").
                    append(ex.getMessage());
            logger.error(btBuilder + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return staffCode;
        }
    }
    
    public SubscriberInfo getSubscriberInfo(String channelIsdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder btBuilder = new StringBuilder();
        SubscriberInfo subscriberInfo = null;
        long startTime = System.currentTimeMillis();
        String sql = "";
        try {
            connection = getConnection("cm_pre");
            sql = "SELECT   cus.name custname,\n"
                    + "         cus.id_type idtype,\n"
                    + "         NVL (cus.id_no, cus.bus_permit_no) idno,\n"
                    + "         cus.id_issue_date idissuedate,\n"
                    + "         cus.id_issue_place idissueplace,\n"
                    + "         cus.address custaddress,\n"
                    + "         cus.province province,\n"
                    + "         cus.district district,\n"
                    + "         cus.precinct precinct,\n"
                    + "         cus.birth_date birthdate,\n"
                    + "         sub.serial serial,\n"
                    + "         sub.act_status actstatus,\n"
                    + "         '1' subtype,\n"
                    + "         cus.cust_id custid,\n"
                    + "         sub.is_using_wallet isusingwallet\n"
                    + "  FROM   cm_pre.customer cus, cm_pre.sub_mb sub\n"
                    + " WHERE       cus.cust_id = sub.cust_id\n"
                    + "         AND cus.status = 1\n"
                    + "         AND sub.status = 2\n"
                    + "         AND sub.isdn = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, channelIsdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                subscriberInfo = new SubscriberInfo();
                subscriberInfo.setCustName(rs.getString("custname"));
                subscriberInfo.setIdType(rs.getString("idtype"));
                subscriberInfo.setIdNo(rs.getString("idno"));
                subscriberInfo.setIdIssueDate(rs.getTimestamp("idissuedate"));
                subscriberInfo.setIdIssuePlace(rs.getString("idissueplace"));
                subscriberInfo.setCustAddress(rs.getString("custaddress"));
                subscriberInfo.setProvince(rs.getString("province"));
                subscriberInfo.setDistrict(rs.getString("district"));
                subscriberInfo.setPrecinct(rs.getString("precinct"));
                subscriberInfo.setBirthDate(rs.getTimestamp("birthdate"));
                subscriberInfo.setSerial(rs.getString("serial"));
                subscriberInfo.setActStatus(rs.getString("actstatus"));
                subscriberInfo.setSubType(rs.getString("subtype"));
                subscriberInfo.setCustId(rs.getLong("custId"));
                subscriberInfo.setIsUsingWallet(rs.getLong("isusingwallet"));
                
                break;
            }
            if (subscriberInfo == null) {
                closeStatement(ps);
                closeResultSet(rs);
                closeConnection(connection);
                
                connection = getConnection("cm_pos");
                sql = "SELECT   cus.name custname,\n"
                        + "         cus.id_type idtype,\n"
                        + "         NVL (cus.id_no, cus.bus_permit_no) idno,\n"
                        + "         cus.id_issue_date idissuedate,\n"
                        + "         cus.id_issue_place idissueplace,\n"
                        + "         cus.address custaddress,\n"
                        + "         cus.province province,\n"
                        + "         cus.district district,\n"
                        + "         cus.precinct precinct,\n"
                        + "         cus.birth_date birthdate,\n"
                        + "         sub.serial serial,\n"
                        + "         '2' subtype,\n"
                        + "         cus.cust_id custid,\n"
                        + "         sub.is_using_wallet isusingwallet\n"
                        + "  FROM   cm_pos.customer cus, cm_pos.sub_mb sub, cm_pos.contract co\n"
                        + " WHERE       co.contract_id = sub.contract_id\n"
                        + "         AND co.cust_id = cus.cust_id\n"
                        + "         AND co.status = 2\n"
                        + "         AND cus.status = 1\n"
                        + "         AND sub.status = 2\n"
                        + "         AND sub.isdn = ?";
                
                ps = connection.prepareStatement(sql);
                ps.setString(1, channelIsdn);
                rs = ps.executeQuery();
                while (rs.next()) {
                    subscriberInfo = new SubscriberInfo();
                    subscriberInfo.setCustName(rs.getString("custname"));
                    subscriberInfo.setIdType(rs.getString("idtype"));
                    subscriberInfo.setIdNo(rs.getString("idno"));
                    subscriberInfo.setIdIssueDate(rs.getTimestamp("idissuedate"));
                    subscriberInfo.setIdIssuePlace(rs.getString("idissueplace"));
                    subscriberInfo.setCustAddress(rs.getString("custaddress"));
                    subscriberInfo.setProvince(rs.getString("province"));
                    subscriberInfo.setDistrict(rs.getString("district"));
                    subscriberInfo.setPrecinct(rs.getString("precinct"));
                    subscriberInfo.setBirthDate(rs.getTimestamp("birthdate"));
                    subscriberInfo.setSerial(rs.getString("serial"));
                    subscriberInfo.setSubType(rs.getString("subtype"));
                    subscriberInfo.setCustId(rs.getLong("custId"));
                    subscriberInfo.setIsUsingWallet(rs.getLong("isusingwallet"));
                    
                    break;
                }
            }
            logger.info("End getSubscriberInfo channelIsdn:  " + channelIsdn + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            btBuilder.setLength(0);
            btBuilder.append(loggerLabel).append(new Date()).append("\nERROR getSubscriberInfo ---- channelIsdn ").
                    append(channelIsdn).
                    append(" Message: ").
                    append(ex.getMessage());
            logger.error(btBuilder + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return subscriberInfo;
        }
    }
    
    public boolean checkIsdnWallet(String isdnWallet) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        boolean result = false;
        try {
            connection = getConnection("dbsm");
            ps = connection.prepareStatement(
                    "select * from Staff where isdn_Wallet = ? and status not in (0)");
            ps.setString(1, isdnWallet);
            rs = ps.executeQuery();
            while (rs.next()) {
                String channel = rs.getString("staff_code");
                if (channel != null && channel.trim().length() > 0) {
                    result = true;
                }
                break;
            }
            logTimeDb("Time to checkIsdnWallet " + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkIsdnWallet defaul return false");
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
    
    public int insertLogCallWsWallet(String isdn, Long ewalletId, String actionType, Long statusProcess,
            Long numberProcess, String description, String customerName, Date doB, String idNo,
            String channelType, Long parentId, String idIssuePlace, Date idIssueDate) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "INSERT INTO sm.log_call_ws_wallet (ISDN,EWALLET_ID,ACTION_TYPE,STATUS_PROCESS,NUMBER_PROCESS,"
                    + "INSERT_DATE,DESCRIPTION,ID,CUSTOMER_NAME,DOB,ID_NO,CHANNEL_TYPE,PARENT_ID,IDISSUEPLACE,IDISSUEDATE) \n"
                    + "VALUES(?,?,?,?,?,sysdate,?,log_call_ws_wallet_seq.nextval,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setLong(2, ewalletId);
            ps.setString(3, actionType);
            ps.setLong(4, statusProcess);
            ps.setLong(5, numberProcess);
            ps.setString(6, description);
            ps.setString(7, customerName);
            ps.setDate(8, new java.sql.Date(doB.getTime()));
            ps.setString(9, idNo);
            ps.setString(10, channelType);
            ps.setLong(11, parentId);
            ps.setString(12, idIssuePlace);
            ps.setDate(13, new java.sql.Date(idIssueDate.getTime()));
            
            result = ps.executeUpdate();
            logger.info("End insertLogCallWsWallet isdn " + isdn + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertLogCallWsWallet: ").
                    append(sql).append("\n")
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
    
    public int insertStaffLocation(String staffId, String staffCode, String staffOwnerId, Date lastUpdateTime, String channelTypeId,
            String x, String y, String imgUrl, String imgUrl1, String imgUrl2, String imgPath) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbapp2");
            sql = "INSERT INTO Staff_Location(Staff_Id, Staff_Code, Staff_Owner_Id, Last_Update_Time, Channel_Type_Id, X, Y, Img_Url, Img_Url1, Img_Url2, Img_Path) \n"
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staffId);
            ps.setString(2, staffCode);
            ps.setString(3, staffOwnerId);
            ps.setDate(4, new java.sql.Date(lastUpdateTime.getTime()));
            ps.setString(5, channelTypeId);
            ps.setString(6, x);
            ps.setString(7, y);
            ps.setString(8, imgUrl);
            ps.setString(9, imgUrl1);
            ps.setString(10, imgUrl2);
            ps.setString(11, imgPath);
            
            result = ps.executeUpdate();
            logger.info("End insertStaffLocation staffCode " + staffCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertStaffLocation: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
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
    
    public int insertStaff(Staff staff) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "insert into staff (staff_id,channel_wallet,isdn_wallet,parent_id_wallet,shop_id,staff_code,trade_name,contact_name,name,"
                    + "staff_owner_id,id_no,birthday,id_issue_date,id_issue_place,province,district,precinct,address,channel_type_id,"
                    + "discount_policy,price_policy,status,registry_date,tel,last_update_user,last_update_time,create_method,bts_code,imei_smartphone,"
                    + "another_phone,street_name,serial,last_update_key,type,area_manage_id) "
                    + "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, staff.getStaffId());
            ps.setString(2, staff.getChannelWallet());
            ps.setString(3, staff.getIsdnWallet());
            ps.setString(4, staff.getParentIdWallet());
            
            ps.setString(5, staff.getShopId());
            ps.setString(6, staff.getStaffCode());
            ps.setString(7, staff.getTradeName());
            ps.setString(8, staff.getContactName());
            ps.setString(9, staff.getName());
            ps.setString(10, staff.getStaffOwnerId());
            ps.setString(11, staff.getIdNo());
            ps.setDate(12, new java.sql.Date(staff.getBirthday().getTime()));
            ps.setDate(13, new java.sql.Date(staff.getIdIssueDate().getTime()));
            ps.setString(14, staff.getIdIssuePlace());
            ps.setString(15, staff.getProvince());
            ps.setString(16, staff.getDistrict());
            ps.setString(17, staff.getPrecinct());
            ps.setString(18, staff.getAddress());
            ps.setString(19, staff.getChannelTypeId());
            ps.setString(20, staff.getDiscountPolicy());
            ps.setString(21, staff.getPricePolicy());
            ps.setString(22, staff.getStatus());
            ps.setDate(23, new java.sql.Date(staff.getRegistryDate().getTime()));
            ps.setString(24, staff.getTel());
            ps.setString(25, staff.getLastUpdateUser());
            ps.setDate(26, new java.sql.Date(staff.getLastUpdateTime().getTime()));
            ps.setString(27, staff.getCreateMethod());
            ps.setString(28, staff.getBtsCode());
            ps.setString(29, staff.getImei());
            ps.setString(30, staff.getAnotherPhone());
            ps.setString(31, staff.getStreetName());
            ps.setString(32, staff.getSerial());
            ps.setString(33, staff.getLastUpdateKey());
            
            result = ps.executeUpdate();
            logger.info("End insertStaff staffCode " + staff.getStaffCode() + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertStaff: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staff.getStaffCode())
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
    
    public int insertStockOwnerTmp(Long channelTypeId, String code, String name, Long ownerId, Long ownerType) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "insert into Stock_Owner_Tmp (stock_id, channel_type_id, code,name,owner_id,owner_type) "
                    + "values (STOCK_OWNER_TMP_SEQ.nextval,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, channelTypeId);
            ps.setString(2, code);
            ps.setString(3, name);
            ps.setLong(4, ownerId);
            ps.setLong(5, ownerType);
            
            result = ps.executeUpdate();
            logger.info("End insertStockOwnerTmp staffCode " + code + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertStockOwnerTmp: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(code)
                    .append(" result ")
                    .append(result);
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return result;
        }
    }
    
    public int updateContractPath(String path, String staffCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder brBuilder = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            sql = "update staff set image_url = ? where status = 1 and staff_code = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, path);
            ps.setString(2, staffCode);
            result = ps.executeUpdate();
            logger.info("End updateContractPath staffCode " + staffCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(loggerLabel).append(new Date()).
                    append("\nERROR updateContractPath: ").
                    append(sql).append("\n")
                    .append(" staffCode ")
                    .append(staffCode)
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
    
    public String getAddress(String province, String district, String precinct) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder btBuilder = new StringBuilder();
        String address = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("dbsm");
            String sql = "select * from area where province = ? and district = ? and precinct = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, province);
            ps.setString(2, district);
            ps.setString(3, precinct);
            rs = ps.executeQuery();
            while (rs.next()) {
                address = rs.getString("full_name");
            }
            logger.info("End getAddress province:  " + province + "district: " + district + "precinct: " + precinct + " time " + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            btBuilder.setLength(0);
            btBuilder.append(loggerLabel).append(new Date()).append("\nERROR getAddress ---- province ").
                    append(province).
                    append(" Message: ").
                    append(ex.getMessage());
            logger.error(btBuilder + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return address;
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
            connection = getConnection("dbapp2");
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
