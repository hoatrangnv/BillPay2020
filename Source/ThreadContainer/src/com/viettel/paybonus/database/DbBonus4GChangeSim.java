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
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.DataResources;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbBonus4GChangeSim extends DbProcessorAbstract {
	
	private String loggerLabel = DbBonus4GChangeSim.class.getSimpleName() + ": ";
	private PoolStore poolStore;
	private String dbNameCofig;
	
	public DbBonus4GChangeSim() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
		poolStore = new PoolStore(dbNameCofig, logger);
	}
	
	public DbBonus4GChangeSim(String sessionName, Logger logger) throws SQLException, Exception {
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
		BonusSim4G record = new BonusSim4G();
		long timeSt = System.currentTimeMillis();
		try {
			record.setId(rs.getLong("id"));
			record.setActionAuditId(rs.getLong("action_audit_id"));
			record.setIsdn(rs.getString("isdn"));
			record.setNewSerial(rs.getString("new_serial"));
			record.setIdNo(rs.getString("id_no"));
			record.setStatus(rs.getInt("status"));
			record.setCreateTime(rs.getDate("create_time"));
			record.setStaffCode(rs.getString("staff_code"));
			record.setDuration(rs.getLong("duration"));
			record.setResultCode(rs.getString("result_code"));
			record.setDescription(rs.getString("description"));
			record.setDateProcess(rs.getDate("date_process"));
			record.setOldSerial(rs.getString("old_serial"));
			record.setOldImsi(rs.getString("old_imsi"));
			record.setChannelType(rs.getString("channel_type"));
			record.setVas_code(rs.getString("vas_code"));
			record.setBonusStatus(rs.getInt("bonus_status"));
			record.setBonusDesc(rs.getString("bonus_desc"));
			record.setBonusTime(rs.getDate("bonus_time"));
			record.setLanguage(rs.getString("ussd_loc"));
			
		} catch (Exception ex) {
			logger.error("ERROR parse BonusChangeSim4G");
			logger.error(AppManager.logException(timeSt, ex));
		}
		return record;
	}
	
	public Agent getAgentInfoByUser(String staffCode) {
		/**
		 * SELECT staff_id, isdn_wallet, channel_type_id FROM sm.staff WHERE staff_code = ? and status = 1 AND ROWNUM < 2;
		 */
		ParamList paramList = new ParamList();
		Agent agent = null;
		long timeSt = System.currentTimeMillis();
		try {
			paramList.add(new Param("STAFF_CODE", staffCode.trim().toUpperCase(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("STATUS", 1, Param.DataType.LONG, Param.IN));
//            paramList.add(new Param("channel_wallet", "", Param.OperatorType.IS_NOT_NULL, Param.DataType.CONST, Param.IN));
//            paramList.add(new Param("isdn_wallet", "", Param.OperatorType.IS_NOT_NULL, Param.DataType.CONST, Param.IN));
			paramList.add(new Param("isdn_wallet", "", Param.OperatorType.IS_NOT_NULL, Param.DataType.CONST, Param.IN));
			paramList.add(new Param("STAFF_CODE", null, Param.DataType.STRING, Param.OUT));
			paramList.add(new Param("STAFF_ID", null, Param.DataType.STRING, Param.OUT));
			paramList.add(new Param("tel", null, Param.DataType.STRING, Param.OUT));
			paramList.add(new Param("isdn_wallet", null, Param.DataType.STRING, Param.OUT));
			paramList.add(new Param("channel_type_id", null, Param.DataType.INT, Param.OUT));
			DataResources rs = poolStore.selectTable(paramList, "sm.staff");
			agent = new Agent();
			while (rs.next()) {
				String code = rs.getString("STAFF_CODE");
				String id = rs.getString("STAFF_ID");
				String isdn = rs.getString("tel");
				String isdnEmola = rs.getString("isdn_wallet");
				int channelTypeId = rs.getInt("channel_type_id");
				if (isdnEmola != null && isdnEmola.trim().length() > 0) {
					agent.setStaffCode(code);
					agent.setStaffId(Long.valueOf(id));
					agent.setIsdnWallet(isdnEmola);
					agent.setChannelTypeId(channelTypeId);
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
			paramList.add(new Param("BONUS_TYPE", 7L, Param.DataType.LONG, Param.IN));
			PoolStore.PoolResult prs = poolStore.insertTable(paramList, "EWALLET_LOG");
			logTimeDb("Time to insertEwalletLog isdn " + log.getMobile(), timeSt);
			return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
		} catch (Exception ex) {
			logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getMobile());
			logger.error(AppManager.logException(timeSt, ex));
			return -1;
		}
	}
	
	@Override
	public void updateSqlMoParam(List<Record> lrc) {
		throw new UnsupportedOperationException("Not supported yet.");
	}
	
	public int[] deleteQueueTimeout(List<String> listId) {
		return new int[0];
	}
	
	@Override
	public int[] deleteQueue(List<Record> listRecords) {
		long timeStart = System.currentTimeMillis();
		PreparedStatement ps = null;
		Connection connection = null;
		String batchId = "";
		String sqlDeleteMo = "update request_changesim_4g set bonus_status =?, bonus_desc =?, bonus_time = sysdate,data_added =?"
				+ ",add_data_status =? ,add_data_desc =?,bonus_result_code=?,imei_hs =? where id = ? ";
		try {
			connection = ConnectionPoolManager.getConnection("cm_pre");
			ps = connection.prepareStatement(sqlDeleteMo);
			for (Record rc : listRecords) {
				BonusSim4G pn = (BonusSim4G) rc;
				batchId = pn.getBatchId();
				ps.setInt(1, pn.getBonusStatus());
				ps.setString(2, pn.getBonusDesc());
				ps.setLong(3, pn.getDataValuesAdded());
				ps.setString(4, pn.getAddDataStatus());
				ps.setString(5, pn.getAddDataDesc());
				ps.setString(6, pn.getResultCode());
				ps.setString(7, pn.getImeiHS());
				ps.setLong(8, pn.getId());
				ps.addBatch();
			}
			return ps.executeBatch();
		} catch (Exception ex) {
			logger.error("ERROR update request_changesim_4g  batchid " + batchId, ex);
			logger.error(AppManager.logException(timeStart, ex));
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			logTimeDb("Time to update request_changesim_4g, batchid " + batchId, timeStart);
		}
	}
	
	@Override
	public int[] insertQueueHis(List<Record> listRecords) {
		return new int[0];
	}
	
	@Override
	public int[] insertQueueOutput(List<Record> listRecords) {
		return new int[0];
	}
	
	@Override
	public int[] updateQueueInput(List<Record> listRecords) {
		return new int[0];
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
	
	public String getProductCode(String isdn) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		String productCode = "";
		try {
			connection = getConnection("cm_pre");
			sql = "SELECT PRODUCT_CODE FROM CM_PRE.SUB_MB WHERE ISDN =? AND STATUS =2";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn);
			rs = ps.executeQuery();
			while (rs.next()) {
				productCode = rs.getString("PRODUCT_CODE");
				break;
			}
			logger.info("End getProductCode isdn " + isdn + " product code " + productCode + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR getProductCode: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(isdn);
			logger.error(br + ex.toString());
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return productCode;
		}
	}
	
	public String checkUsedHandSet4G(String isdn) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		String imei = "";
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection("hsmdm");
			sql = "select imei from sub_used_handset_4g where isdn = ? ";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn);
			rs = ps.executeQuery();
			while (rs.next()) {
				imei = rs.getString("imei");
			}
			logger.info("End checkUsedHandSet4G isdn " + isdn + " product imei " + imei + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkUsedHandSet4G: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(isdn);
			logger.error(br + ex.toString());
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return imei;
		}
	}
	
	public boolean checkIMEIReceived(String imei) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		boolean isValid = false;
		try {
			connection = getConnection("cm_pre");
			sql = "select isdn from request_Changesim_4g where substr(imei_hs,0,14) = ? and status =1 and result_code =0 "
					+ "and action_audit_id <> 0 and (bonus_result_code ='E01' or add_data_status ='0') "
					+ " union "
					+ "select isdn from waiting_bonus_changesim4g where substr(imei,0,14) = ? and status =0";
			ps = connection.prepareStatement(sql);
			ps.setString(1, imei.length() > 14 ? imei.substring(0, 14) : imei);
			ps.setString(2, imei.length() > 14 ? imei.substring(0, 14) : imei);
			rs = ps.executeQuery();
			while (rs.next()) {
				isValid = true;
				break;
			}
			logger.info("End checkUsedHandSet4G imei " + imei + " product isValid " + isValid + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkUsedHandSet4G: ").
					append(sql).append("\n")
					.append(" imei ")
					.append(imei);
			logger.error(br + ex.toString());
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return isValid;
		}
	}
	
	public boolean checkReceivedPromotion(String isdn) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		boolean isReceived = false;
		try {
			connection = getConnection("cm_pre");
			sql = "select isdn from request_Changesim_4g where isdn = ? and status =1 and result_code =0 "
					+ "and action_audit_id <> 0 and (bonus_result_code ='E01' or add_data_status ='0') ";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn);
			rs = ps.executeQuery();
			while (rs.next()) {
				isReceived = true;
				break;
			}
			logger.info("End checkReceivedPromotion isdn " + isdn + " product isReceived " + isReceived + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkReceivedPromotion: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(isdn);
			logger.error(br + ex.toString());
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return isReceived;
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

	/**
	 *
	 * @param isdn
	 * @param type 0 curent date
	 * @return
	 */
	public boolean checkUseData3G(String isdn, int month) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		boolean isValid = false;
		try {
			connection = getConnection("report");
			if (month == 0) {
				sql = "select kb_data/1024 as mb_date From rp_mobile_traffic_acc where rp_date = trunc(sysdate-1) and isdn=?";
			} else {
				sql = "select kb_data/1024 as mb_date From rp_mobile_traffic_acc where rp_date = trunc(LAST_DAY(ADD_MONTHS(sysdate-1,?))) and isdn = ?";
			}
			ps = connection.prepareStatement(sql);
			if (month == 0) {
				ps.setString(1, isdn);
			} else {
				ps.setInt(1, month);
				ps.setString(2, isdn);
			}
			
			rs = ps.executeQuery();
			while (rs.next()) {
				if (rs.getLong("mb_date") > 0) {
					isValid = true;
				}
			}
			logger.info("End checkUseData isdn " + isdn + " month " + month + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkUseData: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(isdn);
			logger.error(br + ex.toString());
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return isValid;
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
	
	public int insertWaitingBonusQueue(BonusSim4G bn) {
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		int result = 0;
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection("cm_pre");
			sql = "insert into waiting_bonus_changesim4g (action_audit_id,isdn,status,old_serial,new_serial,imei,description)"
					+ " values(?,?,?,?,?,?,?)";
			ps = connection.prepareStatement(sql);
			if (bn.getIsdn().startsWith("258")) {
				bn.setIsdn(bn.getIsdn().substring(3));
			}
			ps.setLong(1, bn.getActionAuditId());
			ps.setString(2, bn.getIsdn());
			ps.setInt(3, 1);
			ps.setString(4, bn.getOldSerial());
			ps.setString(5, bn.getNewSerial());
			ps.setString(6, bn.getImeiHS());
			ps.setString(7, "Add to queue");
			result = ps.executeUpdate();
			logger.info("End watingBonusQueue isdn " + bn.getIsdn() + " imei " + bn.getImeiHS() + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR watingBonusQueue: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(bn.getImeiHS())
					.append(" imei ")
					.append(bn.getImeiHS())
					.append(" result ")
					.append(result);
			logger.error(br + ex.toString());
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}
	
	public Customer getCustomerByIsdn(String isdn) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		Customer customer = null;
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection("cm_pre");
			String sql = "select a.cust_id,a.name,a.sex,a.birth_date,a.id_no from cm_pre.customer a, cm_pre.sub_mb b where a.cust_id = b.cust_id and b.status =2 and b.isdn = ?";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn);
			rs = ps.executeQuery();
			while (rs.next()) {
				customer = new Customer();
				customer.setSubName(rs.getString("name"));
				customer.setGender(rs.getString("sex").equals("F") ? "1" : "0");
				customer.setCustId(rs.getLong("cust_id"));
				customer.setBirthDate(rs.getString("birth_date") != null ? new SimpleDateFormat("dd/MM/yyyy").format(rs.getDate("birth_date")) : "");
				customer.setIdNo(rs.getString("id_no"));
				break;
			}
			logger.info("End getCustomerByIsdn: isdn " + isdn + " time: "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).append("\nERROR getCustomerByIsdn ").append(isdn).append(" Message: ").
					append(ex.getMessage());
			logger.error(br + ex.toString());
			customer = null;
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return customer;
		}
	}
	
	public int insertLogCallWsWallet(String isdn, Long ewalletId, String request, String custName, String idNo, String parentId, String issuePlace) {
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		int result = 0;
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection("dbsm");
			sql = "insert into Log_Call_Ws_Wallet (id,isdn,ewallet_id,action_type,status_process,number_process,insert_date,description,customer_name,dob,id_no,channel_type,parent_id,idissueplace,idissuedate)\n"
					+ "values (log_call_ws_wallet_seq.nextval,?,?,'1','0','0',sysdate,?,?,null,?,'',?,?,null)";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn);
			ps.setLong(2, ewalletId);
			ps.setString(3, request);
			ps.setString(4, custName);
			ps.setString(5, idNo);
			if (parentId == null) {
				parentId = "";
			}
			ps.setString(6, parentId);
			ps.setString(7, issuePlace);
			result = ps.executeUpdate();
			logger.info("End insertLogCallWsWallet staffId " + ewalletId + " isdn " + isdn + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR insertSaleToKeyPos: ").
					append(sql).append("\n")
					.append(" staffId ")
					.append(ewalletId)
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
}
