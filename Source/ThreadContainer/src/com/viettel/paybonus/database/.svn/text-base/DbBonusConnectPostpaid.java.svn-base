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

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbBonusConnectPostpaid extends DbProcessorAbstract {

	private String loggerLabel = DbBonusConnectPostpaid.class.getSimpleName() + ": ";
	private PoolStore poolStore;
	private String dbNameCofig;

	public DbBonusConnectPostpaid() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
		poolStore = new PoolStore(dbNameCofig, logger);
	}

	public DbBonusConnectPostpaid(String sessionName, Logger logger) throws SQLException, Exception {
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
		BonusConnectPostPaid record = new BonusConnectPostPaid();
		long timeSt = System.currentTimeMillis();
		try {
			record.setSubId(rs.getLong("sub_id"));
			record.setActionAuditId(rs.getLong("action_audit_id"));
			record.setIsdn(rs.getString("isdn"));
			record.setShopCode(rs.getString("shop_code"));
			record.setStaffCode(rs.getString("user_created"));
			record.setSerial(rs.getString("serial"));
			record.setProductCode(rs.getString("product_code"));
			record.setImsi(rs.getString("imsi"));
			record.setBonusValues(0L);

		} catch (Exception ex) {
			logger.error("ERROR parse BonusConnectPostPaid");
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
			paramList.add(new Param("BONUS_TYPE", 11L, Param.DataType.LONG, Param.IN));
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
		String sqlDeleteMo = "insert into bonus_connect_post_paid (action_audit_id,isdn,serial,staff_code,shop_code,product_code"
				+ ",bonus_values,result_code,description, process_time) values (?,?,?,?,?,?,?,?,?,sysdate)";
		try {
			connection = ConnectionPoolManager.getConnection("cm_pos");
			ps = connection.prepareStatement(sqlDeleteMo);
			for (Record rc : listRecords) {
				BonusConnectPostPaid pn = (BonusConnectPostPaid) rc;
				ps.setLong(1, pn.getActionAuditId());
				ps.setString(2, pn.getIsdn());
				ps.setString(3, pn.getSerial());
				ps.setString(4, pn.getStaffCode());
				ps.setString(5, pn.getShopCode());
				ps.setString(6, pn.getProductCode());
				ps.setLong(7, pn.getBonusValues());
				ps.setString(8, pn.getResultCode());
				ps.setString(9, pn.getDescription());
				batchId = pn.getBatchId();
				ps.addBatch();
			}
			return ps.executeBatch();
		} catch (Exception ex) {
			logger.error("ERROR update bonus_connect_post_paid  batchid " + batchId, ex);
			logger.error(AppManager.logException(timeStart, ex));
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			logTimeDb("Time to update bonus_connect_post_paid, batchid " + batchId, timeStart);
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

	public boolean checkAlreadyProcessed(String isdn) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		boolean isProcessed = false;
		try {
			connection = getConnection("cm_pos");
			sql = "select isdn from bonus_connect_post_paid where product_code= '0' and isdn = ?";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn);
			rs = ps.executeQuery();
			while (rs.next()) {
				isProcessed = true;
				break;
			}
			logger.info("End checkAlreadyProcessed isdn " + isdn + " product isProcessed " + isProcessed + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkAlreadyProcessed: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(isdn);
			logger.error(br + ex.toString());
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return isProcessed;
		}
	}

	public SpecialProductMB getSpecialProductMBByProduct(String productCode) {
		ResultSet rs1 = null;
		Connection connection = null;
		String sqlMo = "select * from special_mobile_product where status =1 and  upper(product_code) =?";
		PreparedStatement psMo = null;
		SpecialProductMB product = null;
		try {
			connection = getConnection("cm_pos");
			psMo = connection.prepareStatement(sqlMo);
			psMo.setString(1, productCode.trim().toUpperCase());
			rs1 = psMo.executeQuery();
			while (rs1.next()) {
				product = new SpecialProductMB();
				product.setId(rs1.getLong("id"));
				product.setPrductCode(rs1.getString("product_code"));
				product.setPrepaidMonths(rs1.getLong("prepaid_months"));
				product.setPayAdvence(rs1.getLong("pay_advence"));
				product.setLimit(rs1.getLong("limit"));
				product.setSaleServiceCode(rs1.getString("sale_service_code"));
				product.setAgentMoney(rs1.getLong("agent_money"));
				product.setSpecialUser(rs1.getString("special_user") != null ? rs1.getString("special_user") : "");
				product.setStatus(rs1.getLong("status"));
				product.setBonusAgent(rs1.getLong("bonus_agent"));
				product.setBonusOther(rs1.getLong("bonus_other"));
				product.setMonthlyFee(rs1.getLong("monthly_fee"));
				break;
			}
			logger.info("End getSpecialProductMB product code: " + productCode + "-" + product.toString());
		} catch (Exception ex) {
			logger.error("ERROR getSpecialProductMB exception: " + ex.getLocalizedMessage());
			return null;
		} finally {
			closeResultSet(rs1);
			closeStatement(psMo);
			closeConnection(connection);
		}
		return product;
	}

	public boolean checkStaffBelongAgentShop(String staff, long shopID) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		boolean isTrue = false;
		try {
			connection = getConnection("dbsm");
			sql = "select * from staff where lower(staff_Code)= ? and status =1 and shop_id =? ";
			ps = connection.prepareStatement(sql);
			ps.setString(1, staff.trim().toLowerCase());
			ps.setLong(2, shopID);
			rs = ps.executeQuery();
			while (rs.next()) {
				isTrue = true;
				break;
			}
			logger.info("End checkStaffBelongAgentShop staff " + staff + " shop id " + shopID + " result " + isTrue + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkStaffBelongAgentShop: ").
					append(sql).append("\n")
					.append(" staff ")
					.append(staff)
					.append(" shop ")
					.append(shopID);
			logger.error(br + ex.toString());
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return isTrue;
		}
	}
}
