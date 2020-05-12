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
public class DbVipSubCheckValidSub extends DbProcessorAbstract {

//    private Logger logger;
	private String loggerLabel = DbVipSubCheckValidSub.class.getSimpleName() + ": ";
//    private StringBuffer br = new StringBuffer();
	private PoolStore poolStore;
	private String sqlDeleteMo = "update vip_sub_detail set sub_status =? where isdn = ? and vip_sub_detail_id =?";

	public DbVipSubCheckValidSub() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		poolStore = new PoolStore("dbvipsub", logger);
	}

	public DbVipSubCheckValidSub(String sessionName, Logger logger) throws SQLException, Exception {
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
		VipSubDetail record = new VipSubDetail();
		long timeSt = System.currentTimeMillis();
		try {
			record.setId(rs.getLong(VipSubDetail.VIP_SUB_INFO_ID));
			record.setVipSubInfoId(rs.getLong(VipSubDetail.VIP_SUB_INFO_ID));
			record.setVipSubDetailId(rs.getLong(VipSubDetail.VIP_SUB_DETAIL_ID));
			record.setIsdn(rs.getString("isdn"));
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
		long timeStart = System.currentTimeMillis();
		PreparedStatement ps = null;
		Connection connection = null;
		String batchId = "";
		try {
			connection = ConnectionPoolManager.getConnection("dbvipsub");
			ps = connection.prepareStatement(sqlDeleteMo);
			for (Record rc : listRecords) {
				VipSubDetail sd = (VipSubDetail) rc;
				batchId = sd.getBatchId();
				ps.setInt(1, sd.getSubStatus());
				ps.setString(2, sd.getIsdn());
				ps.setLong(3, sd.getVipSubDetailId());
				ps.addBatch();
			}
			return ps.executeBatch();
		} catch (Exception ex) {
			logger.error("ERROR deleteQueue vip_sub_detail batchid " + batchId, ex);
			logger.error(AppManager.logException(timeStart, ex));
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			logTimeDb("Time to deleteQueue vip_sub_detail, batchid " + batchId, timeStart);
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
			deleteQueueTimeout(ids);
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
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public boolean checkCorrectProfile(String isdn) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		boolean result = false;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection("cm_pre");
			sql = "select a.isdn from cm_pre.sub_profile_info a, cm_pre.sub_mb b where a.isdn = b.isdn and b.status = 2 \n"
					+ "and a.cust_id = b.cust_id and a.isdn = ? and a.check_status = 1";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn);
			rs = ps.executeQuery();
			while (rs.next()) {
				String otpResult = rs.getString("isdn");
				if (otpResult != null && otpResult.length() > 0) {
					result = true;
					break;
				}
			}
			logger.info("End checkCorrectProfile isdn " + isdn + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkCorrectProfile: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(isdn)
					.append(" result ")
					.append(result);
			logger.error(br + ex.toString());
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public boolean checkCorrectOldProfile(String isdn) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		boolean result = false;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection("cm_pre");
			sql = "select a.isdn_account from profile.action_profile a, cm_pre.customer b where  \r\n"
					+ "a.cust_id = b.cust_id and a.isdn_account = ? and a.check_info = '0' \r\n"
					+ "and a.check_status = 1";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn);
			rs = ps.executeQuery();
			while (rs.next()) {
				String otpResult = rs.getString("isdn");
				if (otpResult != null && otpResult.length() > 0) {
					result = true;
					break;
				}
			}
			logger.info("End checkCorrectOldProfile isdn " + isdn + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkCorrectOldProfile: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(isdn)
					.append(" result ")
					.append(result);
			logger.error(br + ex.toString());
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public boolean checkNormalSub(String isdn) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		boolean result = false;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection("cm_pre");
			sql = "select isdn from cm_pre.sub_mb where isdn =? and status =2 and act_status ='00'";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn);
			rs = ps.executeQuery();
			while (rs.next()) {
				String otpResult = rs.getString("isdn");
				if (otpResult != null && otpResult.length() > 0) {
					result = true;
					break;
				}
			}
			logger.info("End checkNormalSub isdn " + isdn + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkNormalSub: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(isdn)
					.append(" result ")
					.append(result);
			logger.error(br + ex.toString());
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public int checkTotalQueue(Long vipSubId) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		int result = 0;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection("dbvipsub");
			sql = "select b.isdn from vip_sub_info a, vip_sub_detail b where a.vip_sub_info_id = b.vip_sub_info_id and a.vip_sub_info_id =? and sub_status is null";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, vipSubId);
			rs = ps.executeQuery();
			while (rs.next()) {
				String isdn = rs.getString("isdn");
				if (isdn != null && isdn.length() > 0) {
					result = result + 1;
				}
			}
			logger.info("End checkFinish vipSubId " + vipSubId + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkTotalQueue: ").
					append(sql).append("\n")
					.append(" vipSubId ")
					.append(vipSubId)
					.append(" result ")
					.append(result);
			logger.error(br + ex.toString());
			result = -1;
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}
	
	
	public int getLastRecord(Long vipSubId) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		int result = 0;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection("dbvipsub");
			sql = "select b.vip_sub_info_id from vip_sub_info a, vip_sub_detail b where a.vip_sub_info_id = b.vip_sub_info_id and a.vip_sub_info_id =1261 order by a.vip_sub_info_id,b.vip_sub_detail_id desc ";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, vipSubId);
			rs = ps.executeQuery();
			while (rs.next()) {
				String isdn = rs.getString("isdn");
				if (isdn != null && isdn.length() > 0) {
					result = rs.getInt("vip_sub_info_id");
				}
			}
			logger.info("End getLastRecord vipSubId " + vipSubId + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR getLastRecord: ").
					append(sql).append("\n")
					.append(" vipSubId ")
					.append(vipSubId)
					.append(" result ")
					.append(result);
			logger.error(br + ex.toString());
			result = -1;
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}
}
