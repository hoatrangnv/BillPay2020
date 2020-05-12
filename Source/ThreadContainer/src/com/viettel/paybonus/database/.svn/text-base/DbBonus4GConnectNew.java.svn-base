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
public class DbBonus4GConnectNew extends DbProcessorAbstract {

	private String loggerLabel = DbPayBonusSecond.class.getSimpleName() + ": ";
	private PoolStore poolStore;
	private String dbNameCofig;

	public DbBonus4GConnectNew() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = ResourceBundle.getBundle("configPayBonus").getString("dbNameConfig");
		poolStore = new PoolStore(dbNameCofig, logger);
	}

	public DbBonus4GConnectNew(String sessionName, Logger logger) throws SQLException, Exception {
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
			record.setActionAuditId(rs.getLong("action_audit_id"));
			record.setIsdn(rs.getString(("isdn")));
			record.setStatus(rs.getInt("status"));
			record.setNewSerial(rs.getString("sim_serial"));
		} catch (Exception ex) {
			logger.error("ERROR parse BonusChangeSim4G");
			logger.error(AppManager.logException(timeSt, ex));
		}
		return record;
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
		return new int[0];
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
			sql = "select isdn from waiting_bonus_connectsim4g where  isdn = ? and (status =0 or result_code = '0')";
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

	public String getDataVolume4G(String isdn) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		String imei = "";
		long startTime = System.currentTimeMillis();
		try {
			if (!isdn.startsWith("258")) {
				isdn = "258" + isdn;
			}
			connection = getConnection("pre_call");
			sql = "select servedimeisv, ((sum(datavolumegprsuplink)+sum(datavolumegprsdownlink))/1024/1024) as data_volume \n"
					+ "from cdr_ggsn_3com where recordopeningtime >= trunc(sysdate-1) and rattype=6 and servedmsisdn=? \n"
					+ "group by servedimeisv having ((sum(datavolumegprsuplink)+sum(datavolumegprsdownlink))/1024/1024) > 0";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn);
			rs = ps.executeQuery();
			while (rs.next()) {
				imei = rs.getString("servedimeisv");
			}
			logger.info("End getDataVolume4G isdn " + isdn + " imei " + imei + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR getDataVolume4G: ").
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
					+ " and action_audit_id <> 0 and (bonus_result_code ='E01' or add_data_status ='0') "
					+ " union "
					+ " select isdn from waiting_bonus_changesim4g where substr(imei,0,14) = ? and status =0"
					+ " union "
					+ " select isdn from waiting_bonus_connectsim4g where substr(imei,0,14) = ? and status =0";
			ps = connection.prepareStatement(sql);
			ps.setString(1, imei.substring(0, 14));
			ps.setString(2, imei.substring(0, 14));
			ps.setString(3, imei.substring(0, 14));
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

	public BonusSim4G getInforBonusSim4G(String isdn) {
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		BonusSim4G bn = null;
		try {
			connection = getConnection("cm_pre");
			bn = new BonusSim4G();
			sql = "select  * from waiting_bonus_connectsim4g where isdn =?";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn);
			rs = ps.executeQuery();
			while (rs.next()) {
				bn.setActionAuditId(rs.getLong("action_audit_id"));
				bn.setIsdn(rs.getString(("isdn")));
				bn.setStatus(rs.getInt("status"));
				bn.setNewSerial(rs.getString("sim_serial"));
				bn.setTimeCheck(rs.getInt("times_check"));
				break;
			}
			logger.info("End checkUsedHandSet4G imei " + isdn + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkUsedHandSet4G: ").
					append(sql).append("\n");
			logger.error(br + ex.toString());
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return bn;
		}
	}

	public int insertInforBonusSim4G(BonusSim4G bn) {
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		int result = 0;
		try {
			connection = getConnection("cm_pre");
			sql = "insert into waiting_bonus_connectsim4g(action_audit_id,isdn,status,sim_serial,imei,import_time,\n"
					+ "last_process_date,add_data_value,result_code,description,times_check)\n"
					+ "values(?,?,?,?,?,sysdate,sysdate,?,?,?,?)";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, bn.getActionAuditId());
			ps.setString(2, bn.getIsdn());
			ps.setInt(3, bn.getStatus());
			ps.setString(4, bn.getNewSerial());
			ps.setString(5, bn.getImeiHS());
			ps.setLong(6, bn.getDataValuesAdded());
			ps.setString(7, bn.getResultCode());
			ps.setString(8, bn.getAddDataDesc());
			ps.setInt(9, bn.getTimeCheck());
			result = ps.executeUpdate();

			logger.info("End insertInforBonusSim4G  " + bn.getIsdn() + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkUsedHandSet4G: ").
					append(sql).append("\n");
			logger.error(br + ex.toString());
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public int updateInforBonusSim4G(BonusSim4G bn) {
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		long startTime = System.currentTimeMillis();
		int result = 0;
		try {
			connection = getConnection("cm_pre");
			sql = "update waiting_bonus_connectsim4g set last_process_date= sysdate ,add_data_value =?,result_code =?,description =?,times_check = times_check+1,imei=?,status =? where action_audit_id=?";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, bn.getDataValuesAdded());
			ps.setString(2, bn.getResultCode());
			ps.setString(3, bn.getAddDataDesc());
			ps.setString(4, bn.getImeiHS());
			ps.setInt(5, bn.getStatus());
			ps.setLong(6, bn.getActionAuditId());
			result = ps.executeUpdate();

			logger.info("End updateInforBonusSim4G  " + bn.getIsdn() + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR updateInforBonusSim4G: ").
					append(sql).append("\n");
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

}
