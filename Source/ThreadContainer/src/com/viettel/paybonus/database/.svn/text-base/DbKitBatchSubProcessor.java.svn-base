/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.EwalletLog;
import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.KitBatchSub;
import com.viettel.threadfw.database.DbProcessorAbstract;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbKitBatchSubProcessor extends DbProcessorAbstract {

	private String loggerLabel = DbKitBatchSubProcessor.class.getSimpleName() + ": ";
	private PoolStore poolStore;
	private String dbNameCofig;

	public DbKitBatchSubProcessor() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = "cm_pre";
		poolStore = new PoolStore(dbNameCofig, logger);
	}

	public DbKitBatchSubProcessor(String sessionName, Logger logger) throws SQLException, Exception {
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
		KitBatchSub record = new KitBatchSub();
		long timeSt = System.currentTimeMillis();
		SimpleDateFormat df = new SimpleDateFormat("ddMMyyyyHHmmss");
		try {
			record.setIsdn(rs.getString("isdn"));
			record.setSerial(rs.getString("serial"));
			record.setProductCode(rs.getString("product_code"));
			record.setReceiveDate(df.format(rs.getDate("process_time")));
		} catch (Exception ex) {
			logger.error("ERROR parse KitBatchSub");
			logger.error(AppManager.logException(timeSt, ex));
		}
		return record;
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
		List<ParamList> listParam = new ArrayList<ParamList>();
		String batchId = "";
		long timeSt = System.currentTimeMillis();
		try {
			for (Record rc : listRecords) {
				KitBatchSub sd = (KitBatchSub) rc;
				batchId = sd.getBatchId();
				ParamList paramList = new ParamList();
				paramList.add(new Param("ISDN", sd.getIsdn(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("SERIAL", sd.getSerial(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("PRODUCT_CODE", sd.getProductCode(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("PRODUCT_FEE", sd.getMoneyProduct(), Param.DataType.LONG, Param.IN));
				paramList.add(new Param("CHARITY", sd.getCharity(), Param.DataType.LONG, Param.IN));
				paramList.add(new Param("DESCRIPTION", sd.getDescription(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("RESULT_CODE", sd.getResultCode(), Param.DataType.STRING, Param.IN));
				paramList.add(new Param("PROCESS_DATE", "sysdate", Param.DataType.CONST, Param.IN));
				listParam.add(paramList);
			}
			int[] res = poolStore.insertTable(listParam.toArray(new ParamList[listParam.size()]), "KIT_BATCH_PROCESS_TEMP");
			logTimeDb("Time to insertQueueHis KIT_BATCH_PROCESS_TEMP, batchid " + batchId + " total result: " + res.length, timeSt);
			return res;
		} catch (Exception ex) {
			logger.error("ERROR insertQueueHis KIT_BATCH_PROCESS_TEMP batchid " + batchId, ex);
			logger.error(AppManager.logException(timeSt, ex));
			return null;
		}
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

	public boolean checkProcessedStatus(String isdn) {
		Connection connection = null;
		PreparedStatement ps = null;
		String sql = "";
		ResultSet result = null;
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection("cm_pre");
			sql = " SELECT ISDN FROM KIT_BATCH_PROCESS_TEMP WHERE ISDN = ? OR ISDN = ?";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn.startsWith("258") ? isdn.substring(3) : isdn);
			ps.setString(2, isdn.startsWith("258") ? isdn : "258" + isdn);
			result = ps.executeQuery();
			while (result.next()) {
				return Boolean.TRUE;
			}
			logger.info("End check checkProcessedStatus   isdn " + isdn + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
			return Boolean.FALSE;
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkProcessedStatus: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(isdn)
					.append(" result ")
					.append(result);
			logger.error(br + ex.toString());
			return Boolean.FALSE;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public boolean checkCreateTransactionAndPayBonus(String isdn) {
		Connection connection = null;
		PreparedStatement ps = null;
		String sql = "";
		ResultSet result = null;
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection("cm_pre");
			sql = " SELECT * FROM CM_PRE.KIT_BATCH_DETAIL A WHERE \n"
					+ " (A.KIT_BATCH_ID = 1 OR (A.KIT_BATCH_ID >= 22 AND A.KIT_BATCH_ID <= 41))\n"
					+ " AND (ISDN = ? OR ISDN =?) ";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn.startsWith("258") ? isdn.substring(3) : isdn);
			ps.setString(2, isdn.startsWith("258") ? isdn : "258" + isdn);
			result = ps.executeQuery();
			while (result.next()) {
				return Boolean.TRUE;
			}
			logger.info("End check checkCreateTransaction   isdn " + isdn + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
			return Boolean.FALSE;
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR checkCreateTransaction: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(isdn)
					.append(" result ")
					.append(result);
			logger.error(br + ex.toString());
			return Boolean.FALSE;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public int insertSaleTrans(Long saleTransId, Long shopId, Long staffId, Long saleServiceId, Long saleServicePriceId, Double amountTax,
			Long subId, String isdn, Long reasonId) {
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
					+ "LAST_UPDATE)  "
					+ " VALUES(?,sysdate,'4','2',NULL,NULL,NULL,?,?,'1',?,?,NULL,NULL,NULL,NULL, "
					+ "?,?,17,?,?,?,NULL,NULL,?,NULL,NULL,\n"
					+ "NULL,NULL,NULL,NULL,NULL,NULL,?,1,NULL,?,NULL,NULL,NULL,'0',NULL,NULL,NULL,0,'MT',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,\n"
					+ "sysdate)";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, saleTransId);
			ps.setLong(2, shopId);
			ps.setLong(3, staffId);
			ps.setLong(4, saleServiceId);
			ps.setLong(5, saleServicePriceId);
			ps.setDouble(6, amountTax);
			Double amountNotTax = amountTax / 1.17;
			ps.setDouble(7, amountNotTax);
			Double tax = amountTax - amountNotTax;
			ps.setDouble(8, tax);
			ps.setLong(9, subId);
			ps.setString(10, isdn);
			ps.setString(11, isdn);
			ps.setLong(12, reasonId);
			String saleTransCode = "SS0000" + String.format("%0" + 9 + "d", saleTransId);
			ps.setString(13, saleTransCode);
			result = ps.executeUpdate();
			logger.info("End  saleTransId " + saleTransId + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR : ").
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

	public Long getSubID(String isdn) {
		Connection connection = null;
		PreparedStatement ps = null;
		String sql = "";
		ResultSet result = null;
		long startTime = System.currentTimeMillis();
		Long subId = 0L;
		try {
			connection = getConnection("cm_pre");
			sql = " SELECT SUB_ID FROM SUB_MB WHERE ( ISDN =? OR ISDN = ? ) AND STATUS =2 ";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn.startsWith("258") ? isdn.substring(3) : isdn);
			ps.setString(2, isdn.startsWith("258") ? isdn : "258" + isdn);
			result = ps.executeQuery();
			while (result.next()) {
				subId = result.getLong("SUB_ID");
			}
			logger.info("End check getSubID   isdn " + isdn + " result " + result + " time " + (System.currentTimeMillis() - startTime));
			return subId;
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR getSubID: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(isdn)
					.append(" result ")
					.append(result);
			logger.error(br + ex.toString());
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
		}
	}

	public int insertSaleTransDetail(Long saleTransDetailId, Long saleTransId, String stockModelId, String priceId, Long saleServiceId, String saleServicePriceId,
			String stockTypeId, String stockTypeName, String stockModelCode, String stockModelName, String saleServicesCode, String saleServicesName, String accountModelCode,
			String accountModelName, String saleServicesPriceVat, String priceVat, String price, String saleServicesPrice, Double amountTax, Double discountAmout) {

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
					+ "VALUES(?,?,sysdate,?,1,?,1,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,"
					+ "?,?,?,NULL,?,?,?,?,?,?,?,'MT',?,?,?,?,?,?,?,?,?,?)";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, saleTransDetailId);
			ps.setLong(2, saleTransId);
			ps.setString(3, stockModelId);
			ps.setString(4, priceId);
			ps.setLong(5, saleServiceId);
			ps.setString(6, saleServicePriceId);
			ps.setString(7, stockTypeId);
			ps.setString(8, stockTypeName);
			ps.setString(9, stockModelCode);
			ps.setString(10, stockModelName);
			ps.setString(11, saleServicesCode);
			ps.setString(12, saleServicesName);
			ps.setString(13, accountModelCode);
			ps.setString(14, accountModelName);
			Double amountNotTax = amountTax / 1.17;
			Double tax = amountTax - amountNotTax;
			ps.setDouble(15, tax);
			ps.setString(16, saleServicesPriceVat);
			ps.setString(17, priceVat);
			ps.setString(18, price);
			ps.setString(19, saleServicesPrice);
			ps.setDouble(20, amountTax);
			if (discountAmout > 0) {
				ps.setDouble(21, discountAmout);
				ps.setDouble(22, 0);
				ps.setDouble(23, 0);
				ps.setDouble(24, 0);
			} else {
				ps.setString(21, "");
				ps.setDouble(22, amountNotTax);
				ps.setDouble(23, tax);
				ps.setDouble(24, amountTax);
			}

			result = ps.executeUpdate();
			logger.info("End  saleTransId " + saleTransId + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR : ").
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
			logger.info("End Serial saleTransDetailId " + saleTransDetailId + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR Serial: ").
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

	public Long[] getActionInfo(String isdn) {
		Connection connection = null;
		PreparedStatement ps = null;
		String sql = "";
		ResultSet result = null;
		long startTime = System.currentTimeMillis();
		Long[] arrInfos = new Long[2];
		try {
			connection = getConnection("cm_pre");
			sql = " select action_audit_id,action_code from  cm_pre.sub_profile_info where create_time >='15-mar-2019' and isdn = ? or isdn =? order by create_time desc ";
			ps = connection.prepareStatement(sql);
			ps.setString(1, isdn.startsWith("258") ? isdn.substring(3) : isdn);
			ps.setString(2, isdn.startsWith("258") ? isdn : "258" + isdn);
			result = ps.executeQuery();
			while (result.next()) {
				arrInfos[0] = result.getLong("action_audit_id");
				arrInfos[1] = result.getLong("action_code");
				break;
			}
			logger.info("End check getActionInfo   isdn " + isdn + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
			return arrInfos;
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR getActionInfo: ").
					append(sql).append("\n")
					.append(" isdn ")
					.append(isdn)
					.append(" result ")
					.append(result);
			logger.error(br + ex.toString());
			return null;
		} finally {
			closeStatement(ps);
			closeConnection(connection);
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

	public long getActionAuditIdSeq() {
		Connection connection = null;
		ResultSet rs = null;
		long actionAuditId = 0;
		String sqlMo = "select seq_action_audit.nextval action_audit_id from dual";
		PreparedStatement psMo = null;
		try {
			connection = getConnection("cm_pre");
			psMo = connection.prepareStatement(sqlMo);
			if (QUERY_TIMEOUT > 0) {
				psMo.setQueryTimeout(QUERY_TIMEOUT);
			}
			rs = psMo.executeQuery();
			while (rs.next()) {
				actionAuditId = rs.getLong("action_audit_id");
				break;
			}
		} catch (Exception ex) {
			logger.error("ERROR getActionAuditIdSeq value: " + actionAuditId);
			return 0;
		} finally {
			closeStatement(psMo);
			closeConnection(connection);
		}
		return actionAuditId;
	}

	public int insertActionAudit(long actionAuditId, String actionCode, long reasonId, long subId, String shopCode, String userName, String description) {
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
					+ " VALUES(?,sysdate,?,?,?,?, "
					+ "'3',?,'127.0.0.1',?)";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, actionAuditId);
			ps.setString(2, actionCode);
			ps.setLong(3, reasonId);
			ps.setString(4, shopCode);
			ps.setString(5, userName);
			ps.setLong(6, subId);
			ps.setString(7, description);

			result = ps.executeUpdate();
			logger.info("End insertActionAudit action audit id  " + actionAuditId + " actionCode " + actionCode
					+ " reasonId " + reasonId + " shopCode " + shopCode + " userName " + userName + " subId " + subId + " result " + result + " time "
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

}
