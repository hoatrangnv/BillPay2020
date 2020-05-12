/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.database;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.smsfw.manager.AppManager;
import com.viettel.smsfw.database.DbProcessorAbstract;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.DataResources;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import com.viettel.vas.wsfw.common.WebserviceManager;
import com.viettel.vas.wsfw.object.Config;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import com.viettel.vas.wsfw.object.EwalletLog;
import com.viettel.vas.wsfw.object.InvoiceInfo;
import com.viettel.vas.wsfw.object.ProductMonthlyFee;
import com.viettel.vas.wsfw.object.SubAdslLLPrepaid;
import com.viettel.vas.wsfw.object.Subscriber;
import com.viettel.vas.wsfw.object.TransLog;
import com.viettel.vas.wsfw.object.UserInfo;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Thong tin phien ban
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class DbProcessor extends DbProcessorAbstract {

	private String loggerLabel = DbProcessor.class.getSimpleName() + ": ";
	private PoolStore poolStore;
	private String dbNameCofig;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");

	public DbProcessor() throws SQLException, Exception {
		this.logger = Logger.getLogger(loggerLabel);
		dbNameCofig = ResourceBundle.getBundle("vas").getString("dbNameConfig");
		poolStore = new PoolStore(dbNameCofig, logger);
	}

	public DbProcessor(String sessionName, Logger logger) throws SQLException, Exception {
		this.logger = logger;
		poolStore = new PoolStore(sessionName, logger);
	}

	public int insertTopupLog(TransLog transLog) {
		List<ParamList> listParam = new ArrayList<ParamList>();
		long timeSt = System.currentTimeMillis();
		try {
			ParamList paramList = new ParamList();
			paramList.add(new Param("TOPUP_LOG_ID", "TOPUP_LOG_SEQ.NEXTVAL", Param.DataType.CONST, Param.IN));
			paramList.add(new Param(TransLog.WS_CODE, transLog.getWsCode(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.CLIENT, transLog.getClient(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.DURATION, transLog.getDuration(), Param.DataType.LONG, Param.IN));
			paramList.add(new Param(TransLog.INPUT, transLog.getInput(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.ISDN, transLog.getIsdn(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.OUTPUT, transLog.getOutput(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.MONEY, transLog.getMoney(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.DATA, transLog.getDataPackage(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.FUND, transLog.getFundCode(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.FUND_VALUE_BEFORE, transLog.getFundValueBefore(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.FUND_VALUE_AFTER, transLog.getFundValueAfter(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.TRANS_TYPE, transLog.getTransType(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.SUB_TYPE, transLog.getSubType(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.REQUEST_ID, transLog.getRequestId(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.RESULT_CODE, transLog.getResultCode(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param(TransLog.IP_REMOTE, transLog.getIpRemote(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("START_DATE", transLog.getStartTime(), Param.DataType.TIMESTAMP, Param.IN));
			paramList.add(new Param("PARTNER_CODE", transLog.getPartnerCode(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("SOURCE_ID", transLog.getSourceId(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("SERVICETYPE", transLog.getServiceType(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("pin_code", transLog.getPinCode(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("CREDIT_ACCOUNT_NUMBER", transLog.getCreditAccountNumber(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("INVOICE_LIST_ID", transLog.getInvoiceListId(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("SIGNATURE", transLog.getSignature(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("app_name", WebserviceManager.appId, Param.DataType.STRING, Param.IN));
			listParam.add(paramList);
			PoolStore.PoolResult prs = poolStore.insertTable(paramList, "TOPUP_LOG");
			logTimeDb("Time to insertTopupLog", timeSt);
			return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
		} catch (Exception ex) {
			logger.error("ERROR insertTopupLog", ex);
			logger.error(AppManager.logException(timeSt, ex));
			return -1;
		}
	}

	public int insertEwalletLog(EwalletLog log) {

		ParamList paramList = new ParamList();
		long timeSt = System.currentTimeMillis();
		try {
//            paramList.add(new Param("EWALLET_LOG_ID", log.geteWalletLogId(), Param.DataType.LONG, Param.IN));
			paramList.add(new Param("REQUEST_ID", log.getRequestId(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("CLIENT", log.getStaffCode(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("MOBILE", log.getIsdn(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("TRANS_ID", log.getTransId(), Param.DataType.STRING, Param.IN));
//            paramList.add(new Param("ACTION_CODE", log.getActionCode(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("AMOUNT", log.getAmount(), Param.DataType.DOUBLE, Param.IN));
			paramList.add(new Param("FUNCTION_NAME", log.getFunctionName(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("URL", log.getUrl(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("USERNAME", log.getUserName(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("REQUEST", log.getRequest(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("RESPONSE", log.getRespone(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("DURATION", log.getDuration(), Param.DataType.LONG, Param.IN));
			paramList.add(new Param("ERROR_CODE", log.getErrorCode(), Param.DataType.STRING, Param.IN));
			paramList.add(new Param("DESCRIPTION", log.getDescription(), Param.DataType.STRING, Param.IN));
			PoolStore.PoolResult prs = poolStore.insertTable(paramList, "EWALLET_LOG");
			logTimeDb("Time to insertEwalletLog isdn " + log.getIsdn(), timeSt);
			return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
		} catch (Exception ex) {
			logger.error("ERROR insertEwalletLog default return -1: isdn " + log.getIsdn());
			logger.error(AppManager.logException(timeSt, ex));
			return -1;
		}
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

	private void closePrepareStatement(PreparedStatement ps) {
		try {
			if (ps != null) {
				ps.close();
				ps = null;
			}
		} catch (Exception e) {
			ps = null;
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
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public int[] deleteQueue(List<Record> listRecords) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public int[] insertQueueHis(List<Record> listRecords) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public int[] insertQueueOutput(List<Record> listRecords) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void processTimeoutRecord(List<String> ids) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean checkProductAllow(String productCode, String isdn) {
		/**
		 * SELECT product_code FROM ba_product_config WHERE product_code = p_product_code AND status = 0;
		 */
		ParamList paramList = new ParamList();
		boolean result = true;
		long timeSt = System.currentTimeMillis();
		try {
			paramList.add(new Param("product_code", productCode, Param.DataType.STRING, Param.IN));
			paramList.add(new Param("STATUS", "0", Param.DataType.CONST, Param.IN));
			paramList.add(new Param("product_code", null, Param.DataType.STRING, Param.OUT));
			DataResources rs = poolStore.selectTable(paramList, "ba_product_config");
			while (rs.next()) {
				String product = rs.getString("product_code");
				if (product != null && product.trim().length() > 0) {
					result = false;
					break;
				}
			}
			logTimeDb("Time to checkProductAllow productCode " + productCode + ", isdn " + isdn + " result: " + result, timeSt);
		} catch (Exception ex) {
			logger.error("ERROR checkProductAllow default return false " + productCode + " isdn " + isdn);
			logger.error(AppManager.logException(timeSt, ex));
			result = false;
		}
		return result;
	}

	public String getActionCode(long pkId, long actionId) {
		/**
		 * select action_code from action where action_id = ? and status = 1;
		 */
		ParamList paramList = new ParamList();
		String ationCode = "";
		long timeSt = System.currentTimeMillis();
		try {
			paramList.add(new Param("action_id", actionId, Param.DataType.LONG, Param.IN));
			paramList.add(new Param("status", "1", Param.DataType.CONST, Param.IN));
			paramList.add(new Param("action_code", null, Param.DataType.STRING, Param.OUT));
			DataResources rs = poolStore.selectTable(paramList, "profile.action");
			while (rs.next()) {
				ationCode = rs.getString("action_code");
			}
			logTimeDb("Time to getActionCode for actionauditid " + pkId + " action_code " + ationCode, timeSt);
		} catch (Exception ex) {
			logger.error("ERROR getActionCode for actionauditid " + pkId + " action_code " + ationCode);
			logger.error(AppManager.logException(timeSt, ex));
		}
		return ationCode;
	}

	public int insertSendSmsHis(String isdn, String content, String shortCode, String client, String errorCode) {
		int res = 0;
		long timeSt = System.currentTimeMillis();
		try {
			if (content == null || isdn == null || content.trim().length() <= 0
					|| isdn.trim().length() <= 0 || shortCode.trim().length() <= 0) {
				return res;
			}
			ParamList paramList = new ParamList();
			paramList.add(new Param("MT_HIS_ID", "MT_SEQ.NEXTVAL", Param.DataType.CONST, Param.IN));
			paramList.add(new Param("MSISDN", isdn, Param.DataType.STRING, Param.IN));
			paramList.add(new Param("MESSAGE", content, Param.DataType.STRING, Param.IN));
			paramList.add(new Param("CHANNEL", shortCode, Param.DataType.STRING, Param.IN));
			paramList.add(new Param("node_name", client, Param.DataType.STRING, Param.IN));
			paramList.add(new Param("status", errorCode, Param.DataType.STRING, Param.IN));
			paramList.add(new Param("SENT_TIME", "sysdate", Param.DataType.CONST, Param.IN));
			PoolStore.PoolResult prs = poolStore.insertTable(paramList, "MT_HIS");
			logTimeDb("Time to insertSendSmsHis isdn " + isdn, timeSt);
			return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
		} catch (Exception ex) {
			logger.error("ERROR insertSendSmsHis isdn " + isdn, ex);
			logger.error(AppManager.logException(timeSt, ex));
			return res;
		}
	}

	public HashMap<String, UserInfo> getUserInfo() {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
//        UserInfo user = null;
//        String sessionName = ResourceBundle.getBundle("vas").getString("sessionName");
//        String sqlGetUser = "select * from  WS_USER where username=?";
		String sqlGetUser = "select * from  WS_USER";
		Connection connection = null;
		HashMap result = new HashMap<String, UserInfo>();
		try {
			connection = poolStore.getConnection();
			ps = connection.prepareStatement(sqlGetUser);
//            ps.setString(1, userName);
			rs = ps.executeQuery();
			while (rs.next()) {
				UserInfo user = new UserInfo();
				user.setId(rs.getLong(UserInfo.ID));
				user.setUser(rs.getString("username"));
				user.setPassword(rs.getString(UserInfo.PASSWORD));
				user.setIp(rs.getString(UserInfo.IP_ADDRESS));
				user.setFundCode(rs.getString("FUND_CODE"));
				user.setPartnerCode(rs.getString("PARTNER_CODE"));
				logger.info("Load user " + user.getUser());
				result.put(user.getUser(), user);
			}
			logTimeDb("Time to getUserInfo ", timeSt);
		} catch (Exception ex) {
			logger.error("Error getUserInfo", ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public boolean checkSameRequestId(String requestId, String client) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs1 = null;
		Connection connection = null;
		boolean result = false;
		String sqlMo = "select request_id from topup_log where start_date > trunc(sysdate-30) and REQUEST_ID = ? and CLIENT = ?";
		PreparedStatement psMo = null;
		String ownerSub = "";
		try {
			connection = ConnectionPoolManager.getConnection("dbtopup");
			psMo = connection.prepareStatement(sqlMo);
			if (QUERY_TIMEOUT > 0) {
				psMo.setQueryTimeout(QUERY_TIMEOUT);
			}
			psMo.setString(1, requestId);
			psMo.setString(2, client);
			rs1 = psMo.executeQuery();
			while (rs1.next()) {
				ownerSub = rs1.getString("request_id");
				if (ownerSub != null && ownerSub.trim().length() > 0) {
					result = true;
					break;
				}
			}
			logTimeDb("Time to checkSameRequestId requestId " + requestId + " client " + client
					+ " result: " + result, timeSt);
		} catch (Exception ex) {
			logger.error("ERROR checkSameRequestId default return false requestId " + requestId + " client " + client);
			logger.error(AppManager.logException(timeSt, ex));
			result = false;
		} finally {
			closeResultSet(rs1);
			closeStatement(psMo);
			closeConnection(connection);
		}
		return result;
	}

//    public boolean checkPartnerCode(String client, String partner) {
//        /**
//         * SELECT partner_code FROM client WHERE username = client AND status =
//         * 1;
//         */
//        ParamList paramList = new ParamList();
//        boolean result = false;
//        long timeSt = System.currentTimeMillis();
//        try {
//            paramList.add(new Param("username", client, Param.DataType.STRING, Param.IN));
//            paramList.add(new Param("PARTNER_CODE", null, Param.DataType.STRING, Param.OUT));
//            DataResources rs = poolStore.selectTable(paramList, "ws_user");
//            while (rs.next()) {
//                String product = rs.getString("PARTNER_CODE");
//                if (product != null && partner.trim().toUpperCase().equals(product.trim().toUpperCase())) {
//                    result = true;
//                    break;
//                }
//            }
//            logTimeDb("Time to checkPartnerCode, client " + client + ", partner " + partner + "  result: " + result, timeSt);
//        } catch (Exception ex) {
//            logger.error("ERROR checkPartnerCode default return false, partner " + partner + " client " + client);
//            logger.error(AppManager.logException(timeSt, ex));
//            result = false;
//        }
//        return result;
//    }
	public long getContractMobile(String isdn, String orgMsisdn) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		long contract = 0;
		String sqlGetUser = "select a.contract_id from sub_mb a where a.isdn = ? and a.status = 2 "
				+ "and exists(select contract_id from contract where contract_id = a.contract_id and status = 2)";
		Connection connection = null;
		try {
			logger.info("Start getContractMobile for sub " + isdn);
			connection = getConnection("cm_pos");
			ps = connection.prepareStatement(sqlGetUser);
			if (isdn.startsWith("258")) {
				isdn = isdn.substring(3);
			}
			ps.setString(1, isdn);
			rs = ps.executeQuery();
			if (rs.next()) {
				contract = rs.getLong("contract_id");
			}
			logTimeDb("Time to getContractMobile isdn " + isdn + " orgMsisdn " + orgMsisdn + " contract_id " + contract, timeSt);
		} catch (Exception ex) {
			logger.error("Error getContractMobile orgMsisdn " + orgMsisdn, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return contract;
		}
	}

	public long getContractMobileByRefer(String ref) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		long contract = 0;
		String sqlGetUser = "select a.contract_id from contract a where a.status = 2 and a.reference_id = ? and exists ("
				+ "select isdn from sub_mb where status = 2 and contract_id = a.contract_id)";
		Connection connection = null;
		try {
			logger.info("Start getContractMobileByRefer for ref " + ref);
			connection = getConnection("cm_pos");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, ref);
			rs = ps.executeQuery();
			if (rs.next()) {
				contract = rs.getLong("contract_id");
			}
			logTimeDb("Time to getContractMobileByRefer ref " + ref + " contract_id " + contract, timeSt);
		} catch (Exception ex) {
			logger.error("Error getContractMobileByRefer ref " + ref, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return contract;
		}
	}

	public long getContractFbbByRefer(String ref) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		long contract = 0;
		String sqlGetUser = "select a.contract_id from contract a where a.status = 2 and a.reference_id = ? and exists ("
				+ "select isdn from sub_adsl_ll where status = 2 and contract_id = a.contract_id)";
		Connection connection = null;
		try {
			logger.info("Start getContractFbbByRefer for ref " + ref);
			connection = getConnection("cm_pos");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, ref);
			rs = ps.executeQuery();
			if (rs.next()) {
				contract = rs.getLong("contract_id");
			}
			logTimeDb("Time to getContractFbbByRefer ref " + ref + " contract_id " + contract, timeSt);
		} catch (Exception ex) {
			logger.error("Error getContractFbbByRefer ref " + ref, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return contract;
		}
	}

	public long getContractFbb(String account, String msisdn) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		long contract = 0;
		String sqlGetUser = "select a.contract_id from sub_adsl_ll a where a.account = ? and a.status = 2 and "
				+ "exists(select contract_id from contract where contract_id = a.contract_id and status = 2)";
		Connection connection = null;
		try {
			logger.info("Start getContractFbb for account " + account);
			connection = getConnection("cm_pos");
			ps = connection.prepareStatement(sqlGetUser);
			if (account.startsWith("258")) {
				account = account.substring(3);
			}
			ps.setString(1, account);
			rs = ps.executeQuery();
			if (rs.next()) {
				contract = rs.getLong("contract_id");
			}
			logTimeDb("Time to getContractFbb account " + account + " msisdn " + msisdn + " contract_id " + contract, timeSt);
		} catch (Exception ex) {
			logger.error("Error getContractFbb msisdn " + msisdn, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return contract;
		}
	}

	public SubAdslLLPrepaid checkFtthPrepaid(long contractId, String account) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		SubAdslLLPrepaid result = null;
		String sqlGetUser = "select a.account, a.block_time, a.create_shop, a.create_time, a.create_user, a.EXPIRE_TIME, a.SUB_ADSL_LL_PREPAID_ID, "
				+ "a.new_product_code, a.PREPAID_TYPE, a.SALE_TRANS_ID, b.sub_id, b.product_code, b.act_status, c.tel_fax "
				+ " from sub_adsl_ll_prepaid a, sub_adsl_ll b, contract c where a.contract_id = ? and a.account = b.account "
				+ " and a.contract_id = c.contract_id and b.status = 2 and c.status = 2 order by a.expire_time";
		Connection connection = null;
		try {
			logger.info("Start checkFtthPrepaid for account " + account + " contractId " + contractId);
			connection = getConnection("cm_pos");
			ps = connection.prepareStatement(sqlGetUser);
			if (account.startsWith("258")) {
				account = account.substring(3);
			}
			ps.setLong(1, contractId);
			rs = ps.executeQuery();
			if (rs.next()) {
				String tmp = rs.getString("product_code");
				if (tmp != null && tmp.trim().length() > 0) {
					result = new SubAdslLLPrepaid();
					result.setAccount(rs.getString("account"));
					result.setContractId(contractId);
					result.setBlockTime(rs.getTimestamp("block_time"));
					result.setCreateShop(rs.getString("create_shop"));
					result.setCreateTime(rs.getTimestamp("create_time"));
					result.setCreateUser(rs.getString("create_user"));
					result.setExpireTime(rs.getTimestamp("EXPIRE_TIME"));
					result.setId(rs.getLong("SUB_ADSL_LL_PREPAID_ID"));
					result.setNewProductCode(tmp);
					result.setPrepaidType(rs.getLong("PREPAID_TYPE"));
					result.setSaleTransId(rs.getLong("SALE_TRANS_ID"));
					result.setSubId(rs.getLong("SUB_ID"));
					result.setActStatus(rs.getString("act_status"));
					result.setTelFax(rs.getString("tel_fax"));
				}
			}
			logTimeDb("Time to checkFtthPrepaid account " + account + " result " + result + " contract_id " + contractId, timeSt);
		} catch (Exception ex) {
			logger.error("Error checkFtthPrepaid account " + account + " contractId " + contractId, ex);
			logger.error(AppManager.logException(timeSt, ex));
			result = null;
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public ProductMonthlyFee getMonthlyFeeFtthPre(String account, String productCode) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		ProductMonthlyFee result = null;
		String sqlGetUser = "select monthly_fee, discount_rate from product_monthly_fee where product_code = ? ";
		Connection connection = null;
		try {
			logger.info("Start getMonthlyFeeFtthPre for account " + account);
			connection = getConnection("cm_pos");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, productCode);
			rs = ps.executeQuery();
			if (rs.next()) {
				long tmp = rs.getLong("monthly_fee");
				if (tmp > 0) {
					result = new ProductMonthlyFee();
					result.setMonthlyFee(tmp);
					String dis = rs.getString("discount_rate");
					String[] temp = dis.split("\\|");
					HashMap mapDiscount = new HashMap();
					for (String t : temp) {
						String[] temp2 = t.split(":");
						mapDiscount.put(temp2[0].trim().toUpperCase(), temp2[1].trim().toUpperCase());
					}
					result.setMapDiscount(mapDiscount);
				}
			}
			logTimeDb("Time to getMonthlyFeeFtthPre account " + account + " result " + (result != null ? result.getMonthlyFee() : "null"), timeSt);
		} catch (Exception ex) {
			logger.error("Error getMonthlyFeeFtthPre account " + account, ex);
			logger.error(AppManager.logException(timeSt, ex));
			result = null;
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public boolean checkFtthPospaid(long contractId, String account) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		boolean result = false;
		String sqlGetUser = "select a.account from sub_adsl_ll a where a.contract_id = ? and status = 2 and a.account not in "
				+ "(select account from sub_adsl_ll_prepaid where contract_id = ?)";
		Connection connection = null;
		try {
			logger.info("Start checkFtthPospaid for account " + account + " contractId " + contractId);
			connection = getConnection("cm_pos");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setLong(1, contractId);
			ps.setLong(2, contractId);
			rs = ps.executeQuery();
			if (rs.next()) {
				String tmp = rs.getString("account");
				if (tmp != null && tmp.trim().length() > 0) {
					result = true;
				}
			}
			logTimeDb("Time to checkFtthPospaid account " + account + " result " + result + " contract_id " + contractId, timeSt);
		} catch (Exception ex) {
			logger.error("Error checkFtthPospaid account " + account + " contractId " + contractId, ex);
			logger.error(AppManager.logException(timeSt, ex));
			result = false;
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public int updateExpireFtthPre(long ftthPreId, String account, Date expireTime) {
		int result = 0;
		long timeSt = System.currentTimeMillis();
		PreparedStatement ps = null;
		String sqlGetUser = "update cm_pos.sub_adsl_ll_prepaid set expire_time = ?, block_time = null, "
				+ " warning_count = 0  where sub_adsl_ll_prepaid_id = ?";
		Connection connection = null;
		try {
			connection = getConnection("cm_pos");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setTimestamp(1, new Timestamp(expireTime.getTime()));
			ps.setLong(2, ftthPreId);
			result = ps.executeUpdate();
			logTimeDb("Time to updateExpireFtthPre ftthPreId " + ftthPreId
					+ " account " + account + " expireTime " + expireTime + " result " + result, timeSt);
		} catch (Exception ex) {
			logger.error("Error updateExpireFtthPre ftthPreId " + ftthPreId + " account " + account + " expireTime " + expireTime, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public int insertActionAudit(long prepaidId, String account, String des, long subId, String ip) {
		Connection connection = null;
		PreparedStatement ps = null;
		StringBuilder br = new StringBuilder();
		String sql = "";
		int result = 0;
		long startTime = System.currentTimeMillis();
		try {
			connection = getConnection("cm_pos");
			sql = "INSERT INTO action_audit (ACTION_AUDIT_ID,ISSUE_DATETIME,ACTION_CODE,REASON_ID,SHOP_CODE,USER_NAME,"
					+ " PK_TYPE,PK_ID,IP,DESCRIPTION) "
					+ " VALUES(action_audit_seq.nextval,sysdate,'1000',302201,'SYSTEM','SYSTEM', "
					+ "'3',?,?,?)";
			ps = connection.prepareStatement(sql);
			ps.setLong(1, subId);
			ps.setString(2, ip);
			ps.setString(3, des);
			result = ps.executeUpdate();
			logger.info("End insertActionAudit prepaidId " + prepaidId + " account " + account
					+ " subId " + subId + " result " + result + " time "
					+ (System.currentTimeMillis() - startTime));
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR insertActionAudit: ").
					append(sql).append("\n")
					.append(" prepaidId ")
					.append(prepaidId)
					.append(" account ")
					.append(account)
					.append(" result ")
					.append(result);
			logger.error(br + ex.toString());
		} finally {
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public String getCenter(String account) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		Connection connection = null;
		PreparedStatement ps = null;
		String result = "";
		try {
			connection = ConnectionPoolManager.getConnection("cm_pos");
			ps = connection.prepareStatement("select cen_code from area where area_code = (select address_code from sub_adsl_ll where account = ? and status = 2)");
			if (QUERY_TIMEOUT > 0) {
				ps.setQueryTimeout(QUERY_TIMEOUT);
			}
			if (account.startsWith("258")) {
				account = account.substring(3);
			}
			ps.setString(1, account);
			rs = ps.executeQuery();
			while (rs.next()) {
				result = rs.getString("cen_code");
				break;
			}
			logTimeDb("Time to getCenter account " + account + " result: " + result, timeSt);
		} catch (Exception ex) {
			logger.error("ERROR getCenter defaul return empty, account " + account);
			logger.error(AppManager.logException(timeSt, ex));
			result = "";
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public InvoiceInfo getInvoiceInfo(Long staffId) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		InvoiceInfo user = null;
		String sqlGetUser = "select invoice_list_id,serial_no,block_no,curr_invoice_no FROM invoice_list a "
				+ " WHERE 1 = 1 AND a.BOOK_TYPE_ID in (select BOOK_TYPE_ID from book_type where book_type.invoice_type = 2) "
				+ " and a.staff_id = ? and a.status = 3";
		Connection connection = null;
		try {
			logger.info("Start get invoice info of staff " + staffId);
			connection = getConnection("dbsm");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setLong(1, staffId);
			rs = ps.executeQuery();
			user = new InvoiceInfo();
			while (rs.next()) {
				user.setInvoiceListId(rs.getLong("invoice_list_id"));
				user.setSerialNo(rs.getString("serial_no"));
				user.setBlockNo(rs.getString("block_no"));
				user.setInvoiceNo(rs.getString("curr_invoice_no"));
				break;
			}
			logTimeDb("Time to getInvoiceInfo staffId " + staffId + " invoice_list_id " + user.getInvoiceListId(), timeSt);
		} catch (Exception ex) {
			logger.error("Error getInvoiceInfo", ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return user;
		}
	}

	public long genBillPay(Long contractId, Long money, String currType, Long staffId, String invoiceType,
			String serialNo, String bockNo, String invoiceNumber, Long con, String clientIp, Long groupId,
			String userName, String account) {
		Connection connection = null;
		CallableStatement cstmt = null;
		StringBuilder br = new StringBuilder();
		long res = 1;
		String desc = "";
		long startTime = System.currentTimeMillis();
		try {
			logger.info("Start genBillPay account " + account + " contractId " + contractId + " money " + money
					+ " currType " + currType
					+ " staffId " + staffId + " invoiceType " + invoiceType + " serialNo " + serialNo + " bockNo " + bockNo
					+ " invoiceNumber " + invoiceNumber + " con " + con + " clientIp "
					+ " groupId " + groupId + " userName " + userName);
			connection = getConnection("db_payment");
			cstmt = connection.prepareCall("{call payment.pck_pay113.vc_payment_direct(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}");
			cstmt.setLong("p_contract_id", contractId);
			cstmt.setLong("p_amount", money);
			cstmt.setString("p_currtype", "9");
			cstmt.setLong("p_staff_id", staffId);
			cstmt.setDate("p_receipt_date", new java.sql.Date(startTime));
			cstmt.setString("p_invoice_type", invoiceType);
			cstmt.setString("p_serial_no", serialNo);
			cstmt.setString("p_block_no", bockNo);
			cstmt.setString("p_invoice_number", invoiceNumber);
			cstmt.setLong("p_connection", 0);
			cstmt.setString("p_client_id", clientIp);
			cstmt.setLong("p_group_id", groupId);
			cstmt.setString("p_user_name", userName);
			cstmt.setLong("p_debit", res);
			cstmt.setString("p_error", desc);
			cstmt.execute();
			logger.info("End genBillPay time " + (System.currentTimeMillis() - startTime) + " res " + res + " desc " + desc);
			if (desc != null && desc.trim().length() > 0) {
				return 1;
			} else {
				return 0;
			}
		} catch (Exception ex) {
			br.setLength(0);
			br.append(loggerLabel).append(new Date()).
					append("\nERROR genBillPay account " + account);
			logger.error(br + ex.toString());
			logger.error(AppManager.logException(startTime, ex));
			return 1;
		} finally {
			closeStatement(cstmt);
			closeConnection(connection);
		}
	}

//    public boolean checkFundCode(String client, String fundCode) {
//        /**
//         * SELECT partner_code FROM client WHERE username = client AND status =
//         * 1;
//         */
//        ParamList paramList = new ParamList();
//        boolean result = false;
//        long timeSt = System.currentTimeMillis();
//        try {
//            paramList.add(new Param("username", client, Param.DataType.STRING, Param.IN));
//            paramList.add(new Param("fund_code", null, Param.DataType.STRING, Param.OUT));
//            DataResources rs = poolStore.selectTable(paramList, "ws_user");
//            while (rs.next()) {
//                String product = rs.getString("fund_code");
//                if (product != null && fundCode.trim().toUpperCase().equals(product.trim().toUpperCase())) {
//                    result = true;
//                    break;
//                }
//            }
//            logTimeDb("Time to checkFundCode, client " + client + ", fundCode " + fundCode + "  result: " + result, timeSt);
//        } catch (Exception ex) {
//            logger.error("ERROR checkFundCode default return false, fundCode " + fundCode + " client " + client);
//            logger.error(AppManager.logException(timeSt, ex));
//            result = false;
//        }
//        return result;
//    }
	public long checkTransCodeAvaiable(String bankName, String transCode, String amount, String checkUser) {
		long result = 0;
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		String sqlGetUser = "select * from bank_tranfer_info where upper(bank_name) = upper(?) and upper(tranfer_code) = upper(?) "
				+ " and amount = ? and status = 1 and (check_time is null or check_time < sysdate - 1)";
		Connection connection = null;
		try {
			connection = getConnection("dbsm");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, bankName);
			ps.setString(2, transCode);
			ps.setString(3, amount);
			rs = ps.executeQuery();
			if (rs.next()) {
				long temp = rs.getLong("bank_tranfer_info_id");
				if (temp > 0) {
					result = temp;
				}
			}
//            if (result <= 0 && bankName.equals("BIM")) {
//                logger.info("Check special case for BIM " + transCode + " amount " + amount + " checkUser " + checkUser);
//                String sqlBIM = "select * from bank_tranfer_info where upper(bank_name) = upper(?) and tranfer_code is null "
//                        + " and amount = ? and status = 1 and (check_time is null or check_time < sysdate - 15/(24*60))";
//                PreparedStatement ps2 = connection.prepareStatement(sqlBIM);
//                ps2.setString(1, bankName);
//                ps2.setString(2, amount);
//                ResultSet rs2 = ps2.executeQuery();
//                if (rs2.next()) {
//                    long temp = rs2.getLong("bank_tranfer_info_id");
//                    if (temp > 0) {
//                        result = temp;
//                    }
//                }
//            }
			logTimeDb("Time to checkTransCodeAvaiable transCode " + transCode + " bank_tranfer_info_id " + result, timeSt);
		} catch (Exception ex) {
			logger.error("Error checkTransCodeAvaiable transCode " + transCode, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public int updateTransCodeUsing(String bankName, String transCode, String amount, String checkUser, long bankTranferInfoId) {
		int result = 0;
		long timeSt = System.currentTimeMillis();
		PreparedStatement ps = null;
		String sqlGetUser = "update bank_tranfer_info set check_time = sysdate, check_user = ? "
				+ " where bank_tranfer_info_id = ?";
		Connection connection = null;
		try {
			connection = getConnection("dbsm");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, checkUser);
			ps.setLong(2, bankTranferInfoId);
			result = ps.executeUpdate();
			logTimeDb("Time to updateTransCodeUsing transCode " + transCode
					+ " bank_tranfer_info_id " + bankTranferInfoId + " result " + result, timeSt);
		} catch (Exception ex) {
			logger.error("Error updateTransCodeUsing " + " bank_tranfer_info_id " + bankTranferInfoId + " transCode " + transCode, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public long checkTransCodeBeforeUpdate(String bankName, String transCode, String amount, String checkUser) {
		long result = 0;
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		String sqlGetUser = "select * from bank_tranfer_info "
				+ " where upper(bank_name) = upper(?) and upper(tranfer_code) = upper(?) "
				+ " and amount = ? and status = 1 and check_time is not null and upper(check_user) = upper(?)";
		Connection connection = null;
		try {
			connection = getConnection("dbsm");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, bankName);
			ps.setString(2, transCode);
			ps.setString(3, amount);
			ps.setString(4, checkUser.trim());
			rs = ps.executeQuery();
			if (rs.next()) {
				long temp = rs.getLong("bank_tranfer_info_id");
				if (temp > 0) {
					result = temp;
				}
			}
//            if (result <= 0 && bankName.equals("BIM")) {
//                logger.info("Check special case for BIM " + transCode + " amount " + amount + " checkUser " + checkUser);
//                String sqlBIM = "select * from bank_tranfer_info where upper(bank_name) = upper(?) and tranfer_code is null "
//                        + " and amount = ? and status = 1 and check_time is not null and upper(check_user) = upper(?)";
//                PreparedStatement ps2 = connection.prepareStatement(sqlBIM);
//                ps2.setString(1, bankName);
//                ps2.setString(2, amount);
//                ps2.setString(3, checkUser.trim());
//                ResultSet rs2 = ps2.executeQuery();
//                if (rs2.next()) {
//                    long temp = rs2.getLong("bank_tranfer_info_id");
//                    if (temp > 0) {
//                        result = temp;
//                    }
//                }
//            }
			logTimeDb("Time to checkTransCodeBeforeUpdate transCode " + transCode + " bank_tranfer_info_id " + result, timeSt);
		} catch (Exception ex) {
			logger.error("Error checkTransCodeBeforeUpdate transCode " + transCode, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public int updateTransCodeUsed(String bankName, String transCode, String amount, String updateUser,
			String staffCreate, String staffApprove, String purpose, long bankTranferInfoId) {
		int result = 0;
		long timeSt = System.currentTimeMillis();
		PreparedStatement ps = null;
		String sqlGetUser = "update bank_tranfer_info set status = 0, update_time = sysdate, update_user = ? "
				+ ", STAFF_REQUEST = ?, STAFF_APPROVE = ?, PURPOSE = ?"
				+ " where bank_tranfer_info_id = ?";
		Connection connection = null;
		try {
			connection = getConnection("dbsm");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, updateUser);
			ps.setString(2, staffCreate);
			ps.setString(3, staffApprove);
			ps.setString(4, purpose);
			ps.setLong(5, bankTranferInfoId);
			result = ps.executeUpdate();
			logTimeDb("Time to updateTransCodeUsed transCode " + transCode + " result " + result
					+ " bankTranferInfoId " + bankTranferInfoId, timeSt);
		} catch (Exception ex) {
			logger.error("Error updateTransCodeUsed bankTranferInfoId " + bankTranferInfoId + " transCode " + transCode, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

    public int updateTransCodePending() {
        int result = 0;
        long timeSt = System.currentTimeMillis();
        PreparedStatement ps = null;
        String sqlGetUser = "update bank_tranfer_info set status = 5, update_time = sysdate, update_user = 'SYSTEM_AUTO' "
                + " where create_time < trunc(sysdate - 90) and status = 1 and (revert_time is null or revert_time < trunc(sysdate-3))";
        Connection connection = null;
        try {
            connection = getConnection("dbsm");
            ps = connection.prepareStatement(sqlGetUser);
            result = ps.executeUpdate();
            logTimeDb("Time to updateTransCodePending result " + result, timeSt);
        } catch (Exception ex) {
            logger.error("Error updateTransCodePending ", ex);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closePrepareStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public long checkTransCodeBeforeRollback(String bankName, String transCode, String amount, String checkUser) {
        long result = 0;
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        PreparedStatement ps = null;
        String sqlGetUser = "select * from bank_tranfer_info "
                + " where upper(bank_name) = upper(?) and upper(tranfer_code) = upper(?) "
                + " and amount = ? and status = 1 and check_time is not null and upper(check_user) = upper(?)";
        Connection connection = null;
        try {
            connection = getConnection("dbsm");
            ps = connection.prepareStatement(sqlGetUser);
            ps.setString(1, bankName);
            ps.setString(2, transCode);
            ps.setString(3, amount);
            ps.setString(4, checkUser.trim());
            rs = ps.executeQuery();
            if (rs.next()) {
                long temp = rs.getLong("bank_tranfer_info_id");
                if (temp > 0) {
                    result = temp;
                }
            }
            logTimeDb("Time to checkTransCodeBeforeRollback transCode " + transCode + " bank_tranfer_info_id " + result, timeSt);
        } catch (Exception ex) {
            logger.error("Error checkTransCodeBeforeRollback transCode " + transCode, ex);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closePrepareStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

	public int updateTransCodeRollback(String bankName, String transCode, String amount,
			long bankTranferInfoId, String reason, String user) {
		int result = 0;
		long timeSt = System.currentTimeMillis();
		PreparedStatement ps = null;
		String sqlGetUser = "update bank_tranfer_info set check_time = null, check_user = null, "
				+ "rollback_reason = ?, rollback_user = ?, rollback_time = sysdate "
				+ " where bank_tranfer_info_id = ?";
		Connection connection = null;
		try {
			connection = getConnection("dbsm");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, reason);
			ps.setString(2, user);
			ps.setLong(3, bankTranferInfoId);
			result = ps.executeUpdate();
			logTimeDb("Time to updateTransCodeRollback transCode " + transCode + " result " + result
					+ " bankTranferInfoId " + bankTranferInfoId, timeSt);
		} catch (Exception ex) {
			logger.error("Error updateTransCodeRollback bankTranferInfoId " + bankTranferInfoId + " transCode " + transCode, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public ArrayList<Long> getAllContract() {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		ArrayList<Long> listContract = new ArrayList<Long>();
		String sql = "SELECT contract_id "
				+ "FROM cm_pos.contract where reference_id is null";
		Connection connection = null;
		try {
			connection = getConnection("cm_pos");
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				long contractId = rs.getLong("contract_id");
				listContract.add(contractId);
			}
			logTimeDb("Time to getAllContract ", timeSt);
		} catch (Exception ex) {
			logger.error("Error getAllContract ", ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return listContract;
		}
	}

	public ArrayList<Long> getAllCounter() {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		ArrayList<Long> listContract = new ArrayList<Long>();
		String sql = "select staff_id from sm.staff where status = 1 and channel_type_id  = 14 and type = 5";
		Connection connection = null;
		try {
			connection = getConnection("dbsm");
			ps = connection.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				long contractId = rs.getLong("staff_id");
				listContract.add(contractId);
			}
			logTimeDb("Time to getAllCounter ", timeSt);
		} catch (Exception ex) {
			logger.error("Error getAllCounter ", ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return listContract;
		}
	}

	public int updateContract(long contractId, String referenceId) {
		int result = 0;
		long timeSt = System.currentTimeMillis();
		PreparedStatement ps = null;
		String sqlGetUser = "update cm_pos.contract set reference_id = ? "
				+ " where contract_id = ?";
		Connection connection = null;
		try {
			connection = getConnection("cm_pos");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, referenceId);
			ps.setLong(2, contractId);
			result = ps.executeUpdate();
			logTimeDb("Time to updateContract contractId " + contractId
					+ " referenceId " + referenceId + " result " + result, timeSt);
		} catch (Exception ex) {
			logger.error("Error updateContract contractId " + contractId + " referenceId " + referenceId, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public int updateCounter(long staffId, String referenceId) {
		int result = 0;
		long timeSt = System.currentTimeMillis();
		PreparedStatement ps = null;
		String sqlGetUser = "update sm.staff set reference_id = ? "
				+ " where staff_id = ?";
		Connection connection = null;
		try {
			connection = getConnection("dbsm");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, referenceId);
			ps.setLong(2, staffId);
			result = ps.executeUpdate();
			logTimeDb("Time to updateCounter staffId " + staffId
					+ " referenceId " + referenceId + " result " + result, timeSt);
		} catch (Exception ex) {
			logger.error("Error updateCounter staffId " + staffId + " referenceId " + referenceId, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
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
			if (msisdn == null || msisdn.trim().length() <= 0 || message == null || message.trim().length() <= 0) {
				logger.info("Can not sendSms invalid isdn " + msisdn + " message " + message + " result " + result + " time "
						+ (System.currentTimeMillis() - startTime));
			} else {
				connection = getConnection("db_payment");
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
			}
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

	public int updateSubAdslLL(String account) {
		int result = 0;
		long timeSt = System.currentTimeMillis();
		PreparedStatement ps = null;
		String sqlGetUser = "update cm_pos.sub_adsl_ll set act_status = '000' "
				+ " where account = ? and status = 2";
		Connection connection = null;
		try {
			connection = getConnection("cm_pos");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, account);
			result = ps.executeUpdate();
			logTimeDb("Time to updateSubAdslLL "
					+ " account " + account + " result " + result, timeSt);
		} catch (Exception ex) {
			logger.error("Error updateSubAdslLL " + " account " + account, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

	public String checkTopupHis(String requestId) {
		String result = "";
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		String sqlGetUser = "select * from topup_log where start_date > trunc(sysdate-30) and request_id = ?";
		Connection connection = null;
		try {
			connection = getConnection("dbtopup");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, requestId);
			rs = ps.executeQuery();
			if (rs.next()) {
				String output = rs.getString("output");
				String money = rs.getString("money");
				if (output != null && output.trim().length() > 0) {
					result = result + output + "|" + money;
				}
			}
			logTimeDb("Time to checkTopupHis requestId " + requestId + " result " + result, timeSt);
		} catch (Exception ex) {
			logger.error("Error checkTopupHis requestId " + requestId, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			if (result == null || result.trim().length() <= 1) {
				result = "2|Not exist trans with your requestId|0";
			}
			return result;
		}
	}

	public int insertMakeTransInput(String client, String money) {
		int result = 0;
		long timeSt = System.currentTimeMillis();
		PreparedStatement ps = null;
		String sqlGetUser = "INSERT INTO make_trans_input (CLIENT,MONEY,ID,TRANS_DATE,TABLE_NAME,SERVICE_TYPE,MAKE_TRANS_STATUS) \n"
				+ "VALUES(?,?,make_trans_input_seq.nextval,sysdate,'make_trans_input','pre',NULL)";
		Connection connection = null;
		try {
			connection = getConnection("dbtopup");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, client);
			ps.setDouble(2, Double.valueOf(money));
			result = ps.executeUpdate();
			logTimeDb("Time to insertMakeTransInput "
					+ " client " + client + " money " + money + " result " + result, timeSt);
		} catch (Exception ex) {
			logger.error("Error insertMakeTransInput " + " client " + client + " money " + money, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closePrepareStatement(ps);
			closeConnection(connection);
			return result;
		}
	}

    public int insertMakeTransInputWithName(String client, String money, String custName, String address, String nuit) {
        int result = 0;
        long timeSt = System.currentTimeMillis();
        PreparedStatement ps = null;
        String sqlGetUser = "INSERT INTO make_trans_input (CLIENT,MONEY,ID,TRANS_DATE,TABLE_NAME,SERVICE_TYPE,MAKE_TRANS_STATUS,CUST_NAME,ADDRESS,NUIT) \n"
                + "VALUES(?,?,make_trans_input_seq.nextval,sysdate,'make_trans_input','pre',NULL,?,?,?)";
        Connection connection = null;
        try {
            connection = getConnection("dbtopup");
            ps = connection.prepareStatement(sqlGetUser);
            ps.setString(1, client);
            ps.setDouble(2, Double.valueOf(money));
            ps.setString(3, custName);
            ps.setString(4, address);
            ps.setString(5, nuit);
            result = ps.executeUpdate();
            logTimeDb("Time to insertMakeTransInput "
                    + " client " + client + " money " + money + " result " + result, timeSt);
        } catch (Exception ex) {
            logger.error("Error insertMakeTransInput " + " client " + client + " money " + money, ex);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closePrepareStatement(ps);
            closeConnection(connection);
            return result;
        }
    }

    public int renewalSubMbFtth(Long subId, Date addday, Long addmonth, boolean checkExpireTimeAndSysdate) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        int res = 0;
        String tempSQL = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = getConnection("cm_pos");
            if (checkExpireTimeAndSysdate) {
                tempSQL = "expire_time =  ? \n";
            } else {
                tempSQL = "expire_time =  ? \n";
            }
            String sql = "update sub_mb_ftth set status = 1, renewal_time = sysdate, blocked_time = null, prepaid_type = ?, add_policy_time = sysdate, add_policy_next_time = sysdate, blocked_time_by_process = null,\n"
                    + tempSQL
                    + "where account = (select account from sub_req_adsl_ll where sub_id = ?) and status !=2";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, addmonth);
            ps.setTimestamp(2, new Timestamp(addday.getTime()));
            ps.setLong(3, subId);
            res = ps.executeUpdate();
            logger.info("End renewalSubMbFtth time " + (System.currentTimeMillis() - startTime) + "subId : " + subId + "prepaidType : " + addmonth
                    + "subId : " + subId);
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR renewalSubMbFtth subId ").append(subId);
            logger.error(br + "Exception : " + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public String[] checkDebitInWsuser() {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        PreparedStatement ps = null;
        String[] ls = new String[2];
        String sql = " select debit_status,debit_value from ws_user  ";
        Connection connection = null;
        try {
            connection = poolStore.getConnection();
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String debitStatus = rs.getString("debit_status");
                String debitValue = rs.getString("debit_value");
                ls[0] = debitStatus;
                ls[1] = debitValue;
            }
            logTimeDb("Time to checkDebitInWsuser ", timeSt);
        } catch (Exception ex) {
            logger.error("Error checkDebitInWsuser ", ex);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closePrepareStatement(ps);
            closeConnection(connection);
            return ls;
        }
    }

    public int insertMO(String msisdn, String param, String dbId, String headNum, String actionType, String command) throws Exception {
        int result = 0;
        PreparedStatement ps = null;
        Connection connection = null;
        String SQL_INSERT_MO = "INSERT INTO mo (MO_ID,MSISDN,COMMAND,PARAM,RECEIVE_TIME,ACTION_TYPE,CHANNEL,CHANNEL_TYPE) "
                + "VALUES(mo_seq.nextval,?,?,?,sysdate,?,?,'MPS')";
        try {
            connection = ConnectionPoolManager.getConnection(dbId);
            ps = connection.prepareStatement(SQL_INSERT_MO);
            ps.setQueryTimeout(AppManager.queryDbTimeout);
            ps.setString(1, msisdn);
            ps.setString(2, command);
            ps.setString(3, param);
            ps.setString(4, actionType);
            ps.setString(5, headNum);
            result = ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("Fail to insert MO msisdn " + msisdn + ex.toString());
            throw ex;
        } finally {
            closePrepareStatement(ps);
            closeConnection(connection);
            logger.info("Finish insert MO msisdn " + msisdn + " result " + result);
        }
        return result;
    }

    public int insertGp(String msisdn, String param, String dbId) throws Exception {
        int result = 0;
        PreparedStatement ps = null;
        Connection connection = null;
        String SQL_INSERT_MO = "insert into os_sms_input (sms_trans_code, msisdn, msg, time_receive, id)\n"
                + "values (?, ?, ?, sysdate, os_sms_input_seq.nextval)";
        try {
            connection = ConnectionPoolManager.getConnection(dbId);
            ps = connection.prepareStatement(SQL_INSERT_MO);
            ps.setQueryTimeout(AppManager.queryDbTimeout);
            ps.setString(1, "S-" + msisdn + "-" + sdf.format(new Date()));
            ps.setString(2, msisdn);
            ps.setString(3, param);
            result = ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("Fail to insertGp msisdn " + msisdn + ex.toString());
            throw ex;
        } finally {
            closePrepareStatement(ps);
            closeConnection(connection);
            logger.info("Finish insertGp msisdn " + msisdn + " result " + result);
        }
        return result;
    }

    public long getCustId(Long contractId) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs = null;
        PreparedStatement ps = null;
        String sqlGetCustId = "select cust_id from contract where contract_id =? and status =2 ";
        Connection connection = null;
        long custId = 0;
        try {
            logger.info("Start get getCustId info of staff " + sqlGetCustId);
            connection = getConnection("cm_pos");
            ps = connection.prepareStatement(sqlGetCustId);
            ps.setLong(1, contractId);
            rs = ps.executeQuery();
            while (rs.next()) {
                custId = rs.getLong("cust_id");
                break;
            }
            logTimeDb("Time to getCustId contractId " + contractId, timeSt);
        } catch (Exception ex) {
            logger.error("Error getInvoiceInfo", ex);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            closeResultSet(rs);
            closePrepareStatement(ps);
            closeConnection(connection);
            return custId;
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

    public boolean checkSubAlreadyExtendAnotherGroup(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "select * from kit_batch_extend where isdn = ? and result_code = '0'";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                String phone = rs.getString("isdn");
                if (phone != null) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkSubAlreadyExtendAnotherGroup isdn " + isdn
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkSubAlreadyExtendAnotherGroup ---- isdn ").
                    append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkSubAlreadyExtendAnotherGroup2(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "select * from kit_batch_detail where isdn = ? and result_code = '0'";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                String phone = rs.getString("isdn");
                if (phone != null) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkSubAlreadyExtendAnotherGroup2 isdn " + isdn
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkSubAlreadyExtendAnotherGroup2 ---- isdn ").
                    append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public String getProductCodeByIsdn(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String productCode = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "select * from cm_pre.sub_mb where isdn = ? and status = 2 and act_status = '00'";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                productCode = rs.getString("product_code");
            }
            logger.info("End getProductCodeByIsdn isdn " + isdn
                    + " productCode " + productCode + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getProductCodeByIsdn ---- isdn ").
                    append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return productCode;
        }
    }

    public boolean checkKitElitePrepaid(String isdn, String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean result = false;
        StringBuilder btBuilder = new StringBuilder();
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            String sql = "select * from kit_elite_prepaid where isdn = ? and product_code = ? and status = 1 and remain_month > 0";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setString(2, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                logger.info("checkKitElitePrepaid isdn " + isdn + " result " + Boolean.TRUE);
                result = true;
                break;
            }
            logger.info("End checkKitElitePrepaid isdn " + isdn
                    + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            btBuilder.setLength(0);
            btBuilder.append(loggerLabel).append(new Date()).append("\nERROR checkKitElitePrepaid ---- isdn ").append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(btBuilder + ex.toString());
            return result;
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
        }
        return result;

    }

    public String getProductCodeKitElitePrepaid(String isdn, String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        String rsProductCode = "";
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "select * from kit_elite_prepaid where isdn = ? and product_code = ? and status = 1 and remain_month > 0";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setString(2, productCode);
            rs = ps.executeQuery();
            while (rs.next()) {
                rsProductCode = rs.getString("product_code");
            }
            logger.info("End getProductCodeKitElitePrepaid isdn " + isdn
                    + " productCode " + productCode + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getProductCodeKitElitePrepaid ---- isdn ").
                    append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return rsProductCode;
        }
    }

    public int updateKitElitePrepaid(String isdn, String remainTime, int paidMonth, boolean isExpire, String productCode) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int res = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "update kit_elite_prepaid set prepaid_month = prepaid_month + ?, remain_month = remain_month + ?, "
                    + "remain_time = trunc(to_date(?,'yyyyMMddhh24miss') - ?)\n"
                    + "where isdn = ? and status = 1 and product_code = ?";
            ps = connection.prepareStatement(sql);
            ps.setInt(1, paidMonth);
            ps.setInt(2, paidMonth);
            ps.setString(3, remainTime);
            if (isExpire) {
                ps.setInt(4, 31);
            } else {
                ps.setInt(4, 30);
            }
            ps.setString(5, isdn);
            ps.setString(6, productCode);
            res = ps.executeUpdate();
            logger.info("End updateKitElitePrepaid time " + (System.currentTimeMillis() - startTime) + "isdn" + isdn + "remainTime: " + remainTime + "productCode: " + productCode);
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR updateKitElitePrepaid isdn ").append(isdn).append("remainTime: ").append(remainTime).append("productCode: ").append(productCode);
            logger.error(br + "Exception : " + ex.toString());
        } finally {
            closeStatement(ps);
            closeConnection(connection);
            return res;
        }
    }

    public boolean checkElitePackage(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "select * from cm_pre.sub_mb where isdn = ? and status = 2 \n"
                    + "and product_code in (select product_code from product.product_connect_kit \n"
                    + "where status = 1 and vip_product = 1 and product_code <> 'DATA_SIM')";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                String phone = rs.getString("isdn");
                if (phone != null) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkElitePackage isdn " + isdn
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkElitePackage ---- isdn ").
                    append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkBranchPromotionSub(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        if (!isdn.startsWith("258")) {
            isdn = "258" + isdn;
        }
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("dbElite");
            sql = "select * from branch_promotion_sub where isdn = ? and expire_time > trunc(sysdate)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                String phone = rs.getString("isdn");
                if (phone != null) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkElitePackage isdn " + isdn
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkElitePackage ---- isdn ").
                    append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public long getSubId(String isdn) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        long subId = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "select * from cm_pre.sub_mb where isdn = ? and status = 2";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            rs = ps.executeQuery();
            while (rs.next()) {
                subId = rs.getLong("sub_id");
            }
            logger.info("End getSubId isdn " + isdn
                    + " getSubId " + subId + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getSubId ---- isdn ").
                    append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return subId;
        }
    }

    public boolean checkSubExtendOnCurrentGroup(String isdn, Long kitBatchId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result = false;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "select * from kit_batch_detail where isdn = ? "
                    + "and (kit_batch_id = ?  or kit_batch_id in (select kit_batch_id from kit_batch_info where extend_from_kit_batch_id = ?))";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setLong(2, kitBatchId);
            ps.setLong(3, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String phone = rs.getString("isdn");
                if (phone != null) {
                    result = true;
                    break;
                }
            }
            logger.info("End checkSubExtendOnCurrentGroup isdn " + isdn
                    + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkSubExtendOnCurrentGroup ---- isdn ").
                    append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result;
        }
    }

    public boolean checkSubExtendOnCurrentGroup2(String isdn, Long kitBatchId) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        boolean result2 = false;

        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "select * from kit_batch_extend where isdn = ? and kit_batch_id = ? and result_code = '0'";
            ps = connection.prepareStatement(sql);
            ps.setString(1, isdn);
            ps.setLong(2, kitBatchId);
            rs = ps.executeQuery();
            while (rs.next()) {
                String phone = rs.getString("isdn");
                if (phone != null) {
                    result2 = true;
                    break;
                }
            }

            logger.info("End checkSubExtendOnCurrentGroup isdn " + isdn
                    + " result " + result2 + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR checkSubExtendOnCurrentGroup ---- isdn ").
                    append(isdn).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeStatement(ps);
            closeResultSet(rs);
            closeConnection(connection);
            return result2;
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
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "select distinct cug_name from cm_pre.kit_batch_detail where kit_batch_id = ? and cug_id is not null";
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

    public int insertKitElitePrepaid(String isdn, int paidMonth, int kitBatchId, String createUser, String productCode, String remainTime, boolean isExpire) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "INSERT INTO kit_elite_prepaid (id,prepaid_month,remain_month,prepaid_type,status,"
                    + "create_time,remain_time,create_user,product_code,isdn, kit_batch_id) \n"
                    + "VALUES(kit_elite_prepaid_seq.nextval,?,?,1,1,sysdate,trunc(to_date(?,'yyyyMMddhh24miss') - ?),?,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setInt(1, paidMonth);
            ps.setInt(2, paidMonth);
            ps.setString(3, remainTime);
            if (isExpire) {
                ps.setInt(4, 31);
            } else {
                ps.setInt(4, 30);
            }
            ps.setString(5, createUser);
            ps.setString(6, productCode);
            ps.setString(7, isdn);
            ps.setLong(8, kitBatchId);
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

    public Long getSequence(String sequenceName, String connName) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        Long sequenceId = 0L;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection(connName);
            String sql = " select " + sequenceName + ".nextval as sequence_id from dual";
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                sequenceId = rs.getLong("sequence_id");
            }
            logger.info("End getSequence with idNo: " + sequenceName + "time: "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).append("\nERROR getSequence: ------ idNo: ")
                    .append(sequenceName).append(" Message: ").
                    append(ex.getMessage());
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(connection);
            return sequenceId;
        }
    }

    public int insertActionAuditCmPre(Long actionAuditId, String actionCode, long reasonId, String shopCode, String staffCode, long subId, String ip, String des) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "INSERT INTO action_audit (ACTION_AUDIT_ID,ISSUE_DATETIME,ACTION_CODE,REASON_ID,SHOP_CODE,USER_NAME,"
                    + " PK_TYPE,PK_ID,IP,DESCRIPTION) "
                    + " VALUES(?,sysdate,?,?,?,?, "
                    + "'3',?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, actionAuditId);
            ps.setString(2, actionCode);
            ps.setLong(3, reasonId);
            ps.setString(4, shopCode);
            ps.setString(5, staffCode);
            ps.setLong(6, subId);
            ps.setString(7, ip);
            ps.setString(8, des);
            result = ps.executeUpdate();
            logger.info("End insertActionAuditV2 actionCode " + actionCode + " staffCode " + staffCode + " reasonId " + reasonId
                    + " des " + des
                    + " subId " + subId + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));

            // 20180227 HopPD Start add danh sach nhan tin nhan 
            String receiveMessageList = Config.getConfig(Config.receiveMessageList, logger);
            if (receiveMessageList != null) {
                String[] arrReceiveMessage = receiveMessageList.split("\\;");
                for (String receiveMessage : arrReceiveMessage) {
                    sendSms(receiveMessage, des, "86904");
                }
            }
            // 20180227 HopPD End add danh sach nhan tin nhan

        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertActionAuditV2: ").
                    append(sql).append("\n")
                    .append(" actionCode ")
                    .append(actionCode)
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

    public int insertKitRenewGroupHis(Long kitBatchId, String isdn, String productCode, String staffCode, String bankName, String bankTransCode,
            String bankAmount, double moneyProduct, double totalMoneyBatch, String pricePlanCode, String expireTime, int paidMonth,
            double totalDiscount, String discoutRate) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            sql = "insert into kit_renew_group_his (kit_batch_id, isdn, product_code, staff_code, bank_name, bank_trans_code, bank_amount, money_product, total_money_batch, price_plan_code, expire_time,create_time, paid_month, total_discount, discount_rate)\n"
                    + "values (?,?,?,?,?,?,?,?,?,?,?,sysdate,?,?,?)";
            ps = connection.prepareStatement(sql);
            ps.setLong(1, kitBatchId);
            ps.setString(2, isdn);
            ps.setString(3, productCode);
            ps.setString(4, staffCode);
            ps.setString(5, bankName);
            ps.setString(6, bankTransCode);
            ps.setString(7, bankAmount);
            ps.setDouble(8, moneyProduct);
            ps.setDouble(9, totalMoneyBatch);
            ps.setString(10, pricePlanCode);
            ps.setString(11, expireTime);
            ps.setInt(12, paidMonth);
            ps.setDouble(13, totalDiscount);
            ps.setString(14, discoutRate);
            result = ps.executeUpdate();
            logger.info("End insertKitRenewGroupHis isdn " + isdn + " staff_code " + staffCode + " result " + result + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            br.setLength(0);
            br.append(loggerLabel).append(new Date()).
                    append("\nERROR insertKitRenewGroupHis: ").
                    append(sql).append("\n")
                    .append(" isdn ")
                    .append(isdn)
                    .append(" staff_code ")
                    .append(staffCode);
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
            connection = ConnectionPoolManager.getConnection("dbsm");
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
            return result != null ? result.toString() : null;
        }
    }

    public int insertSaleTrans(Long saleTransId, Long shopId, Long staffId, Long saleServiceId, Long saleServicePriceId, Double amountTax,
            Long subId, String isdn, Long reasonId, String ewalletRequestId, int payMethod, String bankTransCode, double discount) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
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
            ps.setString(12, isdn);
            ps.setLong(13, reasonId);
            String saleTransCode = "SS0000" + String.format("%0" + 9 + "d", saleTransId);
            ps.setString(14, saleTransCode);
            if ((payMethod == 1 && ewalletRequestId != null && ewalletRequestId.length() > 0) || amountTax == 0) {
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

    public int insertSaleTransDetail(Long saleTransDetailId, Long saleTransId, String stockModelId, String priceId, String saleServiceId, String saleServicePriceId,
            String stockTypeId, String stockTypeName, String stockModelCode, String stockModelName, String saleServicesCode, String saleServicesName, String accountModelCode,
            String accountModelName, String saleServicesPriceVat, String priceVat, String price, String saleServicesPrice, Double amountTax, Double discountAmout) {

        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("dbsm");
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
            ps.setString(5, saleServiceId);
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
                ps.setDouble(21, discountAmout / 1.17);
                ps.setDouble(22, (amountTax - discountAmout) / 1.17);
                ps.setDouble(23, (amountTax - discountAmout) - (amountTax - discountAmout) / 1.17);
                ps.setDouble(24, amountTax - discountAmout);
            } else {
                ps.setString(21, "");
                ps.setDouble(22, amountNotTax);
                ps.setDouble(23, tax);
                ps.setDouble(24, amountTax);
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

    public int insertLogMakeSaleTransFail(Long saleTransId, String isdn, String serial, Long shopId, Long staffId, Long saleServicesId, Long saleServicesPriceId,
            Long reasonId, Long saleTransDetailId, Long saleTransSerialId, String tableName) {
        Connection connection = null;
        PreparedStatement ps = null;
        StringBuilder br = new StringBuilder();
        String sql = "";
        int result = 0;
        long startTime = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
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

    /**
     * count number of change pck time
     *
     * @param isdn
     * @param productCode
     * @return
     */
    public int countChangeReg48h(String isdn, String productCode) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        PreparedStatement psMo = null;
        int count = 0;
        String sql = " select isdn as pck_times from kit_vas where isdn = ? and lower(product_code) = ? "
                + " and status =1  and create_date >= sysdate - 48/24 ";
        try {
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sql);
            psMo.setString(1, isdn);
            psMo.setString(2, productCode.toLowerCase());
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                count = count + 1;
            }
            logger.info("End countChangePck isdn " + isdn + " ,count " + count + " time "
                    + (System.currentTimeMillis() - timeSt));
            return count;
        } catch (Exception ex) {
            logger.error(logger.getClass() + "--> countChangePck ERROR isdn " + isdn);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return 0;
    }

    public boolean checkSubChangeBuymore48h(String isdn, String param) {
        long timeSt = System.currentTimeMillis();
        ResultSet rs1 = null;
        Connection connection = null;
        PreparedStatement psMo = null;
        boolean result = true;
        if (!isdn.startsWith("258")) {
            isdn = "258" + isdn;
        }
        if (param.startsWith("258")) {
            param = param.substring(3);
        }
        String sql = "select * from mo_his where msisdn = ? %SQL% "
                + "and err_code = '0' and action_type in (2,99,88) and process_time >= sysdate - 48/24 ";
        if (!param.isEmpty()) {
            sql = sql.replace("%SQL%", "and param like '%" + param + "%'");
        } else {
            sql = sql.replace("%SQL%", "");
        }
        try {
            connection = ConnectionPoolManager.getConnection("dbElite");
            psMo = connection.prepareStatement(sql);
            psMo.setString(1, isdn);
            rs1 = psMo.executeQuery();
            while (rs1.next()) {
                result = false;
                break;
            }
            logger.info("End checkSubRenew48hours isdn " + isdn + " ,result " + result + " time "
                    + (System.currentTimeMillis() - timeSt));
        } catch (Exception ex) {
            br.setLength(0);
            logger.error(logger.getClass() + "--> checkSubRenew48hours ERROR isdn " + isdn);
            logger.error(br + ex.toString());
        } finally {
            closeResultSet(rs1);
            closeStatement(psMo);
            closeConnection(connection);
        }
        return result;
    }

    public ArrayList<Subscriber> getAllSub(String enterpiseWallet) {
        ArrayList<Subscriber> listSubs = new ArrayList<Subscriber>();
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement psMo = null;
        String sql = "SELECT   c.isdn, c.product_code "
                + "  FROM   kit_batch_info a, kit_batch_detail b, sub_mb c "
                + " WHERE       a.kit_batch_id = b.kit_batch_id "
                + "         AND (a.enterprise_wallet = '" + enterpiseWallet + "' or a.kit_batch_id=0) "
                + "         AND b.isdn = c.isdn "
                + "         AND c.status = 2 "
                + "         AND b.result_code ='0' AND a.total_success > 0 "
                + " UNION "
                + " SELECT   c.isdn, c.product_code "
                + "  FROM   kit_batch_info a, kit_batch_extend b, sub_mb c "
                + " WHERE       a.kit_batch_id = b.kit_batch_id "
                + "         AND (a.enterprise_wallet = '" + enterpiseWallet + "' or a.kit_batch_id=0) "
                + "         AND b.isdn = c.isdn "
                + "         AND c.status = 2 "
                + "         AND b.result_code ='0'  AND a.total_success > 0 ";
        long timeStart = System.currentTimeMillis();
        try {
            connection = ConnectionPoolManager.getConnection("cm_pre");
            psMo = connection.prepareStatement(sql);
            rs = psMo.executeQuery();
            while (rs.next()) {
                Subscriber sub = new Subscriber();
                sub.setIsdn(rs.getString("isdn"));
                sub.setProductCode(rs.getString("product_code"));
                listSubs.add(sub);
            }
            logTimeDb("Time to getAllSub, total subs " + listSubs.size(), timeStart);
            return listSubs;
        } catch (Exception ex) {
            logger.error("ERROR getAllSub ", ex);
            AppManager.logException(timeStart, ex);
        } finally {
            closeStatement(psMo);
            closeConnection(connection);
            return null;
        }
    }

    public String getPricePlanOfProduct(String productCode) {
        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement psMo = null;
        String ppBuyMore = "";
        long timeSt = System.currentTimeMillis();
        String sql = "select * from product.product_connect_kit where product_code = ? ";
        try {
            connection = ConnectionPoolManager.getConnection("product");
            psMo = connection.prepareStatement(sql);
            psMo.setString(1, productCode);
            rs = psMo.executeQuery();
            while (rs.next()) {
                ppBuyMore = rs.getString("pp_buy_more");
                break;
            }
            logTimeDb("Time to getPricePlanOfProduct, pp_buy_more " + ppBuyMore, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getPricePlanOfProduct ", ex);
            AppManager.logException(timeSt, ex);
        } finally {
            closeStatement(psMo);
            closeResultSet(rs);
            closeConnection(connection);
            return ppBuyMore;
        }
    }

    public int insertMoV2(long moId, String msisdn, String command, String param, String actionType, String channel, String channelType) throws Exception {
        int result = 0;
        PreparedStatement ps = null;
        Connection connection = null;
        String SQL_INSERT_MO = "INSERT INTO mo (MO_ID,MSISDN,COMMAND,PARAM,RECEIVE_TIME,ACTION_TYPE,CHANNEL,CHANNEL_TYPE) "
                + "VALUES(?,?,?,?,sysdate,?,?,?)";
        try {
            connection = ConnectionPoolManager.getConnection("dbvas");
            ps = connection.prepareStatement(SQL_INSERT_MO);
            ps.setQueryTimeout(AppManager.queryDbTimeout);
            ps.setLong(1, moId);
            ps.setString(2, msisdn);
            ps.setString(3, command);
            ps.setString(4, param);
            ps.setString(5, actionType);
            ps.setString(6, channel);
            ps.setString(7, channelType);
            result = ps.executeUpdate();
        } catch (Exception ex) {
            logger.error("Fail to insert MO msisdn " + msisdn + ex.toString());
            throw ex;
        } finally {
            closePrepareStatement(ps);
            closeConnection(connection);
            logger.info("Finish insert MO msisdn " + msisdn + " result " + result);
        }
        return result;
    }
	
	
	public long getVipSubInfoByRefer(String ref) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		long vipsupInfoId = 0;
		String sqlGetUser = "select vip_sub_info_id from vip_sub_info where  reference_id =?";
		Connection connection = null;
		try {
			logger.info("Start getVipSubInfoByRefer for ref " + ref);
			connection = getConnection("dbvipsub");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, ref);
			rs = ps.executeQuery();
			if (rs.next()) {
				vipsupInfoId = rs.getLong("vip_sub_info_id");
			}
			logTimeDb("Time to getVipSubInfoByRefer ref " + ref + " contract_id " + vipsupInfoId, timeSt);
		} catch (Exception ex) {
			logger.error("Error getVipSubInfoByRefer ref " + ref, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return vipsupInfoId;
		}
	}

	public double getTotalAmountVipsub(long vipsubId) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		double totalAmount = 0;
		String sqlGetUser = "select sum(each_money) money from vip_sub_detail where vip_sub_info_id = ? and status =6 ";
		Connection connection = null;
		try {
			logger.info("Start getTotalAmountVipsub for vipsubId " + vipsubId);
			connection = getConnection("dbvipsub");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setLong(1, vipsubId);
			rs = ps.executeQuery();
			if (rs.next()) {
				totalAmount = rs.getDouble("money");
			}
			logTimeDb("Time to getTotalAmountVipsub vipsubId " + vipsubId + " totalAmount " + totalAmount, timeSt);
		} catch (Exception ex) {
			logger.error("Error getTotalAmountVipsub vipsubId " + vipsubId, ex);
			logger.error(AppManager.logException(timeSt, ex));
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
			return totalAmount;
		}
	}

	public String getVipsubPolicyPriority(long vipsubId, List<String> ignoreId) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		String policyId = "0";
		String subIgnoreSql = "";
		if (ignoreId != null && !ignoreId.isEmpty()) {
			for (String id : ignoreId) {
				subIgnoreSql += " and policy_id <> '" + id + "' ";
			}
		}
		String sqlGetUser = "select policy_id,each_money from (\n"
				+ "select max(each_money) keep(dense_rank first order by each_money) each_money\n"
				+ "     , policy_id\n"
				+ "  from vip_sub_detail where vip_sub_info_id = ? and status =6 \n" + subIgnoreSql
				+ " group by policy_id\n"
				+ " ) order  by each_money desc";
		Connection connection = null;
		try {
			logger.info("Start getVipsubPolicyPriority for vipsubId " + vipsubId);
			connection = getConnection("dbvipsub");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setLong(1, vipsubId);
			rs = ps.executeQuery();
			while (rs.next()) {
				policyId = rs.getString("policy_id");
				break;
			}
			logTimeDb("Time to getVipsubPolicyPriority vipsubId " + vipsubId + " policyId " + policyId, timeSt);
		} catch (Exception ex) {
			logger.error("Error getVipsubPolicyPriority vipsubId " + vipsubId, ex);
			logger.error(AppManager.logException(timeSt, ex));
			return "-1";
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
		}
		return policyId;
	}

	public double getTotalMoneyOfPolicy(String policyId) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		double totalAmount = 0;
		String sqlGetUser = "select sum(each_money) total_money from vip_sub_detail where policy_id=? and status =6";
		Connection connection = null;
		try {
			logger.info("Start getTotalAmountVipsub for policyId " + policyId);
			connection = getConnection("dbvipsub");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setString(1, policyId);
			rs = ps.executeQuery();
			while (rs.next()) {
				totalAmount = rs.getDouble("total_money");
				break;
			}
			logTimeDb("Time to getTotalMoneyOfPolicy policyId " + policyId + " totalAmount " + totalAmount, timeSt);
		} catch (Exception ex) {
			logger.error("Error getTotalMoneyOfPolicy policyId " + policyId, ex);
			logger.error(AppManager.logException(timeSt, ex));
			return 0;
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
		}
		return totalAmount;
	}

	public int insertVipSubPaymentRemain(long vipsubId, double remainAmount, double lockedMoney) {
		long timeSt = System.currentTimeMillis();
		PreparedStatement ps = null;
		String sqlGetUser = "insert into vip_sub_prepaid (id,vip_sub_info_id,payment_remain,create_time,locked_money,last_renew_scan_time=null ) values(vip_sub_prepaid_seq.nextval,?,?,sysdate,?)";
		Connection connection = null;
		int result = 0;
		try {
			logger.info("Start insertVipSubPaymentRemain for vipsubId " + vipsubId);
			connection = getConnection("dbvipsub");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setLong(1, vipsubId);
			ps.setDouble(2, remainAmount);
			ps.setDouble(3, lockedMoney);
			result = ps.executeUpdate();
			logTimeDb("End to insertVipSubPaymentRemain vipsubId " + vipsubId, timeSt);
		} catch (Exception ex) {
			logger.error("Error insertVipSubPaymentRemain vipsubId " + vipsubId, ex);
			logger.error(AppManager.logException(timeSt, ex));
			return 0;
		} finally {
			closePrepareStatement(ps);
			closeConnection(connection);
		}
		return result;
	}

	public int changeToWattingConfirmVipSub(long vipsubId) {
		long timeSt = System.currentTimeMillis();
		PreparedStatement ps = null;
		String sqlGetUser = "update vip_sub_info set status =8 where vip_sub_info_id =?";
		Connection connection = null;
		int result = 0;
		try {
			logger.info("Start changeToWattingConfirmVipSub for vipsubId " + vipsubId);
			connection = getConnection("dbvipsub");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setLong(1, vipsubId);
			result = ps.executeUpdate();
			logTimeDb("End to changeToWattingConfirmVipSub vipsubId " + vipsubId, timeSt);
		} catch (Exception ex) {
			logger.error("Error changeToWattingConfirmVipSub vipsubId " + vipsubId, ex);
			logger.error(AppManager.logException(timeSt, ex));
			return 0;
		} finally {
			closePrepareStatement(ps);
			closeConnection(connection);
		}
		return result;
	}

	public int changeToWattingConfirmVipSubDetail(long vipsubId, List<String> policyId) {
		long timeSt = System.currentTimeMillis();
		PreparedStatement ps = null;
		String subSql = "";

		if (policyId != null && !policyId.isEmpty()) {
			String idSrt = "";
			for (String id : policyId) {
				idSrt += "'" + id + "',";
			}
			subSql = " and policy_id in (" + idSrt.substring(0, idSrt.length() - 1) + ")";
		}

		String sql = "update vip_sub_detail set status=7,payment_method =3,prepaid_month=0,remain_prepaid_month=0, next_process_time = sysdate"
				+ "  where vip_sub_info_id =? and status =6 " + subSql;
		Connection connection = null;
		int result = 0;
		try {
			logger.info("Start changeToWattingConfirmVipSubDetail for vipsubId " + vipsubId);
			connection = getConnection("dbvipsub");
			ps = connection.prepareStatement(sql);
			ps.setLong(1, vipsubId);
			result = ps.executeUpdate();
			logTimeDb("End to changeToWattingConfirmVipSubDetail vipsubId " + vipsubId, timeSt);
		} catch (Exception ex) {
			logger.error("Error changeToWattingConfirmVipSubDetail vipsubId " + vipsubId, ex);
			logger.error(AppManager.logException(timeSt, ex));
			return 0;
		} finally {
			closePrepareStatement(ps);
			closeConnection(connection);
		}
		return result;
	}

	public long getMoneyPrepaidOfVipsub(long vipsubIbfoId) {
		long timeSt = System.currentTimeMillis();
		ResultSet rs = null;
		PreparedStatement ps = null;
		long paymentRemain = -1;
		String sqlGetUser = "select payment_remain from vip_sub_prepaid where vip_sub_info_id=?";
		Connection connection = null;
		try {
			logger.info("Start getMoneyPrepaidOfVipsub for vipsubIbfoId " + vipsubIbfoId);
			connection = getConnection("dbvipsub");
			ps = connection.prepareStatement(sqlGetUser);
			ps.setLong(1, vipsubIbfoId);
			rs = ps.executeQuery();
			while (rs.next()) {
				paymentRemain = rs.getLong("payment_remain");
				break;
			}
			logTimeDb("Time to getMoneyPrepaidOfVipsub vipsubIbfoId " + vipsubIbfoId + " totalAmount " + paymentRemain, timeSt);
		} catch (Exception ex) {
			logger.error("Error getMoneyPrepaidOfVipsub vipsubIbfoId " + vipsubIbfoId, ex);
			logger.error(AppManager.logException(timeSt, ex));
			return -2;
		} finally {
			closeResultSet(rs);
			closePrepareStatement(ps);
			closeConnection(connection);
		}
		return paymentRemain;
	}

	public int updateVipsubPaymentPrepaid(long vipsubId, double money, double lockedMoney) {
		long timeSt = System.currentTimeMillis();
		PreparedStatement ps = null;
		String sql = "update vip_sub_prepaid set payment_remain =?, locked_money=locked_money+?,create_time=sysdate,update_time=sysdate,last_renew_scan_time=null where vip_sub_info_id=?";
		Connection connection = null;
		int result = 0;
		try {
			logger.info("Start updatePaymentPrepaid for vipsubId " + vipsubId);
			connection = getConnection("dbvipsub");
			ps = connection.prepareStatement(sql);
			ps.setDouble(1, money);
			ps.setDouble(2, lockedMoney);
			ps.setLong(3, vipsubId);
			result = ps.executeUpdate();
			logTimeDb("End to updatePaymentPrepaid vipsubId " + vipsubId + " result " + result, timeSt);
		} catch (Exception ex) {
			logger.error("Error updatePaymentPrepaid vipsubId " + vipsubId, ex);
			logger.error(AppManager.logException(timeSt, ex));
			return 0;
		} finally {
			closePrepareStatement(ps);
			closeConnection(connection);
		}
		return result;
	}
}
