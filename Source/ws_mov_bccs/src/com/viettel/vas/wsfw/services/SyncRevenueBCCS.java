/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.viettel.vas.wsfw.common.Vas;
import com.viettel.vas.wsfw.common.WebserviceAbstract;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.vas.wsfw.database.DbSyncRevenueBccs;
import com.viettel.vas.wsfw.object.ListRevenue;
import com.viettel.vas.wsfw.object.ListRvnService;
import com.viettel.vas.wsfw.object.ListShop;
import com.viettel.vas.wsfw.object.ListStockModel;
import com.viettel.vas.wsfw.object.ListStockType;
import com.viettel.vas.wsfw.object.ResponseRevenue;
import com.viettel.vas.wsfw.object.ResponseRvnService;
import com.viettel.vas.wsfw.object.ResponseShop;
import com.viettel.vas.wsfw.object.ResponseStockModel;
import com.viettel.vas.wsfw.object.ResponseStockType;
import com.viettel.vas.wsfw.object.Revenue;
import com.viettel.vas.wsfw.object.RvnService;
import com.viettel.vas.wsfw.object.Shop;
import com.viettel.vas.wsfw.object.StockModel;
import com.viettel.vas.wsfw.object.StockType;
import com.viettel.vas.wsfw.object.UserInfo;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @author bacnx@viettel.com.vn
 * @since Mar 01, 2019
 * @version 1.0
 */
@WebService
public class SyncRevenueBCCS extends WebserviceAbstract {

	DbSyncRevenueBccs db;
	DbProcessor dbps;
//    private final String USER_LOGIN = "bccs_erp";
//    private final String PASSWORD = "bccs_erp@2019";

	public SyncRevenueBCCS() {
		super("SyncRevenueBCCS");
		try {
			db = new DbSyncRevenueBccs("dbsm", logger);
			dbps = new DbProcessor("dbsm", logger);
		} catch (Exception ex) {
			logger.error("Fail init webservice SyncRevenueBCCS");
			logger.error(ex);
		}
	}

	@WebMethod(operationName = "synRevenueBccs")
	public ResponseRevenue synRevenueBccs(
			@WebParam(name = "user") String wsuser,
			@WebParam(name = "pass") String wspassword,
			@WebParam(name = "syn_type") Integer revenueType,
			@WebParam(name = "start_cycle") String startCycle) throws Exception {
		logger.info("Start process synRevenueBccs user " + wsuser);
		ResponseRevenue response = new ResponseRevenue();
		try {
			if (wsuser == null || wspassword == null || wsuser.length() == 0 || wspassword.length() == 0) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
				return response;
			}
			if (revenueType == null) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_REVENUE_TYPE_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_REVENUE_TYPE_DESC);
			} else if (startCycle == null || startCycle.length() == 0) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_STAR_OF_CYCLE_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_STAR_OF_CYCLE_DESC);
			} else {
				String ip = getIpClient();
				if (ip == null || "".equals(ip.trim())) {
					logger.warn("Can not get ip for client...");
					response.setErrorCode(Vas.SyncBccsErpResultCode.FAIL_GET_IP_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.FAIL_GET_IP_DESC);
					return response;
				}
				UserInfo userws = authenticate(dbps, wsuser, wspassword, ip);
				if (userws == null || userws.getId() < 0) {
					logger.warn("Invalid account " + wsuser);
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
					return response;
				}
				SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
				Date starOfCycle = dateFormat.parse(startCycle);
				Calendar c = Calendar.getInstance();
				c.setTime(starOfCycle);
				c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH));

				List<Revenue> listLvn = new ArrayList<Revenue>();
				if (revenueType == Vas.Constanst.API_SALE_REVENUE_TYPE) {
					listLvn = db.getBccsSaleRevenueCloseCycle(dateFormat.format(starOfCycle), dateFormat.format(c.getTime()), Vas.Constanst.API_SALE_REVENUE_TYPE);
				} else if (revenueType == Vas.Constanst.API_PAYMENT_REVENUE_TYPE) {
					listLvn = db.getBccsPaymentRevenueCloseCycle(dateFormat.format(starOfCycle), dateFormat.format(c.getTime()), Vas.Constanst.API_PAYMENT_REVENUE_TYPE);
				} else {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_REVENUE_TYPE_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_REVENUE_TYPE_DESC);
				}
				if (listLvn == null) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
				} else if (listLvn.isEmpty()) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_PROCESSING_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_PROCESSING_DECS);
					response.setListRevenue(new ListRevenue());
				} else {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_DECS);
					ListRevenue listRev = new ListRevenue();
					listRev.setRevenues(listLvn);
					response.setListRevenue(listRev);
				}
			}
			return response;
		} catch (ParseException pe) {
			logger.warn("Had ParseException " + pe.toString());
			response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_STAR_OF_CYCLE_CODE);
			response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_STAR_OF_CYCLE_DESC);
			return response;
		} catch (Exception e) {
			logger.warn("Had exception " + e.toString());
			response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
			response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
			return response;
		} finally {
			logger.info("Finish synRevenueBccs,revenue type:" + revenueType + ",start of cycle:" + startCycle);
		}

	}

	@WebMethod(operationName = "synRevenueBccsAdjusted")
	public ResponseRevenue synRevenueBccsComplement(
			@WebParam(name = "user") String user,
			@WebParam(name = "pass") String pass,
			@WebParam(name = "syn_type") Integer revenueType,
			@WebParam(name = "start_cycle") String startCycle) throws Exception {
		logger.info("Start process synRevenueBccsAdjusted user " + user);
		ResponseRevenue response = new ResponseRevenue();
		try {
			if (user == null || pass == null || user.length() == 0 || pass.length() == 0) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
			} else if (revenueType == null) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_REVENUE_TYPE_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_REVENUE_TYPE_DESC);
			} else if (startCycle == null || startCycle.length() == 0) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_STAR_OF_CYCLE_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_STAR_OF_CYCLE_DESC);
			} else {
				String ip = getIpClient();
				if (ip == null || "".equals(ip.trim())) {
					logger.warn("Can not get ip for client...");
					response.setErrorCode(Vas.SyncBccsErpResultCode.FAIL_GET_IP_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.FAIL_GET_IP_DESC);
					return response;
				}
				UserInfo userws = authenticate(dbps, user, pass, ip);
				if (userws == null || userws.getId() < 0) {
					logger.warn("Invalid account " + user);
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
					return response;
				}
				SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
				Date starOfCycle = dateFormat.parse(startCycle);
				Calendar c = Calendar.getInstance();
				c.setTime(starOfCycle);
				c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH));

				List<Revenue> listLvn = new ArrayList<Revenue>();
				if (revenueType == Vas.Constanst.API_SALE_REVENUE_TYPE) {
					listLvn = db.getBccsRevenueAdjusted(startCycle, Vas.Constanst.API_SALE_REVENUE_TYPE);
					//listLvn = db.getBccsSaleRevenueAdjusted(dateFormat.format(starOfCycle), dateFormat.format(c.getTime()), Vas.Constanst.API_SALE_REVENUE_TYPE);
				} else if (revenueType == Vas.Constanst.API_PAYMENT_REVENUE_TYPE) {
					listLvn = db.getBccsRevenueAdjusted(startCycle, Vas.Constanst.API_PAYMENT_REVENUE_TYPE);
					//listLvn = db.getBccsPaymentRevenueAdjusted(dateFormat.format(starOfCycle), dateFormat.format(c.getTime()), Vas.Constanst.API_PAYMENT_REVENUE_TYPE);
				} else {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_REVENUE_TYPE_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_REVENUE_TYPE_DESC);
				}
				if (listLvn == null) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
				} else if (listLvn.isEmpty()) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_PROCESSING_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_PROCESSING_DECS);
					response.setListRevenue(new ListRevenue());
				} else {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_DECS);
					ListRevenue listRev = new ListRevenue();
					listRev.setRevenues(listLvn);
					response.setListRevenue(listRev);
				}
			}
			return response;
		} catch (ParseException pe) {
			logger.warn("Had ParseException " + pe.toString());
			response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_STAR_OF_CYCLE_CODE);
			response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_STAR_OF_CYCLE_DESC);
			return response;
		} catch (Exception e) {
			logger.warn("Had exception " + e.toString());
			response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
			response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
			return response;
		} finally {
			logger.info("Finish synRevenueBccs,revenue type:" + revenueType + ",start of cycle:" + startCycle);
		}

	}

	@WebMethod(operationName = "synShop")
	public ResponseShop ResponseShop(
			@WebParam(name = "user") String user,
			@WebParam(name = "pass") String pass) throws Exception {
		logger.info("Start process synShop user " + user);
		ResponseShop response = new ResponseShop();
		try {
			if (user == null || pass == null || user.length() == 0 || pass.length() == 0) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
			} else {
				String ip = getIpClient();
				if (ip == null || "".equals(ip.trim())) {
					logger.warn("Can not get ip for client...");
					response.setErrorCode(Vas.SyncBccsErpResultCode.FAIL_GET_IP_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.FAIL_GET_IP_DESC);
					return response;
				}
				UserInfo userws = authenticate(dbps, user, pass, ip);
				if (userws == null || userws.getId() < 0) {
					logger.warn("Invalid account " + user);
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
					return response;
				}
				List<Shop> list = db.getBccsShop();
				if (list == null) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
				} else if (list.isEmpty()) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_PROCESSING_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_PROCESSING_DECS);
				} else {
					ListShop listSh = new ListShop();
					listSh.setShops(list);
					response.setListShop(listSh);
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_DECS);
				}
			}
			return response;
		} catch (Exception e) {
			logger.warn("Had exception " + e.toString());
			response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
			response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
			return response;
		} finally {
			logger.info("Finish synShop");
		}

	}

	@WebMethod(operationName = "synStockModel")
	public ResponseStockModel synStockModel(
			@WebParam(name = "user") String user,
			@WebParam(name = "pass") String pass) throws Exception {
		logger.info("Start process synStockModel user " + user);
		ResponseStockModel response = new ResponseStockModel();
		try {
			if (user == null || pass == null || user.length() == 0 || pass.length() == 0) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
			} else {
				String ip = getIpClient();
				if (ip == null || "".equals(ip.trim())) {
					logger.warn("Can not get ip for client...");
					response.setErrorCode(Vas.SyncBccsErpResultCode.FAIL_GET_IP_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.FAIL_GET_IP_DESC);
					return response;
				}
				UserInfo userws = authenticate(dbps, user, pass, ip);
				if (userws == null || userws.getId() < 0) {
					logger.warn("Invalid account " + user);
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
					return response;
				}
				List<StockModel> list = db.getBccsStockModel();
				if (list == null) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
				} else if (list.isEmpty()) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_PROCESSING_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_PROCESSING_DECS);
				} else {
					ListStockModel listSM = new ListStockModel();
					listSM.setStockModels(list);
					response.setListStockModel(listSM);
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_DECS);
				}
			}
			return response;
		} catch (Exception e) {
			logger.warn("Had exception " + e.toString());
			response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
			response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
			return response;
		} finally {
			logger.info("Finish synStockModel");
		}

	}

	@WebMethod(operationName = "synStockType")
	public ResponseStockType synStockType(
			@WebParam(name = "user") String user,
			@WebParam(name = "pass") String pass) throws Exception {
		logger.info("Start process synStockModel user " + user);
		ResponseStockType response = new ResponseStockType();
		try {
			if (user == null || pass == null || user.length() == 0 || pass.length() == 0) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
			} else {
				String ip = getIpClient();
				if (ip == null || "".equals(ip.trim())) {
					logger.warn("Can not get ip for client...");
					response.setErrorCode(Vas.SyncBccsErpResultCode.FAIL_GET_IP_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.FAIL_GET_IP_DESC);
					return response;
				}
				UserInfo userws = authenticate(dbps, user, pass, ip);
				if (userws == null || userws.getId() < 0) {
					logger.warn("Invalid account " + user);
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
					return response;
				}
				List<StockType> list = db.getBccsStockType();
				if (list == null) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
				} else if (list.isEmpty()) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_PROCESSING_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_PROCESSING_DECS);
				} else {
					ListStockType lisST = new ListStockType();
					lisST.setStockTypes(list);
					response.setListStockType(lisST);
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_DECS);
				}
			}
			return response;
		} catch (Exception e) {
			logger.warn("Had exception " + e.toString());
			response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
			response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
			return response;
		} finally {
			logger.info("Finish synStockType");
		}

	}

	@WebMethod(operationName = "synRvnService")
	public ResponseRvnService synRvnService(
			@WebParam(name = "user") String user,
			@WebParam(name = "pass") String pass) throws Exception {
		logger.info("Start process synStockModel user " + user);
		ResponseRvnService response = new ResponseRvnService();
		try {
			if (user == null || pass == null || user.length() == 0 || pass.length() == 0) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
			} else {
				String ip = getIpClient();
				if (ip == null || "".equals(ip.trim())) {
					logger.warn("Can not get ip for client...");
					response.setErrorCode(Vas.SyncBccsErpResultCode.FAIL_GET_IP_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.FAIL_GET_IP_DESC);
					return response;
				}
				UserInfo userws = authenticate(dbps, user, pass, ip);
				if (userws == null || userws.getId() < 0) {
					logger.warn("Invalid account " + user);
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
					return response;
				}
				List<RvnService> list = db.getBccsRvnService();
				if (list == null) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
				} else if (list.isEmpty()) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_PROCESSING_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_PROCESSING_DECS);
				} else {
					ListRvnService lisST = new ListRvnService();
					lisST.setRvns(list);
					response.setListRvn(lisST);
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_DECS);
				}
			}
			return response;
		} catch (Exception e) {
			logger.warn("Had exception " + e.toString());
			response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
			response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
			return response;
		} finally {
			logger.info("Finish synRvnService");
		}

	}
}
