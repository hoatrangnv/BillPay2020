/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.viettel.vas.wsfw.common.Vas;
import com.viettel.vas.wsfw.common.WebserviceAbstract;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.vas.wsfw.database.DbSyncEmolaFloat;
import com.viettel.vas.wsfw.object.EmolaFloat;
import com.viettel.vas.wsfw.object.ListEmolaFloat;
import com.viettel.vas.wsfw.object.ResponseSyncEmolaFloat;
import com.viettel.vas.wsfw.object.UserInfo;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;

/**
 *
 * @author bacnx@viettel.com.vn
 * @since Mar 01, 2019
 * @version 1.0
 */
@WebService
public class SyncEmolaFloatBCCS extends WebserviceAbstract {
	
	DbSyncEmolaFloat db;
	DbProcessor dbps;
	
	public SyncEmolaFloatBCCS() {
		super("SyncEmolaFloatBCCS");
		try {
			db = new DbSyncEmolaFloat("dbsm", logger);
			dbps = new DbProcessor("dbsm", logger);
		} catch (Exception ex) {
			logger.error("Fail init webservice SyncEmolaFloatBCCS");
			logger.error(ex);
		}
	}
	
	@WebMethod(operationName = "SyncEmolaFloatBCCS")
	public ResponseSyncEmolaFloat syncEmolaFloatBCCS(
			@WebParam(name = "user") String wsuser,
			@WebParam(name = "pass") String wspassword,
			@WebParam(name = "fromDate") String fromDate,
			@WebParam(name = "toDate") String toDate
	) throws Exception {
		logger.info("Start process SyncEmolaFloatBCCS user " + wsuser);
		ResponseSyncEmolaFloat response = new ResponseSyncEmolaFloat();
		String ip = "0.0.0.0";
		try {
			if (wsuser == null || wspassword == null || wsuser.length() == 0 || wspassword.length() == 0) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
				return response;
			}
			ip = getIpClient();
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
			String startDate = getDateReport(fromDate, true);
			String endDate = getDateReport(toDate, false);
			if ("PARSEEXCEPTION".equals(startDate) || "PARSEEXCEPTION".equals(toDate)) {
				logger.warn("Invalid input startDate " + startDate + " toDate " + toDate);
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_DATE_FORMAT_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_DATE_FORMAT_MSS);
				return response;
			}
			if ("EXCEPTION".equals(startDate) || "EXCEPTION".equals(endDate)) {
				logger.warn("Error parse input startDate " + startDate + " toDate " + toDate);
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
				return response;
			}
			
			List<EmolaFloat> listResult = db.getListEmolaFloat(fromDate, toDate);
			if (listResult == null) {
				logger.warn("Error while processing startDate " + startDate + " toDate " + toDate);
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
				return response;
			}
			if (listResult.isEmpty()) {
				logger.warn("No data found startDate " + startDate + " toDate " + toDate);
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_NO_DATA_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_NO_DATA_MSS);
				return response;
			}
			ListEmolaFloat listResponse = new ListEmolaFloat();
			listResponse.setEmolaFloat(listResult);
			response.setErrorCode(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_CODE);
			response.setDescription(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_DECS);
			response.setListEmolaFloat(listResponse);
			return response;
		} catch (Exception e) {
			logger.error("Had exception " + e.toString());
			response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
			response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
			return response;
		}
	}
	
	public String getDateReport(String dateStr, boolean isStartDate) {
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		String dateReturn = "";
		try {
			if (dateStr != null && retNull(dateStr).length() > 0) {
				Date d = sdf.parse(retNull(dateStr));
				dateReturn = sdf.format(d);
			} else {
				Calendar cal = Calendar.getInstance();
				if (isStartDate) {
					cal.set(Calendar.DAY_OF_MONTH, 1);
					dateReturn = sdf.format(cal.getTime());
				} else {
					dateReturn = sdf.format(cal.getTime());
				}
			}
			return dateReturn;
		} catch (ParseException e1) {
			return "PARSEEXCEPTION";
		} catch (Exception e) {
			return "EXCEPTION";
		}
	}
	
	public boolean isNotNull(String str) {
		if (str == null || str.trim().length() == 0) {
			return false;
		} else {
			return true;
		}
	}
	
	public String retNull(String str) {
		if (str == null || str.trim().length() == 0) {
			return "";
		} else {
			return str.trim();
		}
	}
}
