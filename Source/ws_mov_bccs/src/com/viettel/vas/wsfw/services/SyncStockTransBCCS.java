/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.viettel.vas.wsfw.common.Vas;
import com.viettel.vas.wsfw.common.WebserviceAbstract;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.vas.wsfw.database.DbSyncStockTransBccs;
import com.viettel.vas.wsfw.object.ResponseSyncNewPartner;
import com.viettel.vas.wsfw.object.ResponseSyncStockTransBCCS;
import com.viettel.vas.wsfw.object.StockModel;
import com.viettel.vas.wsfw.object.UserInfo;
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
public class SyncStockTransBCCS extends WebserviceAbstract {

	DbSyncStockTransBccs db;
	DbProcessor dbps;

	public SyncStockTransBCCS() {
		super("SyncStockTransBCCS");
		try {
			db = new DbSyncStockTransBccs("dbsm", logger);
			dbps = new DbProcessor("dbsm", logger);
		} catch (Exception ex) {
			logger.error("Fail init webservice SyncStockTransBCCS");
			logger.error(ex);
		}
	}

	@WebMethod(operationName = "synStockTransBccs")
	public ResponseSyncStockTransBCCS synStockTransBccs(
			@WebParam(name = "user") String wsuser,
			@WebParam(name = "pass") String wspassword,
			@WebParam(name = "requestCode") String requestCode,
			@WebParam(name = "partnerCode") String partnerCode,
			@WebParam(name = "stockModelCode") String stockModelCode,
			@WebParam(name = "quantity") String quantity,
			@WebParam(name = "stateId") String stateId,
			@WebParam(name = "contractCode") String contractCode,
			@WebParam(name = "reasonId") String reasonId,
			@WebParam(name = "batchCode") String batchCode,
			@WebParam(name = "note") String note
	) throws Exception {
		logger.info("Start process synStockTransBccs user " + wsuser);
		ResponseSyncStockTransBCCS response = null;
		String ip = "0.0.0.0";
		try {

			if (wsuser == null || wspassword == null || wsuser.length() == 0 || wspassword.length() == 0) {
				response = new ResponseSyncStockTransBCCS();
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
				return response;
			}

			ip = getIpClient();
			if (ip == null || "".equals(ip.trim())) {
				response = new ResponseSyncStockTransBCCS();
				logger.warn("Can not get ip for client...");
				response.setErrorCode(Vas.SyncBccsErpResultCode.FAIL_GET_IP_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.FAIL_GET_IP_DESC);
				return response;
			}
			UserInfo userws = authenticate(dbps, wsuser, wspassword, ip);
			if (userws == null || userws.getId() < 0) {
				response = new ResponseSyncStockTransBCCS();
				logger.warn("Invalid account " + wsuser);
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
				return response;
			}
			//Begin sync stock tran
			//Check valid request information
			response = validInputParamSyncStockTrans(requestCode, partnerCode, stockModelCode, quantity, stateId, contractCode, reasonId, batchCode, note);
			if (response != null) {
				logger.warn("Invalid input  " + response.getErrorCode() + " message " + response.getDescription());
				db.insertActionLog(retNull(requestCode), response.getErrorCode(), 0L, "CREATE_STOCK_TRANS", response.getDescription(), "SYNC_ERP", ip, "ADD", response.getErrorCode());
				return response;
			}
			response = new ResponseSyncStockTransBCCS();
			if (db.checkExistedRequestCode(requestCode)) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_REQUEST_ID_EXISTED);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_REQUEST_ID_EXISTED_MSS);
				db.insertActionLog(retNull(requestCode), response.getErrorCode(), 0L, "CREATE_STOCK_TRANS", response.getDescription(), "SYNC_ERP", ip, "ADD", response.getErrorCode());
				return response;
			}
			StockModel smd = db.getStockModelInfo(retNull(stockModelCode));
			if (smd == null) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_STOCK_MODEL_NOT_EXIST);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_STOCK_MODEL_NOT_EXIST_MSS);
				db.insertActionLog(retNull(requestCode), response.getErrorCode(), 0L, "CREATE_STOCK_TRANS", response.getDescription(), "SYNC_ERP", ip, "ADD", response.getErrorCode());
				return response;
			}

			long stockTransId = db.getStockTranSequence();
			if (stockTransId <= 0) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
				db.insertActionLog(retNull(requestCode), response.getErrorCode(), 0L, "CREATE_STOCK_TRANS", response.getDescription(), "SYNC_ERP", ip, "ADD", response.getErrorCode());
				return response;
			}
			long partnerId = db.checkValidPartner(retNull(partnerCode));
			if (partnerId <= 0) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_NOT_EXIST);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_NOT_EXIST_MSS);
				db.insertActionLog(retNull(requestCode), response.getErrorCode(), 0L, "CREATE_STOCK_TRANS", response.getDescription(), "SYNC_ERP", ip, "ADD", response.getErrorCode());
				return response;
			}
			//insert stock trans
			boolean resInsertST = db.insertStockTrans(stockTransId, partnerId, Long.valueOf(reasonId), retNull(contractCode), retNull(batchCode));
			if (resInsertST) {
				boolean resInsertSTD = db.insertStockTransDetail(stockTransId, Long.valueOf(smd.getStockModelId()), Integer.valueOf(retNull(stateId)), Integer.valueOf(retNull(quantity)), 0);
				if (resInsertSTD) {
					String stockTransActionCode = db.getStockTranAction();
					if (isNotNull(stockTransActionCode)) {
						String prefixStockTransCode = "PN";
						boolean resInsertSTA = db.insertStockTransAction(stockTransId, prefixStockTransCode + stockTransActionCode, 2, retNull(note));
						if (resInsertSTA) {
							logger.info("Create stock trans, stock trans detail, stock trans action successful :" + stockTransId + " ip " + ip);
							db.insertActionLog(retNull(requestCode), "00", stockTransId, "CREATE_STOCK_TRANS", "Create stock trans successful ", "SYNC_ERP", ip, "ADD", "00");
							response.setActionCode(prefixStockTransCode + stockTransActionCode);
							response.setDescription(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_DECS);
							response.setErrorCode(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_CODE);
						} else {
							boolean rsCancelST = db.cancleStockTrans(stockTransId);
							logger.error("Error when create stock trans action :" + stockTransId + " result " + resInsertST + " ip " + ip + " cancle stock trans res :" + rsCancelST);
							db.insertActionLog(retNull(requestCode), "01", stockTransId, "CREATE_STOCK_TRANS", "Error when create stock trans action, cancel stock trans res " + rsCancelST, "SYNC_ERP", ip, "ADD", "01");
							response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
							response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
							return response;
						}
					} else {
						boolean rsCancelST = db.cancleStockTrans(stockTransId);
						logger.error("Error when get action code for stock trans action :" + stockTransId + " result " + resInsertST + " ip " + ip + " cancle stock trans res :" + rsCancelST);
						db.insertActionLog(retNull(requestCode), "02", stockTransId, "CREATE_STOCK_TRANS", "Error when get action code for stock trans action, cancel stock trans res " + rsCancelST, "SYNC_ERP", ip, "ADD", "02");
						response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
						response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
						return response;
					}
				} else {
					boolean rsCancelST = db.cancleStockTrans(stockTransId);
					logger.error("Error when crate stock trans detail :" + stockTransId + " result " + resInsertST + " ip " + ip + " cancle stock trans res :" + rsCancelST);
					db.insertActionLog(retNull(requestCode), "03", stockTransId, "CREATE_STOCK_TRANS", "Error when create stock trans detail, cancel stock trans res " + rsCancelST, "SYNC_ERP", ip, "ADD", "03");
					response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
					response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
					return response;
				}
			} else {
				logger.error("Error when crate stock trans:" + stockTransId + " result " + resInsertST + " ip " + ip);
				db.insertActionLog(retNull(requestCode), "04", stockTransId, "CREATE_STOCK_TRANS", "Error when create stock trans", "SYNC_ERP", ip, "ADD", "04");
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
				return response;
			}

			return response;
		} catch (Exception e) {
			logger.error("Had exception " + e.toString());
			response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
			response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
			db.insertActionLog(retNull(requestCode), response.getErrorCode(), 0L, "CREATE_STOCK_TRANS", response.getDescription(), "SYNC_ERP", ip, "ADD", response.getErrorCode());
			return response;
		}
	}

	@WebMethod(operationName = "syncNewPartnerBccs")
	public ResponseSyncNewPartner syncNewPartnerBCCS(
			@WebParam(name = "user") String wsuser,
			@WebParam(name = "pass") String wspassword,
			@WebParam(name = "requestCode") String requestCode,
			@WebParam(name = "code") String code,
			@WebParam(name = "name") String name,
			@WebParam(name = "type") String type,
			@WebParam(name = "address") String address,
			@WebParam(name = "phone") String phone,
			@WebParam(name = "fax") String fax,
			@WebParam(name = "contactName") String contactName
	) throws Exception {
		logger.info("Start process syncNewPartnerBccs user " + wsuser);
		ResponseSyncNewPartner response = null;
		String ip = "0.0.0.0";
		try {
			if (wsuser == null || wspassword == null || wsuser.length() == 0 || wspassword.length() == 0) {
				response = new ResponseSyncNewPartner();
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
				return response;
			}

			ip = getIpClient();
			if (ip == null || "".equals(ip.trim())) {
				response = new ResponseSyncNewPartner();
				logger.warn("Can not get ip for client...");
				response.setErrorCode(Vas.SyncBccsErpResultCode.FAIL_GET_IP_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.FAIL_GET_IP_DESC);
				return response;
			}
			UserInfo userws = authenticate(dbps, wsuser, wspassword, ip);
			if (userws == null || userws.getId() < 0) {
				response = new ResponseSyncNewPartner();
				logger.warn("Invalid account " + wsuser);
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_INVALID_USER_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_INVALID_USER_DESC);
				return response;
			}
			response = validInputParamSyncNewpartner(requestCode, code, name, type, address, phone, fax, contactName);
			if (response != null) {
				logger.warn("Invalid input  " + response.getErrorCode() + " message " + response.getDescription());
				db.insertActionLog(retNull(requestCode), response.getErrorCode(), 0L, "ADD NEW PARTNER", response.getDescription(), "SYNC_ERP", ip, "ADD", response.getErrorCode());
				return response;
			}
			response = new ResponseSyncNewPartner();
			if (db.checkExistedRequestCode(requestCode)) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_REQUEST_ID_EXISTED);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_REQUEST_ID_EXISTED_MSS);
				db.insertActionLog(retNull(requestCode), response.getErrorCode(), 0L, "ADD NEW PARTNER", response.getDescription(), "SYNC_ERP", ip, "ADD", response.getErrorCode());
				return response;
			}
			//String name, String code, int type, String address,String phone, String fax, String contactName, String parnerCode
			long partnerId = db.checkValidPartner(retNull(code));
			if (partnerId == -1) {
				logger.error("Error when check partner code " + code);
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
				db.insertActionLog(retNull(requestCode), response.getErrorCode(), 0L, "ADD NEW PARTNER", response.getDescription(), "SYNC_ERP", ip, "ADD", response.getErrorCode());
				return response;
			}
			if (partnerId > 0) {
				logger.error("Partner code already existed " + code);
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_EXISTED);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_EXISTED_MSS);
				db.insertActionLog(retNull(requestCode), response.getErrorCode(), 0L, "ADD NEW PARTNER", response.getDescription(), "SYNC_ERP", ip, "ADD", response.getErrorCode());
				return response;
			}
			boolean resInsertPartner = db.insertPartner(retNull(name), retNull(code), Integer.valueOf(retNull(type)), retNull(address), retNull(phone), retNull(fax), retNull(contactName));
			if (resInsertPartner) {
				logger.warn("Insert partner successful " + resInsertPartner + " ccode " + code);
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_SUCCESSFULLY_DECS);
				db.insertActionLog(retNull(requestCode), "00", 0L, "ADD NEW PARTNER", "Add new partner successful", "SYNC_ERP", ip, "ADD", "00");
			} else {
				logger.error("Insert partner un-successful " + resInsertPartner);
				response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
				response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
				db.insertActionLog(retNull(requestCode), "01", 0L, "ADD NEW PARTNER", "Add new partner un-successful", "SYNC_ERP", ip, "ADD", "01");
			}

			return response;
		} catch (Exception e) {
			logger.error("Had exception " + e.toString());
			response.setErrorCode(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_CODE);
			response.setDescription(Vas.SyncBccsErpResultCode.API_EXCEPTION_OCCUR_DESC);
			db.insertActionLog(retNull(requestCode), "00", 0L, "ADD NEW PARTNER", "Add new partner successful", "SYNC_ERP", ip, "ADD", "00");
			return response;
		}
	}

	public ResponseSyncStockTransBCCS validInputParamSyncStockTrans(String requestCode, String partnerCode, String stockModelCode, String quantity, String stateId, String contractCode, String reasonId, String batchCode, String note) {
		ResponseSyncStockTransBCCS response = new ResponseSyncStockTransBCCS();
		if (!isNotNull(requestCode)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_REQUEST_ID_EMPTY);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_REQUEST_ID_EMPTY_MSS);
			return response;
		} else if (!validLengthMandatory(requestCode, 20)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_REQUEST_ID_INVALID);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_REQUEST_ID_INVALID_MSS);
			return response;
		}
		if (!isNotNull(partnerCode)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_CODE_EMPTY);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_CODE_EMPTY_MSS);
			return response;
		}
		if (!isNotNull(stockModelCode)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_STOCK_MODEL_CODE_EMPTY);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_STOCK_MODEL_CODE_EMPTY_MSS);
			return response;
		} else {
			if (!validLengthMandatory(stockModelCode, 100)) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_STOCK_MODEL_INVALID);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_STOCK_MODEL_INVALID_MSS);
				return response;
			}
		}
		if (!isNotNull(quantity)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_QUANTITY_EMPTY);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_QUANTITY_EMPTY_MSS);
			return response;
		} else {
			try {
				long x = Long.parseLong(retNull(quantity));
				if (x <= 0) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_QUANTITY_INVALID);
					response.setDescription(Vas.SyncBccsErpResultCode.ERR_QUANTITY_INVALID_MSS);
					return response;
				}
			} catch (NumberFormatException e) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_QUANTITY_EMPTY);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_QUANTITY_EMPTY_MSS);
				return response;
			}
		}
		if (!isNotNull(stateId)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_STATE_ID_INVALID);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_STATE_ID_INVALID_MSS);
			return response;
		} else {
			try {
				long x = Long.parseLong(retNull(stateId));
				if (x != 1 && x != 3) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_STATE_ID_INVALID);
					response.setDescription(Vas.SyncBccsErpResultCode.ERR_STATE_ID_INVALID_MSS);
					return response;
				}
			} catch (NumberFormatException e) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_STATE_ID_INVALID);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_STATE_ID_INVALID_MSS);
				return response;
			}
		}
		if (!isNotNull(contractCode)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_CONTRACT_CODE_EMPTY);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_CONTRACT_CODE_EMPTY_MSS);
			return response;
		} else {
			if (!validLengthMandatory(contractCode, 100)) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_CONTRACT_CODE_INVALID);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_CONTRACT_CODE_INVALID_MSS);
				return response;
			}
		}
		if (!isNotNull(batchCode)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_BATCH_CODE_EMPTY);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_BATCH_CODE_EMPTY_MSS);
			return response;
		} else {
			if (!validLengthMandatory(batchCode, 100)) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_BATCH_CODE_EMPTY);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_BATCH_CODE_EMPTY_MSS);
				return response;
			}
		}
		if (!isNotNull(reasonId)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_REASON_ID_EMPTY);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_REASON_ID_EMPTY_MSS);
			return response;
		} else {
			try {
				long x = Long.parseLong(retNull(reasonId));
				if (x != 200749) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_REASON_ID_INVALID);
					response.setDescription(Vas.SyncBccsErpResultCode.ERR_REASON_ID_INVALID_MSS);
					return response;
				}
			} catch (NumberFormatException e) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_REASON_ID_INVALID);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_REASON_ID_INVALID_MSS);
				return response;
			}
		}
		if (retNull(note).length() > 200) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_NOTE_INVALID);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_NOTE_INVALID_MSS);
			return response;
		}
		return null;
	}

	public ResponseSyncNewPartner validInputParamSyncNewpartner(String requestCode, String code, String name, String type, String address, String phone, String fax, String contact) {
		ResponseSyncNewPartner response = new ResponseSyncNewPartner();
		if (!isNotNull(requestCode)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_REQUEST_ID_EMPTY);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_REQUEST_ID_EMPTY_MSS);
			return response;
		} else if (!validLengthMandatory(requestCode, 20)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_REQUEST_ID_INVALID);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_REQUEST_ID_INVALID_MSS);
			return response;
		}
		if (!isNotNull(code)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_CODE_EMPTY);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_CODE_EMPTY_MSS);
			return response;
		} else if (!validLengthMandatory(code, 100)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_CODE_INVALID);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_CODE_INVALID_MSS);
			return response;
		}
		if (!isNotNull(name)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_NAME_EMPTY);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_NAME_EMPTY_MSS);
			return response;
		} else if (!validLengthMandatory(name, 100)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_NAME_INVALID);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_NAME_INVALID_MSS);
			return response;
		}
		if (!isNotNull(type)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_TYPE_EMPTY);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_TYPE_EMPTY_MSS);
			return response;
		} else {
			try {
				int x = Integer.valueOf(type);
				if (x != 1 && x != 2 && x != 3) {
					response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_TYPE_INVALID);
					response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_TYPE_INVALID_MSS);
					return response;
				}
			} catch (NumberFormatException e) {
				response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_TYPE_INVALID);
				response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_TYPE_INVALID_MSS);
				return response;
			}
		}
		if (!validLengthOptional(address, 200)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_ADDRESS_INVALID);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_ADDRESS_INVALID_MSS);
			return response;
		}
		if (!validLengthOptional(phone, 20)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_PHONE_INVALID);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_PHONE_INVALID_SMS);
			return response;
		}
		if (!validLengthOptional(fax, 20)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_FAX_INVALID);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_FAX_INVALID_MSS);
			return response;
		}
		if (!validLengthOptional(contact, 50)) {
			response.setErrorCode(Vas.SyncBccsErpResultCode.ERR_PARNER_CONTACT_INVALID);
			response.setDescription(Vas.SyncBccsErpResultCode.ERR_PARNER_CONTACT_INVALID_MSS);
			return response;
		}
		return null;
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

	public boolean validLengthMandatory(String s, int length) {
		if (!isNotNull(s)) {
			return false;
		}
		if (retNull(s).length() > length) {
			return false;
		}
		return true;
	}

	public boolean validLengthOptional(String s, int length) {
		if (isNotNull(s) && retNull(s).length() > length) {
			return false;
		}
		return true;
	}
}
