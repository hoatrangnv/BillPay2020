/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.viettel.data.ws.utils.PagamentoServico;
import com.viettel.vas.wsfw.common.WebserviceAbstract;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import com.viettel.vas.wsfw.database.DbPre;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.data.ws.utils.Exchange;
import com.viettel.smsfw.manager.AppManager;
import java.util.ArrayList;
import com.viettel.vas.wsfw.object.ResponseBankTrans;
import com.viettel.vas.wsfw.object.UserInfo;
import java.util.HashMap;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 4, 2013
 * @version 1.0
 */
@WebService
public class BankTransferWs extends WebserviceAbstract {

    Exchange exch;
    DbPre dbPre;
    DbProcessor db;
    String ucmPrice;
    String ucmFee;
    String ucmPackage;
    ArrayList<String> listUcmPackage;
    HashMap<String, String> mapPrice;
    HashMap<String, String> mapFee;
    ArrayList<String> listErrNotEnoughBalance;

    public BankTransferWs() {
        super("BankTransferWs");
        try {
            db = new DbProcessor("dbsm", logger);
//            ArrayList<Long> lstCounter = db.getAllCounter();
//            String ref = "";
//            long[] lstCounter = {1328781l,1328783l,1330982l,1328787l,1332103l,1329512l,1328786l};
//            for (long counter : lstCounter) {
//                ref = PagamentoServico.genReferenceCounterId(counter + "");
//                db.updateCounter(counter, ref);
//            }
//            ArrayList<Long> lstContract = db.getAllContract();
//            String ref = "";            
//            for (long contract : lstContract) {
//                ref = PagamentoServico.genReferenceId(contract + "");
//                db.updateContract(contract, ref);
//            }
        } catch (Exception ex) {
            logger.error("Fail init webservice BankTransferWs");
            logger.error(ex);
        }
    }

    @WebMethod(operationName = "checkTrans")
    public ResponseBankTrans checkTrans(
            @WebParam(name = "transCode") String transCode,
            @WebParam(name = "bankName") String bankName,
            @WebParam(name = "amount") String amount,
            @WebParam(name = "userName") String userName,
            @WebParam(name = "passWord") String passWord) throws Exception {
        ResponseBankTrans response = new ResponseBankTrans();
        logger.info("Start process checkTrans for sub " + transCode
                + " bankName " + bankName + " userName " + userName + " amount " + amount);
//        step 1: validate input
        if (transCode == null || "".equals(transCode.trim())
                || bankName == null || "".equals(bankName.trim())
                || amount == null || "".equals(amount.trim())
                || userName == null || "".equals(userName.trim())
                || passWord == null || "".equals(passWord.trim())) {
            logger.warn("Invalid input transCode " + transCode);
            response.setErrorCode("01");
            response.setDescription("INVALID_INPUT");
            return response;
        }
//        step 2: validate ip
        String ip = getIpClient();
        if (ip == null || "".equals(ip.trim())) {
            logger.warn("Can not get ip for transCode " + transCode);
            response.setErrorCode("01");
            response.setDescription("FAIL_GET_IP");
            return response;
        }
        String decUserName = com.viettel.security.PassTranformer.decrypt(userName);
        String decPassword = com.viettel.security.PassTranformer.decrypt(passWord);
        UserInfo user = authenticate(db, decUserName, decPassword, ip);
        if (user == null || user.getId() < 0) {
            logger.warn("Invalid account " + userName);
            response.setErrorCode("01");
            response.setDescription("WRONG_ACCOUNT_IP");
            return response;
        }
        db.updateTransCodePending();
        long bankTranferInfoId = db.checkTransCodeAvaiable(bankName, transCode, amount, decUserName);
        if (bankTranferInfoId > 0) {
            logger.info("TransCode is avaiable " + transCode + " decUserName " + decUserName);
            int count = db.updateTransCodeUsing(bankName, transCode, amount, decUserName, bankTranferInfoId);
            if (count == 1) {
                response.setErrorCode("00");
                response.setDescription("Trans is avaiable");
                return response;
            } else {
                response.setErrorCode("99");
                response.setDescription("System error, can not update trans");
                return response;
            }
        } else {
            logger.info("TransCode is invalid " + transCode + " decUserName " + decUserName);
            response.setErrorCode("05");
            response.setDescription("TransCode and Amount and Bank not map");
            return response;
        }
    }

    @WebMethod(operationName = "updateTrans")
    public ResponseBankTrans updateTrans(
            @WebParam(name = "transCode") String transCode,
            @WebParam(name = "bankName") String bankName,
            @WebParam(name = "amount") String amount,
            @WebParam(name = "userName") String userName,
            @WebParam(name = "passWord") String passWord,
            @WebParam(name = "staffCreate") String staffCreate,
            @WebParam(name = "staffApprove") String staffApprove,
            @WebParam(name = "purpose") String purpose) throws Exception {
        ResponseBankTrans response = new ResponseBankTrans();
        logger.info("Start process updateTrans for sub " + transCode
                + " bankName " + bankName + " userName " + userName + " amount " + amount);
        long timeSt = System.currentTimeMillis();
        long bankTranferInfoId = 0;
        try {
            //        step 1: validate input
            if (transCode == null || "".equals(transCode.trim())
                    || bankName == null || "".equals(bankName.trim())
                    || amount == null || "".equals(amount.trim())
                    || userName == null || "".equals(userName.trim())
                    || passWord == null || "".equals(passWord.trim())) {
                logger.warn("Invalid input transCode " + transCode);
                response.setErrorCode("01");
                response.setDescription("INVALID_INPUT");
                return response;
            }
            if (staffCreate == null) {
                staffCreate = "";
            }
            if (staffApprove == null) {
                staffApprove = "";
            }
            if (purpose == null) {
                purpose = "";
            }
//        step 2: validate ip
            String ip = getIpClient();
            if (ip == null || "".equals(ip.trim())) {
                logger.warn("Can not get ip for transCode " + transCode);
                response.setErrorCode("01");
                response.setDescription("FAIL_GET_IP");
                return response;
            }
            String decUserName = com.viettel.security.PassTranformer.decrypt(userName);
            String decPassword = com.viettel.security.PassTranformer.decrypt(passWord);
            UserInfo user = authenticate(db, decUserName, decPassword, ip);
            if (user == null || user.getId() < 0) {
                logger.warn("Invalid account " + userName);
                response.setErrorCode("01");
                response.setDescription("WRONG_ACCOUNT_IP");
                return response;
            }
            bankTranferInfoId = db.checkTransCodeBeforeUpdate(bankName, transCode, amount, decUserName);
            if (bankTranferInfoId > 0) {
                logger.info("TransCode is valid " + transCode + " decUserName " + decUserName);
                int count = db.updateTransCodeUsed(bankName, transCode, amount, decUserName,
                        staffCreate, staffApprove, purpose, bankTranferInfoId);
                if (count == 1) {
                    response.setErrorCode("00");
                    response.setDescription("Update trans success");
                    return response;
                } else {
                    response.setErrorCode("99");
                    response.setDescription("System error, can not update trans");
                    return response;
                }
            } else {
                logger.info("TransCode is invalid " + transCode + " decUserName " + decUserName);
                response.setErrorCode("05");
                response.setDescription("TransCode and Amount and Bank not map");
                return response;
            }
        } catch (Exception e) {
            logger.error("Have exception when updateTrans now try to save log this transcode " + transCode);
            logger.error(AppManager.logException(timeSt, e));
            try {
                db.updateTransCodeUsed(bankName, transCode, amount, "",
                        "", "", "", bankTranferInfoId);
            } catch (Exception ex) {
                logger.error("Exception again when try to save log this transcode " + transCode + " " + AppManager.logException(timeSt, ex));
            }
            response.setErrorCode("99");
            response.setDescription("System error, can not update trans");
            return response;
        }
    }

    @WebMethod(operationName = "rollbackTrans")
    public ResponseBankTrans rollbackTrans(
            @WebParam(name = "transCode") String transCode,
            @WebParam(name = "bankName") String bankName,
            @WebParam(name = "amount") String amount,
            @WebParam(name = "reason") String reason,
            @WebParam(name = "userName") String userName,
            @WebParam(name = "passWord") String passWord) throws Exception {
        ResponseBankTrans response = new ResponseBankTrans();
        logger.info("Start process rollbackTrans for sub " + transCode
                + " bankName " + bankName + " userName " + userName + " amount " + amount);
        //        step 1: validate input
        if (transCode == null || "".equals(transCode.trim())
                || bankName == null || "".equals(bankName.trim())
                || amount == null || "".equals(amount.trim())
                || reason == null || "".equals(reason.trim())
                || userName == null || "".equals(userName.trim())
                || passWord == null || "".equals(passWord.trim())) {
            logger.warn("Invalid input transCode " + transCode);
            response.setErrorCode("01");
            response.setDescription("INVALID_INPUT");
            return response;
        }
//        step 2: validate ip
        String ip = getIpClient();
        if (ip == null || "".equals(ip.trim())) {
            logger.warn("Can not get ip for transCode " + transCode);
            response.setErrorCode("01");
            response.setDescription("FAIL_GET_IP");
            return response;
        }
        String decUserName = com.viettel.security.PassTranformer.decrypt(userName);
        String decPassword = com.viettel.security.PassTranformer.decrypt(passWord);
        UserInfo user = authenticate(db, decUserName, decPassword, ip);
        if (user == null || user.getId() < 0) {
            logger.warn("Invalid account " + userName);
            response.setErrorCode("01");
            response.setDescription("WRONG_ACCOUNT_IP");
            return response;
        }
        long bankTranferInfoId = db.checkTransCodeBeforeRollback(bankName, transCode, amount, decUserName);
        if (bankTranferInfoId > 0) {
            logger.info("TransCode is valid to rollback " + transCode + " decUserName " + decUserName);
            int count = db.updateTransCodeRollback(bankName, transCode, amount, bankTranferInfoId, reason, decUserName);
            if (count == 1) {
                response.setErrorCode("00");
                response.setDescription("Trans is avaiable");
                return response;
            } else {
                response.setErrorCode("99");
                response.setDescription("System error, can not update trans");
                return response;
            }
        } else {
            logger.info("TransCode is invalid, can not rollback " + transCode + " decUserName " + decUserName);
            response.setErrorCode("05");
            response.setDescription("TransCode and Amount and Bank not map");
            return response;
        }
    }
}
