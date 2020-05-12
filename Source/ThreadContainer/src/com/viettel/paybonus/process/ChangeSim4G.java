/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.google.gson.Gson;
import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbChangeSim4GProcessor;
import com.viettel.paybonus.obj.ActionLogPr;
import com.viettel.paybonus.obj.Customer;
import com.viettel.paybonus.obj.RequestChangeSim;
import com.viettel.paybonus.obj.ResponseWallet;
import com.viettel.paybonus.obj.SubInfo;
import com.viettel.paybonus.service.EWalletUtil;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class ChangeSim4G extends ProcessRecordAbstract {

    Exchange pro;
    DbChangeSim4GProcessor db;
    String strReasonId;
    String lstIsdnReceiveError;
    String[] arrIsdnReceiveError;
    String changeSimSucessfully;
    SimpleDateFormat sdf2 = new SimpleDateFormat("dd/MM/yyyy");
    String vasCodeConnection;
    String[] arrVasCodeConnection;
    ArrayList<HashMap> lstMapVasCodeConnection;
    String vasCodeParam;
    String[] arrVasCodeParam;
    ArrayList<HashMap> lstMapVasCodeParam;
    String vasCodeActionType;
    String[] arrVasCodeActionType;
    ArrayList<HashMap> lstMapVasCodeActionType;
    String vasCodeChannel;
    String[] arrVasCodeChannel;
    ArrayList<HashMap> lstMapVasCodeChannel;
    String vasCodeAmountMoney;
    String[] arrVasCodeAmountMoney;
    String vasCodeAmountRate;
    ArrayList<HashMap> lstMapVasCodeAmount;
    String smsProfileInvalid;
    String smsSimInfoInvalid;
    String smsCusInfoInvalid;
    String smsSubNotAcitive;
    String smsProductCodeInvalid;
    String smsSimNotSale;
    String smsSimSerialEmpty;
    String smsChangeSimFailed;
    String smsSim4GAlreadyUsed;
    String smsNotSuportedChange4gTo3g;
    String changeSimSurvay;
    String smsProfileInvalidPT;
    String smsSimInfoInvalidPT;
    String smsCusInfoInvalidPT;
    String smsSubNotAcitivePT;
    String smsProductCodeInvalidPT;
    String smsSimNotSalePT;
    String smsSimSerialEmptyPT;
    String smsChangeSimFailedPT;
    String smsSim4GAlreadyUsedPT;
    String smsNotSuportedChange4gTo3gPT;
    Long k4sNO;

    public ChangeSim4G() {
        super();
        logger = Logger.getLogger(ChangeSim4G.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbChangeSim4GProcessor();
        strReasonId = ResourceBundle.getBundle("configPayBonus").getString("changeSimReasonId");
        lstIsdnReceiveError = ResourceBundle.getBundle("configPayBonus").getString("lstIsdnReceiveError");
        arrIsdnReceiveError = lstIsdnReceiveError.split("\\|");
        changeSimSucessfully = ResourceBundle.getBundle("configPayBonus").getString("changeSimSucessfully");
        vasCodeAmountMoney = ResourceBundle.getBundle("configPayBonus").getString("vasCodeAmountMoney");
        arrVasCodeAmountMoney = vasCodeAmountMoney.split("\\|");
        vasCodeAmountRate = ResourceBundle.getBundle("configPayBonus").getString("vasCodeAmountRate");
        vasCodeConnection = ResourceBundle.getBundle("configPayBonus").getString("vasCodeConnection");
        //get message change sim 
        //enlish
        smsProfileInvalid = ResourceBundle.getBundle("configPayBonus").getString("smsProfileInvalid");
        smsSimInfoInvalid = ResourceBundle.getBundle("configPayBonus").getString("smsSimInfoInvalid");
        smsCusInfoInvalid = ResourceBundle.getBundle("configPayBonus").getString("smsCusInfoInvalid");
        smsSubNotAcitive = ResourceBundle.getBundle("configPayBonus").getString("smsSubNotAcitive");
        smsProductCodeInvalid = ResourceBundle.getBundle("configPayBonus").getString("smsProductCodeInvalid");
        smsSimNotSale = ResourceBundle.getBundle("configPayBonus").getString("smsSimNotSale");
        smsSimSerialEmpty = ResourceBundle.getBundle("configPayBonus").getString("smsSimSerialEmpty");
        smsChangeSimFailed = ResourceBundle.getBundle("configPayBonus").getString("smsChangeSimFailed");
        smsSim4GAlreadyUsed = ResourceBundle.getBundle("configPayBonus").getString("smsSim4GAlreadyUsed");
        smsNotSuportedChange4gTo3g = ResourceBundle.getBundle("configPayBonus").getString("smsNotSuportedChange4gTo3g");
        changeSimSurvay = ResourceBundle.getBundle("configPayBonus").getString("changeSimSurvay");

        //purtogal
        smsProfileInvalidPT = ResourceBundle.getBundle("configPayBonus").getString("smsProfileInvalidPT");
        smsSimInfoInvalidPT = ResourceBundle.getBundle("configPayBonus").getString("smsSimInfoInvalidPT");
        smsCusInfoInvalidPT = ResourceBundle.getBundle("configPayBonus").getString("smsCusInfoInvalidPT");
        smsSubNotAcitivePT = ResourceBundle.getBundle("configPayBonus").getString("smsSubNotAcitivePT");
        smsProductCodeInvalidPT = ResourceBundle.getBundle("configPayBonus").getString("smsProductCodeInvalidPT");
        smsSimNotSalePT = ResourceBundle.getBundle("configPayBonus").getString("smsSimNotSalePT");
        smsSimSerialEmptyPT = ResourceBundle.getBundle("configPayBonus").getString("smsSimSerialEmptyPT");
        smsChangeSimFailedPT = ResourceBundle.getBundle("configPayBonus").getString("smsChangeSimFailedPT");
        smsSim4GAlreadyUsedPT = ResourceBundle.getBundle("configPayBonus").getString("smsSim4GAlreadyUsedPT");
        smsNotSuportedChange4gTo3gPT = ResourceBundle.getBundle("configPayBonus").getString("smsNotSuportedChange4gTo3gPT");
        k4sNO = 2L;
        arrVasCodeConnection = vasCodeConnection.split("\\|");
        lstMapVasCodeConnection = new ArrayList<HashMap>();
        for (String vasConn : arrVasCodeConnection) {
            String[] arrConn = vasConn.split("\\:");
            HashMap map = new HashMap();
            map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
            lstMapVasCodeConnection.add(map);
        }

        vasCodeParam = ResourceBundle.getBundle("configPayBonus").getString("vasCodeParam");
        arrVasCodeParam = vasCodeParam.split("\\|");
        lstMapVasCodeParam = new ArrayList<HashMap>();
        for (String vasConn : arrVasCodeParam) {
            String[] arrConn = vasConn.split("\\:");
            HashMap map = new HashMap();
            map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
            lstMapVasCodeParam.add(map);
        }

        vasCodeActionType = ResourceBundle.getBundle("configPayBonus").getString("vasCodeActionType");
        arrVasCodeActionType = vasCodeActionType.split("\\|");
        lstMapVasCodeActionType = new ArrayList<HashMap>();
        for (String vasConn : arrVasCodeActionType) {
            String[] arrConn = vasConn.split("\\:");
            HashMap map = new HashMap();
            map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
            lstMapVasCodeActionType.add(map);
        }

        vasCodeChannel = ResourceBundle.getBundle("configPayBonus").getString("vasCodeChannel");
        arrVasCodeChannel = vasCodeChannel.split("\\|");
        lstMapVasCodeChannel = new ArrayList<HashMap>();
        for (String vasConn : arrVasCodeChannel) {
            String[] arrConn = vasConn.split("\\:");
            HashMap map = new HashMap();
            map.put(arrConn[0].trim().toUpperCase(), arrConn[1].trim());
            lstMapVasCodeChannel.add(map);
        }

        lstMapVasCodeAmount = new ArrayList<HashMap>();
        for (String vasAmount : arrVasCodeAmountMoney) {
            String[] tmp = vasAmount.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapVasCodeAmount.add(map);
        }
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            RequestChangeSim moRecord = (RequestChangeSim) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String staffCode;
        long timeSt;
        boolean newSimIs4G;
        boolean oldSimIs4G;
        Long actionAuditId;
        String newImsi;
        String oldImsi;
        String newEkiValue;
        String oldSerial;
        SubInfo subInfo;
        String productCode;
//        Long reasonId;
        String resultModImsi;
//        String resultModEps;
        String resultModTPLOPTGPRS;
        String resultRemoveKI;
        String resultChangeSim;
        String shopCode;
        //boolean isUssd;
        String channelType;
//        Long saleTransId;
        int simStatus;
//        Long staffId;

        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            staffCode = "";
            newSimIs4G = false;
            oldSimIs4G = false;
            actionAuditId = 0L;
            newImsi = "";
            newEkiValue = "";
            subInfo = null;
            oldImsi = "";
            productCode = "";
            oldSerial = "";
//            reasonId = Long.parseLong(strReasonId);
            resultModImsi = "";
//            resultModEps = "";
            resultModTPLOPTGPRS = "";
            resultRemoveKI = "";
            resultChangeSim = "";
            shopCode = "";
            //isUssd = false;
            channelType = "";
//            saleTransId = 0L;
            simStatus = 9;
//            staffId = 0L;

            RequestChangeSim bn = (RequestChangeSim) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                if (bn.getChannelType() == null || bn.getChannelType().trim().length() <= 0) {
                    logger.warn("Channel type is null or empty, request_changesim_id " + bn.getId());
                    bn.setResultCode("03");
                    bn.setDescription("Channel type is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), true);
                    continue;
                }

                //if ("USSD".equalsIgnoreCase(bn.getChannelType()) || "BCCS".equalsIgnoreCase(bn.getChannelType())) {
                //logger.warn("ChangeSim using USSD, request_changesim_id " + bn.getId() + " channel " + bn.getChannelType());
                //isUssd = true;
                //channelType = "USSD";
                //}
                if ("USSD".equalsIgnoreCase(bn.getChannelType())) {
                    logger.warn("ChangeSim using BCCS, request_changesim_id " + bn.getId() + " channel " + bn.getChannelType());
                    channelType = "USSD";
                }
                if ("BCCS".equalsIgnoreCase(bn.getChannelType())) {
                    logger.warn("ChangeSim using BCCS, request_changesim_id " + bn.getId() + " channel " + bn.getChannelType());
                    channelType = "BCCS";
                }
                if ("mBCCS".equalsIgnoreCase(bn.getChannelType())) {
                    logger.warn("ChangeSim using mBCCS, request_changesim_id " + bn.getId() + " channel " + bn.getChannelType());
                    channelType = "mBCCS";
                }
                if ("".equals(channelType)) {
                    logger.warn("Channel type invalid " + bn.getId() + "channel " + bn.getChannelType());
                    bn.setResultCode("20");
                    bn.setDescription("Channel type invalid");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
                if ("BCCS".equals(channelType) || "mBCCS".equals(channelType)) {
                    staffCode = bn.getStaffCode();
                    if ("".equals(staffCode)) {
                        logger.warn("staffCode in request_changesim_4g is null or empty, request_changesim_id " + bn.getId());
                        bn.setResultCode("01");
                        bn.setDescription("staffCode in request_changesim_4g is null or empty");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        sendSmsToCustomer(bn.getIsdn(), smsSimInfoInvalid, smsSimInfoInvalidPT, bn.getUssd_loc(), true);
                        continue;
                    }
                }

                if (bn.getIsdn() == null || bn.getIsdn().trim().length() <= 0) {
                    logger.warn("ISDN is null or empty, id " + bn.getId());
                    bn.setResultCode("02");
                    bn.setDescription("ISDN is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsCusInfoInvalid, smsCusInfoInvalidPT, bn.getUssd_loc(), true);
                    continue;
                }
                if (bn.getNewSerial() == null || bn.getNewSerial().trim().length() <= 0) {
                    logger.warn("Serial Sim is null or empty, request_changesim_id " + bn.getId());
                    bn.setResultCode("03");
                    bn.setDescription("Serial Sim is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsSimSerialEmpty, smsSimSerialEmptyPT, bn.getUssd_loc(), true);
                    continue;
                }

//                Step 1: Check serial sim 4G based on stockModelId SIM 4G
//                stockModelId = db.getStockModelIdBySerial(bn.getNewSerial());// >> getStockModel of SIM based on Serial
				/*isSim4G = db.isSim4G(bn.getNewSerial(), 207607L);//When deploy server getStockModelId real and replace stockModelId test...
                 if (!isSim4G) {
                 logger.warn("-->>Serial of sim isn't 4G or empty, request_changesim_id " + bn.getId());
                 bn.setResultCode("04");
                 bn.setDescription("Serial of sim isn't 4G");
                 bn.setDuration(System.currentTimeMillis() - timeSt);
                 //Send SMS to customer 
                 sendSmsToCustomer(bn.getIsdn(), smsSimSerialEmpty, smsSimSerialEmptyPT, bn.getUssd_loc(), isUssd);
                 continue;
                 }
                 */
                //if (isUssd) {
                    /*
                 if (!db.checkIdNoOfSubscriber(bn.getIsdn(), bn.getIdNo())) {
                 logger.warn("Invalid ID_No that not map to current id of customer, request_changesim_id " + bn.getId());
                 bn.setResultCode("21");
                 bn.setDescription("Invalid ID_No that not map to current id of customer");
                 bn.setDuration(System.currentTimeMillis() - timeSt);

                 continue;
                 }
                 */
                if (!db.checkCorrectProfile(bn.getIsdn())) {
                    if (!db.checkCorrectOldProfile(bn.getIsdn())) {
                        logger.warn("Invalid Profile so not support to change sim, request_changesim_id " + bn.getId());
                        bn.setResultCode("22");
                        bn.setDescription("Invalid Profile so not support to change sim");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        //Send SMS to customer 
                        sendSmsToCustomer(bn.getIsdn(), smsProfileInvalid, smsProfileInvalidPT, bn.getUssd_loc(), true);
                        continue;
                    }
                }
                //}
//                Step 1.1: Get info of SIM 4G based on serial
                String[] simInfo = db.getImsiEkiSimBySerial(bn.getNewSerial(), bn.getIsdn());
                if (simInfo == null) {
                    logger.warn("Cannot get info of sim, request_changesim_id " + bn.getId());
                    bn.setResultCode("05");
                    bn.setDescription("Cannot get info of sim");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsSimInfoInvalid, smsSimInfoInvalidPT, bn.getUssd_loc(), true);
                    continue;
                } else {
                    newImsi = simInfo[0];
                    newEkiValue = simInfo[1];
                    if (newImsi == null || newEkiValue == null || newImsi.isEmpty() || newEkiValue.isEmpty()) {
                        logger.warn("Cannot get info of sim, request_changesim_id " + bn.getId());
                        bn.setResultCode("05");
                        bn.setDescription("Cannot get info of sim");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        //Send SMS to customer 
                        sendSmsToCustomer(bn.getIsdn(), smsSimInfoInvalid, smsSimInfoInvalidPT, bn.getUssd_loc(), true);
                        continue;
                    }
                }

//                Step 2: Generate actionAuditId for save Log
                actionAuditId = db.getSequence("SEQ_ACTION_AUDIT", "cm_pre");
//                Step 3: Get subscriber info
                subInfo = db.getSubscriberInfo(bn.getIsdn());
                if (subInfo == null) {
                    logger.warn("Cannot get subscriber info, request_changesim_id " + bn.getId() + ", isdn: " + bn.getIsdn());
                    bn.setResultCode("06");
                    bn.setDescription("Cannot get subscriber info");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsCusInfoInvalid, smsCusInfoInvalidPT, bn.getUssd_loc(), true);
                    continue;
                }
                if ("03".equals(subInfo.getActStatus())) {
                    logger.warn("Subscriber not yet active, request_changesim_id " + bn.getId() + ", isdn: " + bn.getIsdn());
                    bn.setResultCode("07");
                    bn.setDescription("Subscriber not yet active");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsSubNotAcitive, smsSubNotAcitivePT, bn.getUssd_loc(), true);
                    continue;
                }
                oldImsi = subInfo.getImsi();
                oldSerial = subInfo.getSerial();
                newSimIs4G = db.isSim4G(bn.getNewSerial(), 207607L);//When deploy server getStockModelId real and replace stockModelId test...
                oldSimIs4G = db.isSim4G(oldSerial, 207607L);
                logger.warn("Request change sim newSimIs4G : " + newSimIs4G + " oldSimIs4G: " + oldSimIs4G + " ID: " + bn.getId() + ", isdn: " + bn.getIsdn());
                if (oldImsi.equals(newImsi)) {
                    logger.warn("oldImsi and newImsi is duplicate, request_changesim_id " + bn.getId() + ", isdn: " + bn.getIsdn());
                    bn.setResultCode("08");
                    bn.setDescription("oldImsi and newImsi is duplicate");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), true);
                    continue;
                }

                if (oldSimIs4G && !newSimIs4G) {
                    logger.warn("The old sim is 4G, but the new sim is 3G,so not suport changing from 4G to 3G request_changesim_id " + bn.getId());
                    bn.setResultCode("50");
                    bn.setDescription("Not suport change from 4G to 3G");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsNotSuportedChange4gTo3g, smsNotSuportedChange4gTo3gPT, bn.getUssd_loc(), true);
                    continue;
                }
//                Get and check ProductCode mapping
                productCode = subInfo.getProductCode();
                if (productCode == null || productCode.trim().length() <= 0) {
                    logger.warn("productCode is null or empty, request_changesim_id " + bn.getId() + ", isdn: " + bn.getIsdn());
                    bn.setResultCode("09");
                    bn.setDescription("productCode is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    //Send SMS to customer 
                    sendSmsToCustomer(bn.getIsdn(), smsProductCodeInvalid, smsProductCodeInvalidPT, bn.getUssd_loc(), true);
                    continue;
                }
//                Step 4: Check newImsi already sold    
                simStatus = db.getSimStatus(bn.getNewSerial());
                if (simStatus == 1) {
                    logger.warn("SIM not yet sale, serial: " + bn.getNewSerial()
                            + " isdn " + bn.getIsdn());
                    bn.setResultCode("21");
                    bn.setDescription("SIM not yet sale");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    sendSmsToCustomer(bn.getIsdn(), smsSimNotSale, smsSimNotSalePT, bn.getUssd_loc(), true);
                    continue;
                } else if (simStatus == 2) {
                    logger.warn("SIM already used, serial: " + bn.getNewSerial()
                            + " isdn " + bn.getIsdn());
                    bn.setResultCode("22");
                    bn.setDescription("SIM 4G already used");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    sendSmsToCustomer(bn.getIsdn(), smsSim4GAlreadyUsed, smsSim4GAlreadyUsedPT, bn.getUssd_loc(), true);
                    continue;
                } else if (simStatus != 0) {
                    logger.warn("Can not get status of new Sim: " + bn.getNewSerial()
                            + " isdn " + bn.getIsdn());
                    bn.setResultCode("23");
                    bn.setDescription("Can not get status of new Sim");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    sendSmsToCustomer(bn.getIsdn(), smsSimSerialEmpty, smsSimSerialEmptyPT, bn.getUssd_loc(), true);
                    continue;
                }
//                String saleServiceCode = db.getSaleServiceCode(reasonId, productCode, bn.getIsdn());
//                if (saleServiceCode == null) {
//                    logger.warn("Can not find saleServiceCode, productCode: " + productCode + " for serial: " + bn.getNewSerial()
//                            + " isdn " + bn.getIsdn());
//                    bn.setResultCode("10");
//                    bn.setDescription("Can not find saleServiceCode");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
//                SaleServices saleService = db.getSaleService(saleServiceCode, bn.getIsdn());
//                if (saleService == null) {
//                    //Update status sub_id_no ve 0
//                    logger.warn("Can not find saleServices, productCode: " + productCode + " for serial: " + bn.getNewSerial()
//                            + " isdn " + bn.getIsdn());
//                    bn.setResultCode("11");
//                    bn.setDescription("Can not find saleServices with saleServicesCode " + saleServiceCode);
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
//                SaleServicesPrice saleServicesPrice = db.getSaleServicesPrice(saleService.getSaleServicesId(), bn.getIsdn());
//                if (saleServicesPrice == null) {
//                    //Update status sub_id_no ve 0
//                    logger.warn("Can not find saleServicesPrices, productCode: " + productCode + " for serial: " + bn.getNewSerial()
//                            + " isdn " + bn.getIsdn());
//                    bn.setResultCode("12");
//                    bn.setDescription("Can not find saleServicesPrices");
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    continue;
//                }
//                Step 4: Check newImsi...
//                if (!isUssd) {
//                    String strTempId = db.getShopIdStaffIdByStaffCode(staffCode);//SHOP_ID|STAFF_ID
//                    if (strTempId.isEmpty() && strTempId.length() <= 0) {
//                        logger.warn("Cannot get staffInfo: SHOP_ID|STAFF_ID , staffCode: " + staffCode
//                                + " isdn " + bn.getIsdn());
//                        bn.setResultCode("20");
//                        bn.setDescription("Cannot get Information: SHOP_ID|STAFF_ID");
//                        bn.setDuration(System.currentTimeMillis() - timeSt);
//                        continue;
//                    }
//                    String[] arrTempId = strTempId.split("\\|");
//                    staffId = Long.valueOf(arrTempId[1]);
//                    if (!db.checkSimAlreadySale(staffId, bn.getNewSerial())) {
//                        logger.warn("SIM 4G not yet sale, serial: " + bn.getNewSerial()
//                                + " isdn " + bn.getIsdn());
//                        bn.setResultCode("21");
//                        bn.setDescription("SIM 4G not yet sale");
//                        bn.setDuration(System.currentTimeMillis() - timeSt);
//                        continue;
//                    }
//                Step 5: Make saleTrans
//                    if (!db.checkStockTotalOfStaff(staffId, stockModelId)) {
//                        logger.warn("StockTotal have quantity and quantity issue not enough , staffCode: " + staffCode
//                                + " isdn " + bn.getIsdn());
//                        bn.setResultCode("22");
//                        bn.setDescription("Quantity and Quantity issue is not enough.");
//                        bn.setDuration(System.currentTimeMillis() - timeSt);
//                        continue;
//                    }
//                    Price priceOfSim = db.getPrice(stockModelId, saleService.getSaleServicesId());
//                    Double totalAmount = saleServicesPrice.getPrice() + priceOfSim.getPrice();// + PRICE OF SIM.... ???;
//                    saleTransId = db.getSequence("SALE_TRANS_SEQ", "dbsm");
//                    shopCode = db.getShopCode(Long.valueOf(arrTempId[0]));
//
//                    int rsMakeSaleTrans = db.insertSaleTrans(saleTransId, Long.valueOf(arrTempId[0]), staffId,
//                            saleService.getSaleServicesId(), saleServicesPrice.getSaleServicesPriceId(), totalAmount, subInfo.getSubId(),
//                            bn.getIsdn(), reasonId, "");
//                    if (rsMakeSaleTrans != 1) {
//                        //Insert fail, insert log, send sms
//                        db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdn(), bn.getNewSerial(), Long.valueOf(arrTempId[0]),
//                                staffId, saleService.getSaleServicesId(), saleServicesPrice.getSaleServicesPriceId(),
//                                reasonId, 0L, 0L, "SALE_TRANS");
//                        for (String isdn : arrIsdnReceiveError) {
//                            db.sendSms(isdn, "Make saleTrans fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
//                                    + saleTransId, "86142");
//                        }
//                    }
                //Make sale trans detail
                //5.1. Detail for SaleServices
//                    Long saleTransDetailSaleService = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
//                    int rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, "", "", saleService.getSaleServicesId(),
//                            String.valueOf(saleServicesPrice.getSaleServicesPriceId()),
//                            "", "", "", "", saleServiceCode, saleService.getName(), saleService.getAccountModelCode(), saleService.getAccountModelName(), "17", "",
//                            "", String.valueOf(saleServicesPrice.getPrice()), saleServicesPrice.getPrice(), 0.0);
//                    if (rsSaleTransDetailSaleServices != 1) {
//                        //Insert fail, insert log, send sms
//                        db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdn(), bn.getNewSerial(), Long.valueOf(arrTempId[0]),
//                                staffId, saleService.getSaleServicesId(), saleServicesPrice.getSaleServicesPriceId(),
//                                reasonId, saleTransDetailSaleService, 0L, "SALE_TRANS_DETAIL");
//                        for (String isdn : arrIsdnReceiveError) {
//                            db.sendSms(isdn, "Make saleTransDetail for SaleServices fail, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailSaleService, "86142");
//                        }
//                    }
//                    SIM ALREADY SALE NO NEED TO MAKE SALE_TRANS_DETAIL FOR SIM AND SALE_TRANS SERIAL
                //5.2. Detail for SIM
//                    StockModel stockModel = db.findStockModelById(stockModelId);
//                    Long saleTransDetailIsdn = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
//                    int rsSaleTransDetailIsdn = db.insertSaleTransDetail(saleTransDetailIsdn, saleTransId, String.valueOf(stockModelId),
//                            String.valueOf(priceOfSim.getPriceId()), saleService.getSaleServicesId(), "",
//                            "4", "Sim Mobile", stockModel.getStockModelCode(), stockModel.getName(), "", "", stockModel.getAccountingModelCode(), stockModel.getAccountingModelName(), "", "17",
//                            String.valueOf(priceOfSim.getPrice()), "", priceOfSim.getPrice(), 0.0);
//                    if (rsSaleTransDetailIsdn != 1) {
//                        //Insert fail, insert log, send sms
//                        db.insertLogMakeSaleTransFail(saleTransId, bn.getIsdn(), bn.getNewSerial(), Long.valueOf(arrTempId[0]),
//                                staffId, saleService.getSaleServicesId(), saleServicesPrice.getSaleServicesPriceId(),
//                                reasonId, saleTransDetailIsdn, 0L, "SALE_TRANS_DETAIL");
//                        for (String isdn : arrIsdnReceiveError) {
//                            db.sendSms(isdn, "Make saleTransDetail for Isdn fail, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailIsdn, "86142");
//                        }
//                    }
                //5.3. Make saleTransSerial
//                    Long saleTransSerialId = db.getSequence("SALE_TRANS_SERIAL_SEQ", "dbsm");
//                    int rsSaleTransSerial = db.insertSaleTransSerial(saleTransSerialId, saleTransDetailIsdn, stockModelId, bn.getNewSerial());
//                    if (rsSaleTransSerial != 1) {
//                        //Insert fail, insert log, send sms
//                        db.insertLogMakeSaleTransFail(0L, bn.getIsdn(), bn.getNewSerial(), Long.valueOf(arrTempId[0]),
//                                staffId, saleService.getSaleServicesId(), saleServicesPrice.getSaleServicesPriceId(),
//                                reasonId, saleTransDetailIsdn, saleTransSerialId, "SALE_TRANS_SERIAL");
//                        for (String isdn : arrIsdnReceiveError) {
//                            db.sendSms(isdn, "Make saleTransSerial fail, more detail in table: kit_make_sale_trans_fail. saleTransSerialId: " + saleTransSerialId, "86142");
//                        }
//                    }
//                }
//                Step 6: ChangeSIM
//                Step 6.1: Check KI

                String checkKI = pro.checkKiSim("258" + bn.getIsdn(), newImsi);
                //Error when query KI >>> KI not load...etc....
                if (!"0".equals(checkKI)) {
                    if ("ERR3048".equals(checkKI)) {
                        //KI not load >>> Add KI
                        String addKI = pro.addKiSim("258" + bn.getIsdn(), newEkiValue, newImsi, k4sNO + "", newSimIs4G);
                        if (!"0".equals(addKI)) {
                            logger.info("Add KI SIM  fail.");
                            bn.setResultCode("12");
                            bn.setDescription("Add KI sim fail, errorCode: " + checkKI);
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), true);
                            continue;
                        }
                    } else {
                        logger.info("Query fail Error isn't KI not load, isdn: " + bn.getIsdn() + ", imsi: " + newImsi);
                        bn.setResultCode("13");
                        bn.setDescription("Query KI not success, errorCode: " + checkKI);
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), true);
                        continue;
                    }
                }
//                Step 6.2: HLR_HW_MODI_IMSI
                resultModImsi = pro.modImsi("258" + bn.getIsdn(), newImsi);
                if (!"0".equals(resultModImsi) && !"ERR3050".equals(resultModImsi)) {//ERR3050: New IMSI in use...
                    logger.error("Fail to Mod IMSI " + bn.getIsdn() + ", newImsi: " + newImsi + ", cancel transaction...");
//                    if (!isUssd) {
//                        db.cancelSaleTrans(staffCode, saleTransId);
//                    }
                    bn.setResultCode("15");
                    bn.setDescription("Fail to Mod IMSI. responseCode: " + resultModImsi);
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), true);

                    continue;
                }
//                Step 6.3: HLR_HW_MOD_EPS
//                LinhNBV 20200403: For case ChangeSim 3G -> 4G: Call HLR_HW_MOD_TPLOPTGPRS with TPLID = 3 (Old is 2). 4G -> 4G no need.
                if (newSimIs4G && !oldSimIs4G) {
//                    resultModEps = pro.modEPS("258" + bn.getIsdn(), "TRUE", 400000000, 800000000);
//                    if (!"0".equals(resultModEps)) {
//                        logger.error("Fail to Mod EPS " + bn.getIsdn() + ", newImsi: " + newImsi + ", cancel transaction...");
////                    if (!isUssd) {
////                        db.cancelSaleTrans(staffCode, saleTransId);
////                    }
//                        //Rollback MOD IMSI
//                        rollbackModIMSI(bn, timeSt, newEkiValue, newImsi, oldImsi, k4sNO, newSimIs4G);
//
//                        bn.setResultCode("16");
//                        bn.setDescription("Fail to Mod EPS. responseCode: " + resultModEps);
//                        bn.setDuration(System.currentTimeMillis() - timeSt);
//                        sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), true);
//                        continue;
//                    }
//                Step 6.4: HLR_HW_MOD_TPLOPTGPRS
                    resultModTPLOPTGPRS = pro.modTPLOPTGPRS("258" + bn.getIsdn(), "TRUE", "3", "TRUE");
                    if (!"0".equals(resultModTPLOPTGPRS)) {
                        logger.error("Fail to Mod TPLOPTGPRS " + bn.getIsdn() + ", newImsi: " + newImsi + ", cancel transaction...");
//                    if (!isUssd) {
//                        db.cancelSaleTrans(staffCode, saleTransId);
//                    }
                        //Rollback MOD IMSI
                        rollbackModIMSI(bn, timeSt, newEkiValue, newImsi, oldImsi, k4sNO, newSimIs4G);

                        bn.setResultCode("17");
                        bn.setDescription("Fail to Mod TPLOPTGPRS. responseCode: " + resultModTPLOPTGPRS);
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), true);
                        continue;
                    }
                }
//                Step 6.5: HLR_HW_REMOVE_KI
                resultRemoveKI = pro.removeKI(oldImsi);
                if (!"0".equals(resultRemoveKI)) {
                    logger.warn("Fail to Remove KI " + bn.getIsdn() + ", newImsi: " + newImsi + ", cancel transaction...");
////                    if (!isUssd) {
////                        db.cancelSaleTrans(staffCode, saleTransId);
////                    }
//                    bn.setResultCode("18");
//                    bn.setDescription("Fail to Remove KI. responseCode: " + resultRemoveKI);
//                    bn.setDuration(System.currentTimeMillis() - timeSt);
//                    sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), isUssd);
//                    continue;
                }
//                Step 6.6: OCSHW_CHANGESIM
                resultChangeSim = pro.changeSim("258" + bn.getIsdn(), newImsi);
                if (!"0".equals(resultChangeSim)) {
                    logger.error("Fail to call OCS_CHANGESIM " + bn.getIsdn() + ", newImsi: " + newImsi + ", cancel transaction...");
//                    if (!isUssd) {
//                        db.cancelSaleTrans(staffCode, saleTransId);
//                    }
                    //Rollback MOD IMSI
                    rollbackModIMSI(bn, timeSt, newEkiValue, newImsi, oldImsi, k4sNO, newSimIs4G);

                    bn.setResultCode("19");
                    bn.setDescription("Fail to call OCS_CHANGESIM. responseCode: " + resultChangeSim);
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    sendSmsToCustomer(bn.getIsdn(), smsChangeSimFailed, smsChangeSimFailedPT, bn.getUssd_loc(), true);
                    continue;
                }

//                Step 5.1: Update subMB, sub_sim_mb
                db.updateSubMbChangeSim(subInfo.getSubId(), bn.getIsdn(), newImsi, bn.getNewSerial());
                if (db.checkSubSimMb(subInfo.getSubId(), oldImsi)) {
//                    Update sub_sim_mb
                    db.updateSubSimMb(subInfo.getSubId(), oldImsi);
                }
//                    insert sub_sim_mb
                db.insertSubSimMb(subInfo.getSubId(), newImsi);
//                Step 5.2: Update Stock_Sim and update Stock_Total
                db.updateStockSim(2L, 2L, bn.getNewSerial(), bn.getIsdn());

                //                if (!isUssd) {
                //                    db.updateStockTotal(staffId, stockModelId);
                //                }
                //                Step 6: Save actionLog
                if ("mBCCS".equals(channelType) || "BCCS".equals(channelType)) {
                    db.insertActionAudit(actionAuditId, bn.getIsdn(), bn.getNewSerial(),
                            "Change SIM successfully for " + bn.getIsdn() + ".", subInfo.getSubId(), shopCode, staffCode);
                } else {
                    db.insertActionAudit(actionAuditId, bn.getIsdn(), bn.getNewSerial(),
                            "Change SIM successfully for " + bn.getIsdn() + ".", subInfo.getSubId(), "USSD", "USSD");
                }

                //				20190326 - Bacnx Check eWallet when the customer had broken sim
                logger.info("Check ewallet  isdn " + bn.getIsdn() + ",custID " + subInfo.getCustId());
                try {
                    checkeWallet(bn, subInfo, timeSt);
                } catch (Exception e) {
                    logger.warn("fail call ewallet isdn " + bn.getIsdn() + ",custID " + subInfo.getCustId() + " " + e.toString());
                }
                //End check ewallet
                //                Step 6.1: Send sms
                String tempMsg = changeSimSucessfully;
                tempMsg = tempMsg.replace("%SIM_SERIAL%", bn.getNewSerial());
                if ("mBCCS".equals(channelType) || "BCCS".equals(channelType)) {
                    String tel = db.getTelByStaffCode(staffCode);
                    db.sendSms(tel, tempMsg, "86904");
                    logger.info("isdn " + bn.getIsdn() + " Message send to staff: " + tempMsg);
                }
                //                Send sms customer...
                if ("BCCS".equals(channelType)) {
                    db.sendSms(bn.getIsdn(), changeSimSurvay, "86904");
                }
                db.sendSms(bn.getIsdn(), tempMsg, "86904");
                logger.info("isdn " + bn.getIsdn() + " Message send to customer: " + tempMsg);
                bn.setResultCode("0");
                bn.setDescription("Change SIM Successfully.");
                bn.setDuration(System.currentTimeMillis() - timeSt);
                bn.setOldImsi(oldImsi);
                bn.setOldSerial(oldSerial);
                bn.setActionAuditId(actionAuditId);

//                Step 7: Register Vas
                double amountVas = 0;
                if (bn.getVasCode() != null && bn.getVasCode().length() > 0) {
                    logger.info("Customer register vas with package: " + bn.getVasCode() + " isdn " + bn.getIsdn());
                    for (int i = 0; i < lstMapVasCodeAmount.size(); i++) {
                        if (lstMapVasCodeAmount.get(i).containsKey(bn.getVasCode().trim().toUpperCase())) {
                            amountVas = Double.valueOf(lstMapVasCodeAmount.get(i).get(bn.getVasCode().trim().toUpperCase()).toString()) * Double.valueOf(vasCodeAmountRate);
                            logger.error("Amount money will be minus on eMola system when register VAS: " + amountVas + ", VAS_CODE: " + bn.getVasCode() + ", isdn: " + bn.getIsdn());
                            break;
                        }
                    }
                    ResponseWallet responseWallet = EWalletUtil.paymentVoucher(db, bn, Double.valueOf(amountVas), logger, bn.getIsdn());
                    if (responseWallet == null || !"01".equals(responseWallet.getResponseCode())) {
                        //Update status sub_id_no ve 0
                        logger.warn("Fail to charge Emola, serial: " + bn.getNewSerial()
                                + " isdn " + bn.getIsdn());
                        bn.setResultCode("20");
                        bn.setDescription("Change SIM Successfully, But failt to charge Emola for registering VasCode");
                        continue;
                    }
                    logger.info("Customer register vas with package: " + bn.getVasCode() + " isdn " + bn.getIsdn());
                    String amountTopup = "";
                    for (int i = 0; i < lstMapVasCodeAmount.size(); i++) {
                        if (lstMapVasCodeAmount.get(i).containsKey(bn.getVasCode().trim().toUpperCase())) {
                            amountTopup = lstMapVasCodeAmount.get(i).get(bn.getVasCode().trim().toUpperCase()).toString();
                            logger.info("Amount money will be topup: " + amountTopup + ", VAS_CODE: " + bn.getVasCode() + ", isdn: " + bn.getIsdn());
                            break;
                        }
                    }
                    if (amountTopup.length() > 0) {
                        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
                        Date now = new Date();
                        String strDate = sdfDate.format(now).replace("-", "").replace(".", "").replace(" ", "");
                        String requestIdTopup = "KIT_" + strDate + bn.getId();
                        String rsTopup = pro.topupWS(requestIdTopup, bn.getIsdn(), amountTopup);
                        if ("0".equals(rsTopup)) {
                            logger.info("Topup successfully for sub: " + bn.getIsdn() + ", vas_code: " + bn.getVasCode() + ", amount: " + amountTopup);
                            String connName = "";
                            for (int i = 0; i < lstMapVasCodeConnection.size(); i++) {
                                if (lstMapVasCodeConnection.get(i).containsKey(bn.getVasCode().trim().toUpperCase())) {
                                    connName = lstMapVasCodeConnection.get(i).get(bn.getVasCode().trim().toUpperCase()).toString();
                                    logger.info("Connection config will be insert MO: " + connName + ", VAS_CODE: " + bn.getVasCode() + ", isdn: " + bn.getIsdn());
                                    break;
                                }
                            }
                            String param = "";
                            for (int i = 0; i < lstMapVasCodeParam.size(); i++) {
                                if (lstMapVasCodeParam.get(i).containsKey(bn.getVasCode().trim().toUpperCase())) {
                                    param = lstMapVasCodeParam.get(i).get(bn.getVasCode().trim().toUpperCase()).toString();
                                    logger.info("Param config will be insert MO: " + connName + ", VAS_CODE: " + bn.getVasCode() + ", isdn: " + bn.getIsdn());
                                    break;
                                }
                            }
                            String actionType = "";
                            for (int i = 0; i < lstMapVasCodeActionType.size(); i++) {
                                if (lstMapVasCodeActionType.get(i).containsKey(bn.getVasCode().trim().toUpperCase())) {
                                    actionType = lstMapVasCodeActionType.get(i).get(bn.getVasCode().trim().toUpperCase()).toString();
                                    logger.info("Action Type config will be insert MO: " + connName + ", VAS_CODE: " + bn.getVasCode() + ", isdn: " + bn.getIsdn());
                                    break;
                                }
                            }
                            String channel = "";
                            for (int i = 0; i < lstMapVasCodeChannel.size(); i++) {
                                if (lstMapVasCodeChannel.get(i).containsKey(bn.getVasCode().trim().toUpperCase())) {
                                    channel = lstMapVasCodeChannel.get(i).get(bn.getVasCode().trim().toUpperCase()).toString();
                                    logger.info("Channel config will be insert MO: " + connName + ", VAS_CODE: " + bn.getVasCode() + ", isdn: " + bn.getIsdn());
                                    break;
                                }
                            }
//                            Insert MO
                            if (param != null && "?".equals(param)) {
                                param = staffCode;
                            }
                            int rsInsertMo = db.insertMO(bn.getIsdn(), bn.getVasCode(), connName, param, actionType, channel);
                            if (rsInsertMo == 1) {
                                logger.info("Insert MO successfully for sub: " + bn.getIsdn() + ", vas_code: " + bn.getVasCode() + ", amount: " + amountTopup);
                            } else {
                                logger.info("Topup successfully but fail to insert MO for sub: " + bn.getIsdn() + ", vas_code: " + bn.getVasCode() + ", amount: " + amountTopup);
                                for (String isdn : arrIsdnReceiveError) {
                                    db.sendSms(isdn, "Topup successfully but failt to insert MO for sub: " + bn.getIsdn() + ", vas_code: " + bn.getVasCode() + ". Please check and insert by hand now.", "86142");
                                }
                            }
                        } else {
                            logger.info("Topup unsuccessfully for sub: " + bn.getIsdn() + ", vas_code: " + bn.getVasCode() + ", amount: " + amountTopup);
                        }
                    } else {
                        logger.info("Cannot get amount topup from VAS_CODE: " + bn.getVasCode() + ", isdn: " + bn.getIsdn());
                    }
                }
                continue;
            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getActionAuditId()
                        + " id " + bn.getId() + " isdn: " + bn.getIsdn()
                        + " so continue with other transaction");
                bn.setDescription("After validate respone code is fail");
                bn.setDuration(System.currentTimeMillis() - timeSt);
                continue;
            }
        }
        listRecord.clear();
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tID|").
                append("|\tISDN|").
                append("|\tNEW_SERIAL|").
                append("|\tID_NO\t|").
                append("|\tSTATUS\t|").
                append("|\tCREATE_TIME\t|").
                append("|\tSTAFF_CODE\t|");
        for (Record record : listRecord) {
            RequestChangeSim bn = (RequestChangeSim) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getNewSerial()).
                    append("||\t").
                    append(bn.getIdNo()).
                    append("||\t").
                    append(bn.getStatus()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
                    append("||\t").
                    append(bn.getStaffCode());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        logger.info("Process Exception....");
        long timeSt = System.currentTimeMillis();
        for (Record record : listRecord) {
            RequestChangeSim bn = (RequestChangeSim) record;
            bn.setResultCode("98");
            bn.setDescription("Exception when process...Ex: " + ex.getLocalizedMessage());
            bn.setDuration(System.currentTimeMillis() - timeSt);
            continue;
        }
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }

    /**
     * Send SMS to customer when use USSD to change SIM
     *
     * @param isdn
     * @param msg
     * @param msgPT
     * @param ussdLoc
     * @param isUssd
     */
    public void sendSmsToCustomer(String isdn, String msg, String msgPT, String ussdLoc, boolean isUssd) {
        if (isUssd) {
            if (msg != null && msg.length() > 0 && isdn != null && isdn.length() > 0) {
                String msisdn = isdn.startsWith("258") ? isdn : "258" + isdn;
                if ("EN".equalsIgnoreCase(ussdLoc)) {
                    db.sendSms(msisdn, msg, "86142");
                } else {
                    db.sendSms(msisdn, msgPT, "86142");
                }
            }
        }
    }

    /**
     * Rollback Mod IMSI, replace new IMIS with old IMSI
     *
     * @param bn
     * @param timeSt
     * @param newEkiValue
     * @param newImsi
     * @param oldImsi
     * @param k4sNO
     * @param isSim4G
     * @return
     */
    public String rollbackModIMSI(RequestChangeSim bn, Long timeSt, String newEkiValue, String newImsi, String oldImsi, Long k4sNO, boolean isSim4G) {
        logger.info("--->Start rollback process." + bn.getIsdn() + ", newImsi: " + newImsi + ", oldImsi: " + oldImsi + ", Rollback to " + oldImsi);
        String oldEKI = db.getEKI(oldImsi);
        String checkKI = pro.checkKiSim("258" + bn.getIsdn(), oldImsi);
        //try to add KI again
        if (!"0".equals(checkKI)) {
            if ("ERR3048".equals(checkKI)) {
                //KI not load >>> Add KI
                String addKI = pro.addKiSim("258" + bn.getIsdn(), oldEKI, oldImsi, k4sNO + "", isSim4G);
                if (!"0".equals(addKI)) {
                    logger.info("Rollback KI: Add KI SIM 4G failed." + bn.getIsdn() + ", newImsi: " + newImsi + ", oldImsi: " + oldImsi);
                }
            } else {
                logger.info("Rollback KI: Query fail Error isn't KI not load, isdn: " + bn.getIsdn() + ", newImsi: " + newImsi + ", oldImsi: " + oldImsi);
            }
        } else {
            logger.info("Rollback KI: Add KI SIM 4G susseccfully." + bn.getIsdn() + ", newImsi: " + newImsi + ", oldImsi: " + oldImsi);
        }
        //Rollback MOD IMSI
        String resultRollbackModImsi = pro.modImsi("258" + bn.getIsdn(), oldImsi);
        if (!"0".equals(resultRollbackModImsi)) {
            logger.error("Rollback MOD: IMSDI failed " + bn.getIsdn() + ", newImsi: " + newImsi + ", oldImsi: " + oldImsi);
        } else {
            logger.error("Rollback MOD: IMSDI susseccfully " + bn.getIsdn() + ", imsi " + oldImsi);
        }
        logger.info("--->End rollback process." + bn.getIsdn() + ", newImsi: " + newImsi + ", oldImsi: " + oldImsi);
        return resultRollbackModImsi;
    }

    public void checkeWallet(RequestChangeSim bn, SubInfo subInfo, long timeSt) throws Exception {
        logger.info("Start check eWallet...........isdn " + bn.getIsdn() + ",custID " + subInfo.getCustId());
        ActionLogPr logPr = new ActionLogPr();
        Customer customer = db.getCustomerInfo(subInfo.getCustId());
        String gender = "1";
        String progressType = "5";//changsim
        if (customer != null) {
            gender = "M".equalsIgnoreCase(customer.getGender()) ? "1" : "0";
            String response = EWalletUtil.checkToWallet(customer.getCustId().toString(),
                    customer.getSubName(), bn.getIsdn(),
                    customer.getSubName(), gender, customer.getBirthDate(),
                    customer.getIdType(), customer.getIdNo(), customer.getAddress(), progressType, customer.getCustId().toString(), logger);
            logger.info("response call Ewallet when change sim " + bn.getIsdn()
                    + " detail respone: " + response);
            ResponseWallet responseWallet = new ResponseWallet();
            logPr.setIsdn(bn.getIsdn());
            logPr.setRequest("custId:" + customer.getCustId().toString() + ";mobile:" + bn.getIsdn() + ";progressType:" + progressType);
            logPr.setResponse(response);
            logPr.setUserName("PROCESS_CHANGE_SIM");
            logPr.setShopCode("MV");
            logPr.setImsi(subInfo.getImsi());
            logPr.setSerial(subInfo.getSerial());
            db.saveActionLogPrWS(logPr);
            if ("Error:".equals(response)) {
                logger.info("Error when check eWallet " + bn.getIsdn() + ",custID " + subInfo.getCustId());
                logPr.setResponseCode("09");
            } else {
                Gson gson = new Gson();
                responseWallet = gson.fromJson(response, ResponseWallet.class);
                if (responseWallet == null && responseWallet.getResponseCode() == null) {
                    logger.info("Fail to call eWallet...........isdn " + bn.getIsdn() + ",custID " + subInfo.getCustId());
                    logPr.setResponseCode("00");
                } else {
                    logPr.setResponseCode("01");
                }
            }
        }
        logger.info("End check eWallet isdn " + bn.getIsdn() + ",custID " + subInfo.getCustId());
    }
}
