/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.services;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.viettel.data.ws.utils.EWalletUtil;
import com.viettel.vas.wsfw.common.WebserviceAbstract;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import com.viettel.vas.wsfw.common.Common;
import com.viettel.vas.wsfw.common.Vas;
import com.viettel.vas.wsfw.database.DbPost;
import com.viettel.vas.wsfw.database.DbPre;
import com.viettel.vas.wsfw.database.DbProcessor;
import com.viettel.vas.wsfw.object.Subscriber;
import com.viettel.vas.wsfw.object.UserInfo;
import com.viettel.data.ws.utils.Exchange;
import com.viettel.smsfw.manager.AppManager;
import com.viettel.vas.util.ExchangeClientChannel;
import com.viettel.vas.wsfw.object.AccountInfo;
import com.viettel.vas.wsfw.object.Config;
import com.viettel.vas.wsfw.object.KitBatchDetailModel;
import com.viettel.vas.wsfw.object.Offer;
import com.viettel.vas.wsfw.object.ProductConnectKit;
import com.viettel.vas.wsfw.object.QueryEwalletInfo;
import com.viettel.vas.wsfw.object.ResponseGroupMgt;
import com.viettel.vas.wsfw.object.ResponseWallet;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author manhnv
 * @since Feb 26, 2020
 * @version 1.0
 */
@WebService
public class GroupManagement extends WebserviceAbstract {

    DbProcessor db;
    DbPre dbPre;
    DbPost dbPost;
    Exchange exch;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public GroupManagement() throws Exception {
        super("GroupManagement");
        try {
            logger.info("Start init webservice GroupManagement");
            dbPre = new DbPre("cm_pre", logger);
            dbPost = new DbPost("cm_pos", logger);
            db = new DbProcessor("dbtopup", logger);
            exch = new Exchange(ExchangeClientChannel.getInstance("../etc/exchange_client.cfg").getInstanceChannel(), logger);

            if (Config.listMessage == null || Config.listMessage.isEmpty()) {
                Config.listMessage = dbPre.getConfigs();
            }
        } catch (Exception e) {
            logger.error("Fail init webservice GroupManagement");
            logger.error(e);
        }
    }

    @WebMethod(operationName = "RemoveMember")
    public ResponseGroupMgt RemoveMember(
            @WebParam(name = "dataInfo", targetNamespace = "") String dataInfo,
            @WebParam(name = "kitBatchId", targetNamespace = "") Integer kitBatchId,
            @WebParam(name = "processUser", targetNamespace = "") String processUser,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword) {
        long timeStart = System.currentTimeMillis();
        ResponseGroupMgt response = new ResponseGroupMgt();
        try {
            logger.info("Start process RemoveMember for sub " + dataInfo + " kitBatchId " + kitBatchId + " processUser " + processUser + " client " + wsuser);
//        step 1: validate input
            if (dataInfo == null || "".equals(dataInfo.trim())
                    || wsuser == null || "".equals(wsuser.trim())
                    || wspassword == null || "".equals(wspassword.trim())) {
                logger.warn("Invalid input sub " + dataInfo + " length " + (dataInfo == null ? 0 : dataInfo.length())
                        + " wsuser " + wsuser + " length " + (wsuser == null ? 0 : wsuser.length())
                        + " wspassword " + wspassword + " length " + (wspassword == null ? 0 : wspassword.length()));
                response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
                response.setDescription("INVALID_INPUT");
                return response;
            }
//        step 2: validate ip
            String ip = getIpClient();
            if (ip == null || "".equals(ip.trim())) {
                logger.warn("Can not get ip for sub " + dataInfo);
                response.setErrorCode(Vas.ResultCode.FAIL_GET_IP);
                response.setDescription("FAIL_GET_IP");
                return response;
            }
            UserInfo user = authenticate(db, wsuser, wspassword, ip);
            if (user == null || user.getId() < 0) {
                logger.warn("Invalid account " + dataInfo);
                response.setErrorCode(Vas.ResultCode.WRONG_ACCOUNT_IP);
                response.setDescription("WRONG_ACCOUNT_IP");
                return response;
            }

            Gson gson = new Gson();
            Type listType = new TypeToken<ArrayList<KitBatchDetailModel>>() {
            }.getType();
            List<KitBatchDetailModel> listSubs = gson.fromJson(dataInfo, listType);
            String msisdn = "";
            String transId = sdf.format(new Date()) + kitBatchId;
            for (KitBatchDetailModel tmpSub : listSubs) {
                msisdn = tmpSub.getIsdn();
                if (msisdn.startsWith(Common.config.countryCode)) {
                    msisdn = msisdn.substring(Common.config.countryCode.length());
                }
                logger.info("Check prepaid sub " + msisdn);
                Subscriber subscriber = dbPre.getSubInfoMobile(msisdn, false);
                if (subscriber == null || subscriber.getSubId() == null || "".equals(subscriber.getSubId())) {
                    //ko tim thay tren CM PRE, thuc hien tim tren CM POST
                    logger.info("Not pre, check postpaid sub " + msisdn);
                    subscriber = dbPost.getSubInfoMobile(msisdn, false);
                    if (subscriber == null || subscriber.getSubId() == null || "".equals(subscriber.getSubId())) {
                        logger.info("Not postpaid mobile, check homephone pre sub " + msisdn);
                        subscriber = dbPre.getSubInfoHomephone(msisdn, false);
                        if (subscriber == null || subscriber.getSubId() == null || "".equals(subscriber.getSubId())) {
                            logger.info("Not prepaid homephone, check postpaid homephone " + msisdn);
                            subscriber = dbPost.getSubInfoHomephone(msisdn, false);
                        }
                    }
                }
                //loi he thong CM
                if (subscriber == null) {
                    logger.info("Not Movitel sub " + msisdn);
                    response.setErrorCode(22);
                    response.setDescription("SUBSCRIBER_INVALID");
                    return response;
                }

                //neu khong co thong tin 
                if (subscriber.getSubId() == null || "".equals(subscriber.getSubId().trim())) {
                    logger.info("Can not get sub info " + msisdn);
                    response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
                    response.setDescription("SUBSCRIBER_INVALID");
                    return response;
                }

                //Check SIM chua kich hoat
                if (!"00".equals(subscriber.getActStatus())) {
                    logger.info("Can not get sub info " + msisdn);
                    response.setErrorCode(26);
                    response.setDescription("SUB_INACTIVE");
                    return response;
                }

                String cugName = db.getCUGInformation(Long.valueOf(kitBatchId));
                logger.info("RemoveMember " + msisdn + ", kit batch id " + kitBatchId + ", cug name " + cugName);
                if (cugName != null && !cugName.isEmpty() && !"N/A".equals(cugName)) {
                    String cugId = cugName.split("\\_")[1];
                    String[] resultAddMember = exch.removeMemberGroupCUG(cugId, msisdn.startsWith("258") ? msisdn : "258" + msisdn);
                    if (resultAddMember == null || resultAddMember.length == 0 || (!"0".equals(resultAddMember[0]) && !"102010462".equals(resultAddMember[0]))) {
                        logger.warn("Fail to remove member " + (msisdn.startsWith("258") ? msisdn : "258" + msisdn) + " to OCS groupid " + cugId
                                + " errorcode " + resultAddMember);
                    } else {
                        logger.info("RemoveMember " + msisdn + ", kit batch id " + kitBatchId + ", cug name " + cugName + " result " + resultAddMember);
                    }
                }
                //Delete price plan call internal group.
                String modiSub = "";
                int removeSub = 0;
                String pricePlanCallInternal = "11205034";
                SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Calendar cal = Calendar.getInstance();
                cal.setTime(new Date());
                cal.add(Calendar.DATE, -1);
                String expireDate = sdf1.format(cal.getTime());
                logger.info("Remove pricePlanCallInternal, put expireTime < sysdate when using OCSHW_MODISUBPRODUCT, isdn: " + msisdn + ", expireDate: " + expireDate);
                modiSub = exch.modiSubProduct(msisdn.startsWith("258") ? msisdn : "258" + msisdn, pricePlanCallInternal, "", "", expireDate);
                logger.info("Remove pricePlanCallInternal, put expireTime < sysdate when using OCSHW_MODISUBPRODUCT, isdn: " + msisdn + ", expireDate: " + expireDate
                        + " return result code " + modiSub);

                if ("0".equals(modiSub) || "WS_PACKAGE_NOT_FOUND".equals(modiSub)) {
                    //Delete from group on BCCS
                    removeSub = dbPre.removeSubFromGroup(msisdn, tmpSub.getKitBatchId(), processUser);
                    logger.info("removeSubFromGroup isdn: " + msisdn + ", kitBatchId: " + kitBatchId
                            + " return result code " + removeSub);
                    //Save audit history log
                    int actionAuditId = dbPre.getActionAuditSeq();
                    if (0 == removeSub) {
                        dbPre.insertActionAudit(actionAuditId, msisdn, "Remove member " + msisdn + " from kitBatchId " + kitBatchId + " success.", processUser, ip);
                        dbPre.insertKitBatchRebuildHis(msisdn, kitBatchId, actionAuditId, processUser);
                        response.setErrorCode(Common.ErrCode.SUCCESS);
                        response.setDescription("SUCCESS");

//               Insert groupManagementLog
                        int insertLog = dbPre.insertGroupManagementLog(0L, kitBatchId.intValue(), msisdn, "",
                                "", 1, 3, processUser, 0, transId, 0d, 0d, "Remove package success", "0", "");
                        if (insertLog == 0) {
                            logger.info("Remove package insert group_management_log success: isdn " + msisdn + ", kitBatchId " + kitBatchId
                                    + " result " + insertLog);
                        } else {
                            logger.warn("Remove package insert group_management_log failed: isdn " + msisdn + ", kitBatchId " + kitBatchId
                                    + " result " + insertLog);
                        }
                    } else {
                        dbPre.insertActionAudit(actionAuditId, msisdn, "Remove member " + msisdn + " from kitBatchId " + kitBatchId + " error.", processUser, ip);
                        dbPre.insertKitBatchRebuildHis(msisdn, kitBatchId, actionAuditId, processUser);
                        response.setErrorCode(Common.ErrCode.FAIL);
                        response.setDescription("Can not remove sub from group.");
                    }

                } else {
                    response.setErrorCode(Common.ErrCode.FAIL);
                    response.setDescription("Can not remove price plan call internal.");
                }
            }
            return response;
        } catch (Exception e) {
            logger.error("[!!!] Error RemoveMember for sub " + dataInfo, e);
            logger.error(AppManager.logException(timeStart, e));
            response.setErrorCode(Common.ErrCode.FAIL);
            response.setDescription("ERROR");
            return response;
        }
    }

    @WebMethod(operationName = "ChangePackage")
    public ResponseGroupMgt ChangePackage(
            @WebParam(name = "isdn", targetNamespace = "") String msisdn,
            @WebParam(name = "packageCode", targetNamespace = "") String packageCode,
            @WebParam(name = "prepaidMonth", targetNamespace = "") Integer prepaidMonth,
            @WebParam(name = "kitBatchId", targetNamespace = "") Integer kitBatchId,
            @WebParam(name = "enterpriseWallet", targetNamespace = "") String enterpriseWallet,
            @WebParam(name = "processUser", targetNamespace = "") String processUser,
            @WebParam(name = "isConfirm", targetNamespace = "") boolean isConfirm,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword) {
        long timeStart = System.currentTimeMillis();
        ResponseGroupMgt response = new ResponseGroupMgt();
        String paramMo = "";
        String lastEffectiveTime = "";
        String changePckActionType = Config.getConfig(Config.actionTypeChangePackage, logger);
        String changePckChannel = Config.getConfig(Config.channelChangePackage, logger);
        String changePckChannelType = Config.getConfig(Config.channelTypeChangePackage, logger);
        String enterpriseProductCode = Config.getConfig(Config.enterpriseProductCode, logger);
        List<String> enterpriseProductCodes = Arrays.asList(enterpriseProductCode.split("\\|"));
        String changeEliteBonusPaidMonth = Config.getConfig(Config.changeEliteBonusPaidMonth, logger);
        String[] arrChangeEliteBonusPaidMonth = changeEliteBonusPaidMonth.split("\\|");
        ProductConnectKit newProductInfo;
        ProductConnectKit oldProductInfo;
        long totalMoney = 0;
        long bonusForPaidMonth = 0;
        long discountForUpgradePackage = 0;
        int validHours = 48;
        try {
            logger.info("Start process ConfirmChangePackage for sub " + msisdn + " client " + wsuser);
//        step 1: validate input
            if (msisdn == null || "".equals(msisdn.trim())
                    || wsuser == null || "".equals(wsuser.trim())
                    || wspassword == null || "".equals(wspassword.trim())
                    || msisdn.length() > 99) {
                logger.warn("Invalid input sub " + msisdn + " length " + (msisdn == null ? 0 : msisdn.length())
                        + " wsuser " + wsuser + " length " + (wsuser == null ? 0 : wsuser.length())
                        + " wspassword " + wspassword + " length " + (wspassword == null ? 0 : wspassword.length()));
                response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
                response.setDescription("INVALID_INPUT");
                return response;
            }
//        step 2: validate ip
            String ip = getIpClient();
            if (ip == null || "".equals(ip.trim())) {
                logger.warn("Can not get ip for sub " + msisdn);
                response.setErrorCode(Vas.ResultCode.FAIL_GET_IP);
                response.setDescription("FAIL_GET_IP");
                return response;
            }
            UserInfo user = authenticate(db, wsuser, wspassword, ip);
            if (user == null || user.getId() < 0) {
                logger.warn("Invalid account " + msisdn);
                response.setErrorCode(Vas.ResultCode.WRONG_ACCOUNT_IP);
                response.setDescription("WRONG_ACCOUNT_IP");
                return response;
            }

            if (msisdn.startsWith(Common.config.countryCode)) {
                msisdn = msisdn.substring(Common.config.countryCode.length());
            }
            logger.info("Check prepaid sub " + msisdn);
            Subscriber subscriber = dbPre.getSubInfoMobile(msisdn, false);
            if (subscriber == null || subscriber.getSubId() == null || "".equals(subscriber.getSubId())) {
                //ko tim thay tren CM PRE, thuc hien tim tren CM POST
                logger.info("Not pre, check postpaid sub " + msisdn);
                subscriber = dbPost.getSubInfoMobile(msisdn, false);
                if (subscriber == null || subscriber.getSubId() == null || "".equals(subscriber.getSubId())) {
                    logger.info("Not postpaid mobile, check homephone pre sub " + msisdn);
                    subscriber = dbPre.getSubInfoHomephone(msisdn, false);
                    if (subscriber == null || subscriber.getSubId() == null || "".equals(subscriber.getSubId())) {
                        logger.info("Not prepaid homephone, check postpaid homephone " + msisdn);
                        subscriber = dbPost.getSubInfoHomephone(msisdn, false);
                    }
                }
            }
            //loi he thong CM
            if (subscriber == null) {
                logger.info("Not Movitel sub " + msisdn);
                response.setErrorCode(22);
                response.setDescription("SUBSCRIBER_INVALID");
                response.setExtraInfo(msisdn);
                return response;
            }

            //neu khong co thong tin 
            if (subscriber.getSubId() == null || "".equals(subscriber.getSubId().trim())) {
                logger.info("Can not get sub info " + msisdn);
                response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
                response.setDescription("SUBSCRIBER_INVALID");
                response.setExtraInfo(msisdn);
                return response;
            }

            //Check SIM chua kich hoat
            if (!"00".equals(subscriber.getActStatus())) {
                logger.info("Can not get sub info " + msisdn);
                response.setErrorCode(26);
                response.setDescription("SUB_INACTIVE");
                response.setExtraInfo(msisdn);
                return response;
            }

            //Check new product duplicate the same old product code
            if (subscriber.getProductCode().toLowerCase().equals(packageCode.toLowerCase())) {
                logger.info("Request change package sub " + msisdn + ", new package " + packageCode + " the same with old productCode " + subscriber.getProductCode());
                response.setErrorCode(10);
                response.setDescription("PRODUCT_CODE_DUPLICATE");
                response.setExtraInfo(subscriber.getIsdn());
                return response;
            }

            newProductInfo = dbPre.getProductInfo(msisdn, packageCode);
            if (newProductInfo == null || newProductInfo.getMoneyFee() == null || newProductInfo.getPricePlan() == null
                    || newProductInfo.getOfferId() <= 0) {
                logger.warn("New package is not Elite produdct " + msisdn + " " + packageCode);
                response.setErrorCode(18);
                response.setDescription("NEW_PACKAGE_NOT_ELITE_PRODUCT");
                response.setExtraInfo(msisdn);
                return response;
            }
            oldProductInfo = dbPre.getProductInfo(msisdn, subscriber.getProductCode());
            if (oldProductInfo == null) {
                logger.warn("Can not get produdct " + msisdn + " " + subscriber.getProductCode());
                response.setErrorCode(18);
                response.setDescription("CAN_NOT_GET_PRODUCT");
                response.setExtraInfo(msisdn);
                return response;
            }

            //Check prepaidMonth between 1 and 24.
            if (prepaidMonth <= 0 || prepaidMonth > 24) {
                logger.info("Request change package sub " + msisdn + ", new package " + packageCode
                        + " prepaid month " + prepaidMonth + " not between 1 and 24");
                response.setErrorCode(17);
                response.setDescription("PREPAID_MONTH_INVALID");
                return response;
            }

            //Check if Elite Enterprise package
            if (enterpriseProductCodes.contains(packageCode.toUpperCase())) {
                //Check prepaid month must be greater than 12 and total sub Elite Enterprise must be greater 100 (both old kitBatch)
                ResponseGroupMgt tmpResponse = checkEnterpriseCondition(kitBatchId, prepaidMonth, enterpriseWallet, 1);
                if (tmpResponse.getErrorCode() != Common.ErrCode.SUCCESS) {
                    return tmpResponse;
                }
            }

            //Check enterprise account
            QueryEwalletInfo enterpriseInfo = EWalletUtil.queryCustomerInfo(enterpriseWallet, logger);
            if (!"1".equals(enterpriseInfo.getStatus())) {
                logger.info("Request change package sub " + msisdn + ", new package " + packageCode + " enterprise wallet not active " + enterpriseWallet);
                response.setErrorCode(13);
                response.setDescription("ENTERPRISE_EWALLET_NOT_ACTIVE");
                return response;
            } else if (!"CO".equals(enterpriseInfo.getChannelCode())) {
                logger.info("Request change package sub " + msisdn + ", new package " + packageCode + " wallet type is not enterprise " + enterpriseWallet);
                response.setErrorCode(14);
                response.setDescription("EWALLET_NOT_BE_ENTERPRISE");
                return response;
            } else if (enterpriseInfo.getProviderCode() == null || enterpriseInfo.getProviderCode().isEmpty()) {
                logger.info("Request change package sub " + msisdn + ", new package " + packageCode + " enterprise wallet not have provider code  " + enterpriseWallet);
                response.setErrorCode(15);
                response.setDescription("ENTERPRISE_EWALLET_NOT_HAVE_PROVIDERCODE");
                return response;
            }

            //Get discount for change package (15, 17, 20%)
            if (!enterpriseProductCodes.contains(packageCode.toUpperCase())) {
                float percentBonus = dbPre.calculatePercentBonus(arrChangeEliteBonusPaidMonth, prepaidMonth, msisdn);
                bonusForPaidMonth = Math.round(percentBonus * Long.valueOf(newProductInfo.getMoneyFee()) * prepaidMonth);
                logger.info("Subscriber " + msisdn + " have paidMonth: " + prepaidMonth + ", value discount after round: " + bonusForPaidMonth);
            }
            if (db.checkElitePackage(msisdn)) {

                HashMap<String, String> lstParams = new HashMap<String, String>();
                lstParams.put("MSISDN", msisdn.startsWith("258") ? msisdn : "258" + msisdn);
                String original = exch.getOriginalOfCommand(msisdn.startsWith("258") ? msisdn : "258" + msisdn, "OCSHW_INTEGRATIONENQUIRY", lstParams);
                if (!original.isEmpty()) {
                    if (original.contains("<Name>Debit</Name>")) {
                        if (!original.contains("<Name>Debit</Name><LongValue>0</LongValue>")) {
                            logger.info("Request change package sub " + msisdn + ", new package " + packageCode + " sub still have debit ");
                            response.setErrorCode(24);
                            response.setDescription("SUB_HAVE_DEBIT");
                            return response;
                        } else {
                            logger.info("subscriber don't have debit, isdn: " + msisdn);
                        }
                    }
                    logger.info("subscriber don't have debit, isdn: " + msisdn);
                    List<Offer> listOffer = exch.parseListOffer(original);
                    if (listOffer != null && !listOffer.isEmpty()) {
                        for (Offer offer : listOffer) {
                            if (offer.getName() != null && offer.getName().toUpperCase().contains("_ADDON") && "1".equals(offer.getState())
                                    && offer.getName().toUpperCase().contains(subscriber.getProductCode().toUpperCase())) {
                                SimpleDateFormat tmpSdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                                if (offer.getRecurringDate() != null && offer.getRecurringDate().length() > 0) {
                                    Date recurringDate = tmpSdf.parse(offer.getRecurringDate());
                                    lastEffectiveTime = sdf.format(recurringDate);
                                    logger.info("LastEffectiveTime of subscriber on vOCS3: " + lastEffectiveTime + ", msisdn: " + msisdn);
                                }
                                break;
                            }
                        }
                    }
                } else {
                    logger.warn("Cannot get original of subscriber " + msisdn);
                }
                if (!checkRecurring48h(lastEffectiveTime, validHours)
                        || (db.countChangeReg48h(msisdn, subscriber.getProductCode()) != 0)
                        || !db.checkSubChangeBuymore48h(enterpriseWallet, msisdn)) {
                    lastEffectiveTime = "";
                    logger.info("RecurringDate not in 48hours or exists kit_vas tables or exists in mo_his with action_type in (2,99), msisdn: " + msisdn);
                } else {
                    logger.info("RecurringDate in 48hours and not exists kit_vas tables and not exists in mo_his with action_type in (2,99), msisdn: " + enterpriseWallet);
                }
                if (lastEffectiveTime.length() > 0
                        && (Long.valueOf(newProductInfo.getMoneyFee()) > Long.valueOf(oldProductInfo.getMoneyFee()))) {
                    logger.info("Change during 48h after extending so save oldProductMoneyFee " + oldProductInfo.getMoneyFee() + " " + enterpriseWallet);
                    discountForUpgradePackage = Long.valueOf(oldProductInfo.getMoneyFee());
                }
            }
            if (Long.valueOf(newProductInfo.getMoneyFee()) * prepaidMonth < bonusForPaidMonth + discountForUpgradePackage) {
                discountForUpgradePackage = 0;
                logger.info("Total paid less than discount " + (Long.valueOf(newProductInfo.getMoneyFee()) * prepaidMonth) + ", discount " + (bonusForPaidMonth + discountForUpgradePackage) + ", set discountForUpgradePackage = 0");
            }

            totalMoney = Long.valueOf(newProductInfo.getMoneyFee()) * prepaidMonth - bonusForPaidMonth - discountForUpgradePackage;

            if (enterpriseInfo.getBalance() != null && enterpriseInfo.getBalance() < totalMoney) {
                logger.info("Confirm change package sub " + msisdn + ", new package " + packageCode
                        + " not enough money " + enterpriseWallet + ", balance " + enterpriseInfo.getBalance() + ", totalMoney " + totalMoney);
                response.setErrorCode(Vas.ResultCode.NOT_ENOUGH_MONEY);
                response.setDescription("NOT_ENOUGH_MONEY");
                return response;
            }

            if (!isConfirm) {
                logger.info("Request change package sub " + msisdn + ", new package " + packageCode + " enterprise wallet " + enterpriseWallet + ", total paid: " + totalMoney + ", total discount: " + (bonusForPaidMonth + discountForUpgradePackage));
                response.setErrorCode(Common.ErrCode.SUCCESS);
                response.setDescription("SUCCESS");
                response.setExtraInfo("" + totalMoney + "|" + (bonusForPaidMonth + discountForUpgradePackage));
                return response;
            }

            long moId = db.getSequence("MO_SEQ", "dbvas");
            String transId = sdf.format(new Date()) + msisdn;
            logger.info("Request change package sub " + msisdn + ", new package " + packageCode + " enterprise wallet " + enterpriseWallet + " start insert mo ");
            //Insert MO
            //pt|2-buy for other|des sub|prepaid month|3-pay by emola
            paramMo = "pt|2|" + msisdn + "|" + prepaidMonth + "|3";
            int insertMo = db.insertMoV2(moId, enterpriseWallet, packageCode, paramMo, changePckActionType, changePckChannel, changePckChannelType);

            if (insertMo == 0) {
                logger.info("Request change package, insert mo success: sub " + msisdn + ", new package " + packageCode + " enterprise wallet " + enterpriseWallet + " result " + insertMo);
            } else {
                logger.info("Request change package, insert mo error: sub " + msisdn + ", new package " + packageCode + " enterprise wallet " + enterpriseWallet + " result " + insertMo);
            }

//            Insert groupManagementLog
            int insertLog = dbPre.insertGroupManagementLog(moId, kitBatchId, msisdn, packageCode,
                    enterpriseWallet, 0, 1, processUser, prepaidMonth, transId, (Double.valueOf(newProductInfo.getMoneyFee()) * prepaidMonth),
                    (double) (bonusForPaidMonth + discountForUpgradePackage), "Insert sub to mo change package", "", "");
            if (insertLog == 0) {
                logger.info("ConfirmChangePackage insert group_management_log success: isdn " + msisdn + ", product " + packageCode + ", prepaidMonth " + prepaidMonth
                        + ", total paid " + (Double.valueOf(newProductInfo.getMoneyFee()) * prepaidMonth)
                        + ", discount " + (bonusForPaidMonth + discountForUpgradePackage) + " result " + insertLog);
            } else {
                logger.warn("ConfirmChangePackage insert group_management_log failed: isdn " + msisdn + ", product " + packageCode + ", prepaidMonth " + prepaidMonth
                        + ", total paid " + (Double.valueOf(newProductInfo.getMoneyFee()) * prepaidMonth)
                        + ", discount " + (bonusForPaidMonth + discountForUpgradePackage) + " result " + insertLog);
            }

            logger.info("Confirm change package sub " + msisdn + ", new package " + packageCode + " enterprise wallet " + enterpriseWallet + " insert mo " + insertMo);
            response.setErrorCode(Common.ErrCode.SUCCESS);
            response.setDescription("SUCCESS");
            return response;
        } catch (Exception e) {
            logger.error("[!!!] Error ConfirmChangePackage for sub " + msisdn, e);
            logger.error(AppManager.logException(timeStart, e));
            response.setErrorCode(Common.ErrCode.FAIL);
            response.setDescription("ERROR");
            return response;
        }
    }

    @WebMethod(operationName = "ViewAccountInfo")
    public ResponseGroupMgt ViewAccountInfo(
            @WebParam(name = "isdn", targetNamespace = "") String msisdn,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword) {
        long timeStart = System.currentTimeMillis();
        ResponseGroupMgt response = new ResponseGroupMgt();
        try {
            logger.info("Start process ViewAccountInfo for sub " + msisdn + " client " + wsuser);
//        step 1: validate input
            if (msisdn == null || "".equals(msisdn.trim())
                    || wsuser == null || "".equals(wsuser.trim())
                    || wspassword == null || "".equals(wspassword.trim())
                    || msisdn.length() > 99) {
                logger.warn("Invalid input sub " + msisdn + " length " + (msisdn == null ? 0 : msisdn.length())
                        + " wsuser " + wsuser + " length " + (wsuser == null ? 0 : wsuser.length())
                        + " wspassword " + wspassword + " length " + (wspassword == null ? 0 : wspassword.length()));
                response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
                response.setDescription("INVALID_INPUT");
                return response;
            }
//        step 2: validate ip
            String ip = getIpClient();
            if (ip == null || "".equals(ip.trim())) {
                logger.warn("Can not get ip for sub " + msisdn);
                response.setErrorCode(Vas.ResultCode.FAIL_GET_IP);
                response.setDescription("FAIL_GET_IP");
                return response;
            }
            UserInfo user = authenticate(db, wsuser, wspassword, ip);
            if (user == null || user.getId() < 0) {
                logger.warn("Invalid account " + msisdn);
                response.setErrorCode(Vas.ResultCode.WRONG_ACCOUNT_IP);
                response.setDescription("WRONG_ACCOUNT_IP");
                return response;
            }

            if (msisdn.startsWith(Common.config.countryCode)) {
                msisdn = msisdn.substring(Common.config.countryCode.length());
            }
            logger.info("Check prepaid sub " + msisdn);
            Subscriber subscriber = dbPre.getSubInfoMobile(msisdn, false);
            if (subscriber == null || subscriber.getSubId() == null || "".equals(subscriber.getSubId())) {
                //ko tim thay tren CM PRE, thuc hien tim tren CM POST
                logger.info("Not pre, check postpaid sub " + msisdn);
                subscriber = dbPost.getSubInfoMobile(msisdn, false);
                if (subscriber == null || subscriber.getSubId() == null || "".equals(subscriber.getSubId())) {
                    logger.info("Not postpaid mobile, check homephone pre sub " + msisdn);
                    subscriber = dbPre.getSubInfoHomephone(msisdn, false);
                    if (subscriber == null || subscriber.getSubId() == null || "".equals(subscriber.getSubId())) {
                        logger.info("Not prepaid homephone, check postpaid homephone " + msisdn);
                        subscriber = dbPost.getSubInfoHomephone(msisdn, false);
                    }
                }
            }
            //loi he thong CM
            if (subscriber == null) {
                logger.info("Not Movitel sub " + msisdn);
                response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
                response.setDescription("SUBSCRIBER_INVALID");
                return response;
            }

            //neu khong co thong tin 
            if (subscriber.getSubId() == null || "".equals(subscriber.getSubId().trim())) {
                logger.info("Can not get sub info " + msisdn);
                response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
                response.setDescription("SUBSCRIBER_INVALID");
                return response;
            }

            //Check product code
            String pricePlan = db.getPricePlanOfProduct(subscriber.getProductCode());
            if (!msisdn.startsWith(Common.config.countryCode)) {
                msisdn = "258" + msisdn;
            }
            logger.info("PricePlan sub " + msisdn + " - " + pricePlan);
            String recurringDate = "";
            HashMap<String, String> lstParams = new HashMap<String, String>();
            lstParams.put("MSISDN", msisdn.startsWith("258") ? msisdn : "258" + msisdn);
            String original = exch.getOriginalOfCommand(msisdn.startsWith("258") ? msisdn : "258" + msisdn, "OCSHW_INTEGRATIONENQUIRY", lstParams);
            logger.info("original sub " + msisdn + " - original " + original);
            List<Offer> listOffer = exch.parseListOffer(original);
            if (listOffer != null && !listOffer.isEmpty()) {
                for (Offer offer : listOffer) {
                    if (offer.getId().equals(pricePlan)) {
                        recurringDate = offer.getRecurringDate();
                        break;
                    }
                }
            }
            response.setExtraInfo(recurringDate);
            response.setErrorCode(Common.ErrCode.SUCCESS);
            response.setDescription("SUCCESS");
            return response;
        } catch (Exception e) {
            logger.error("[!!!] Error ViewAccountInfo for sub " + msisdn, e);
            logger.error(AppManager.logException(timeStart, e));
            response.setErrorCode(Common.ErrCode.FAIL);
            response.setDescription("ERROR");
            return response;
        }
    }

    @WebMethod(operationName = "RenewPackage")
    public ResponseGroupMgt RenewPackage(
            @WebParam(name = "dataInfo", targetNamespace = "") String dataInfo,
            @WebParam(name = "prepaidMonth", targetNamespace = "") Integer prepaidMonth,
            @WebParam(name = "kitBatchId", targetNamespace = "") Integer kitBatchId,
            @WebParam(name = "enterpriseWallet", targetNamespace = "") String enterpriseWallet,
            @WebParam(name = "processUser", targetNamespace = "") String processUser,
            @WebParam(name = "isConfirm", targetNamespace = "") boolean isConfirm,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword) {
        long timeStart = System.currentTimeMillis();
        ResponseGroupMgt response = new ResponseGroupMgt();
        int totalSuccess = 0;
        int totalFailed = 0;
        String shopCode = "";
        double totalMoneyProduct = 0;
        double totalDiscount = 0;
        String staffCode = Config.getConfig(Config.staffCodeMakeSaleTrans, logger);
        String discountRateRenewForPaidMonth = Config.getConfig(Config.discountRenewForPaidMonth, logger);
        String[] arrDiscountRateRenewForPaidMonth = discountRateRenewForPaidMonth.split("\\|");
        String renewGroupElitePricePlan = Config.getConfig(Config.renewGroupElitePricePlan, logger);
        String[] arrRenewGroupElitePricePlan = renewGroupElitePricePlan.split("\\|");
        String enterpriseProductCode = Config.getConfig(Config.enterpriseProductCode, logger);
        List<String> enterpriseProductCodes = Arrays.asList(enterpriseProductCode.split("\\|"));
        ResponseWallet responseWallet;
        ProductConnectKit tmpProductInfo = null;
        String orgRequestId = "";
        try {
            logger.info("Start process RequestRenewPackage for client " + wsuser);
//        step 1: validate input
            if (wsuser == null || "".equals(wsuser.trim())
                    || wspassword == null || "".equals(wspassword.trim())) {
                logger.warn("Invalid input  wsuser " + wsuser + " length " + (wsuser == null ? 0 : wsuser.length())
                        + " wspassword " + wspassword + " length " + (wspassword == null ? 0 : wspassword.length()));
                response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
                response.setDescription("INVALID_INPUT");
                return response;
            }
//        step 2: validate ip
            String ip = getIpClient();
            if (ip == null || "".equals(ip.trim())) {
                logger.warn("Can not get ip ");
                response.setErrorCode(Vas.ResultCode.FAIL_GET_IP);
                response.setDescription("FAIL_GET_IP");
                return response;
            }
            UserInfo user = authenticate(db, wsuser, wspassword, ip);
            if (user == null || user.getId() < 0) {
                logger.warn("Invalid account ");
                response.setErrorCode(Vas.ResultCode.WRONG_ACCOUNT_IP);
                response.setDescription("WRONG_ACCOUNT_IP");
                return response;
            }
            Gson gson = new Gson();
            Type listType = new TypeToken<ArrayList<KitBatchDetailModel>>() {
            }.getType();
            List<KitBatchDetailModel> listSubs = gson.fromJson(dataInfo, listType);

//            //Check prepaidMonth between 1 and 24.
            if (prepaidMonth <= 0 || prepaidMonth > 24) {
                logger.info("Prepaid month invalid " + prepaidMonth + " not between 1 and 24");
                response.setErrorCode(17);
                response.setDescription("PREPAID_MONTH_INVALID");
                return response;
            }

            //Check enterprise account
            QueryEwalletInfo enterpriseInfo = EWalletUtil.queryCustomerInfo(enterpriseWallet, logger);
            if (!"1".equals(enterpriseInfo.getStatus())) {
                logger.info("Request renew package enterprise wallet not active " + enterpriseWallet + " " + enterpriseInfo.getStatus());
                response.setErrorCode(13);
                response.setDescription("ENTERPRISE_EWALLET_NOT_ACTIVE");
                return response;
            } else if (!"CO".equals(enterpriseInfo.getChannelCode())) {
                logger.info("Request change package wallet type is not enterprise " + enterpriseWallet);
                response.setErrorCode(14);
                response.setDescription("EWALLET_NOT_BE_ENTERPRISE");
                return response;
            } else if (enterpriseInfo.getProviderCode() == null || enterpriseInfo.getProviderCode().isEmpty()) {
                logger.info("Request change package enterprise wallet not have provider code  " + enterpriseWallet);
                response.setErrorCode(15);
                response.setDescription("ENTERPRISE_EWALLET_NOT_HAVE_PROVIDERCODE");
                return response;
            }

            if (listSubs == null || listSubs.isEmpty()) {
                logger.warn("List sub is null or empty " + dataInfo);
                response.setErrorCode(27);
                response.setDescription("DONT_HAVE_ANY_SUB_VALID");
                return response;
            }

            List<HashMap> lstRenewGroupElitePricePlan = new ArrayList<HashMap>();
            for (String tmp : arrRenewGroupElitePricePlan) {
                String[] arrTmp = tmp.split("\\:");
                HashMap map = new HashMap();
                map.put(arrTmp[0].trim().toUpperCase(), arrTmp[1].trim());
                lstRenewGroupElitePricePlan.add(map);
            }
            
            float percentBonus = 0;
            double discountForPrepaid = 0;
            for (KitBatchDetailModel tmpSub : listSubs) {
                percentBonus = 0;
                discountForPrepaid = 0;
                String pricePlanCode = "";
                String productCode = db.getProductCodeByIsdn(tmpSub.getIsdn());
                if (productCode == null || productCode.trim().length() <= 0) {
                    logger.info("productCode is empty, cannot connect kit for sim: isdn: " + tmpSub.getIsdn());
                    response.setErrorCode(22);
                    response.setDescription("SUB_INVALID");
                    return response;
                }
                tmpSub.setProductCode(productCode);
                for (int i = 0; i < lstRenewGroupElitePricePlan.size(); i++) {
                    if (lstRenewGroupElitePricePlan.get(i).containsKey(tmpSub.getProductCode().toUpperCase())) {
                        pricePlanCode = lstRenewGroupElitePricePlan.get(i).get(tmpSub.getProductCode().toUpperCase()).toString();
                        break;
                    }
                }
                if (pricePlanCode.isEmpty()) {
                    logger.warn("Current product is not Elite produdct " + tmpSub.getIsdn() + " " + tmpProductInfo);
                    response.setErrorCode(18);
                    response.setDescription("CURRENT_SUB_NOT_ELITE_PRODUCT");
                    response.setExtraInfo(tmpSub.getIsdn());
                    return response;
                }
                tmpProductInfo = dbPre.getProductInfo(tmpSub.getIsdn(), productCode);
                if (tmpProductInfo == null || tmpProductInfo.getMoneyFee() == null || tmpProductInfo.getPricePlan() == null
                        || tmpProductInfo.getOfferId() <= 0) {
                    logger.warn("Current product is not Elite produdct " + tmpSub.getIsdn() + " " + tmpProductInfo);
                    response.setErrorCode(18);
                    response.setDescription("CURRENT_SUB_NOT_ELITE_PRODUCT");
                    return response;
                }
                logger.info("enterpriseProductCodes: " + enterpriseProductCodes + " - " + tmpProductInfo.toString());
                if (!enterpriseProductCodes.contains(tmpProductInfo.getProductCode().toUpperCase())) {
                    percentBonus = dbPre.calculatePercentBonus(arrDiscountRateRenewForPaidMonth, prepaidMonth, tmpSub.getIsdn());
                    discountForPrepaid = Math.round(percentBonus * Long.valueOf(tmpProductInfo.getMoneyFee()) * prepaidMonth);
                    logger.info("Subscriber " + tmpSub.getIsdn() + " have paidMonth: " + prepaidMonth + ", value discount after round: " + discountForPrepaid);
                }

                tmpSub.setMoneyProduct(Double.valueOf(tmpProductInfo.getMoneyFee()));
                totalDiscount += discountForPrepaid;
                totalMoneyProduct += Double.valueOf(tmpProductInfo.getMoneyFee()) * prepaidMonth;
            }

            if (enterpriseInfo.getBalance() >= Math.round(totalMoneyProduct - totalDiscount)) {
                if (!isConfirm) {
                    response.setErrorCode(Common.ErrCode.SUCCESS);
                    response.setDescription("SUCCESS");
                    response.setExtraInfo("" + Math.round(totalMoneyProduct - totalDiscount) + "|" + totalDiscount + "|" + listSubs.size() + "|" + totalSuccess + "|" + totalFailed);
                    return response;
                }
                //Step payment
                responseWallet = EWalletUtil.chargeEmolaEnterpriseV2(sdf.format(new Date()) + kitBatchId, db,
                        enterpriseWallet, (totalMoneyProduct - totalDiscount) * 1.00, logger, enterpriseWallet, staffCode, enterpriseInfo.getProviderCode(), enterpriseInfo.getCustomerCode()
                        , "Deduct emola when renew kit batch id " + kitBatchId);
                if (responseWallet == null || !"01".equals(responseWallet.getResponseCode())) {
                    logger.info("Can not debit enterprise: "
                            + ", enterpriseWallet: " + enterpriseWallet + ", staff: " + processUser + " result " + responseWallet);
                    response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
                    response.setDescription("CAN_NOT_DEBIT_ENTERPRISE");
                    return response;
                }
                orgRequestId = responseWallet.getRequestId();//RequestId using when revertTransaction
                for (KitBatchDetailModel tmpSub : listSubs) {
                    //Not exist >> create new...base on create time of group...
                    if (db.checkKitElitePrepaid(tmpSub.getIsdn(), tmpSub.getProductCode())) {
                        //exist record on table...
                        String tmpProductCode = db.getProductCodeKitElitePrepaid(tmpSub.getIsdn(), tmpSub.getProductCode());
                        if (tmpSub.getProductCode().toUpperCase().equals(tmpProductCode.toUpperCase())) {
                            logger.info("current productCode is the same productCode in batch, paidMonth: " + prepaidMonth + " id: "
                                    + kitBatchId + ", staffCode: " + processUser);
                            int rsUpdate = db.updateKitElitePrepaid(tmpSub.getIsdn(), "", prepaidMonth, false, tmpSub.getProductCode());
                            if (rsUpdate == 1) {
                                logger.info("update kit_elite_prepaid successfully, paidMonth: " + prepaidMonth + ", id: "
                                        + kitBatchId + ", staffCode: " + processUser);
                            } else {
                                logger.info("update kit_elite_prepaid unsuccessfully, paidMonth: " + prepaidMonth + " id: "
                                        + kitBatchId + ", staffCode: " + processUser);
                            }
                        } else {
                            logger.info("current productCode isn't the same productCode in batch, paidMonth: " + prepaidMonth + " id: "
                                    + kitBatchId + ", staffCode: " + processUser);
                            int rsPrepaid = db.insertKitElitePrepaid(tmpSub.getIsdn(), prepaidMonth, 0, processUser, tmpSub.getProductCode(), "", false);
                            if (rsPrepaid == 1) {
                                logger.info("insert kit_elite_prepaid successfully, paidMonth: " + prepaidMonth + ",id: "
                                        + kitBatchId + ", staffCode: " + processUser);
                            } else {
                                logger.info("insert kit_elite_prepaid unsuccessfully, paidMonth: " + prepaidMonth + ", id: "
                                        + kitBatchId + ", staffCode: " + processUser);
                            }
                        }

                    } else {
                        int rsPrepaid = db.insertKitElitePrepaid(tmpSub.getIsdn(), prepaidMonth, 0, processUser, tmpSub.getProductCode(), "", false);
                        if (rsPrepaid == 1) {
                            logger.info("insert kit_elite_prepaid successfully, paidMonth: " + prepaidMonth + ", id: "
                                    + kitBatchId + ", staffCode: " + processUser);
                        } else {
                            logger.info("insert kit_elite_prepaid unsuccessfully, paidMonth: " + prepaidMonth + ", id: "
                                    + kitBatchId + ", staffCode: " + processUser);
                        }
                    }
                    long actionAuditId = db.getSequence("seq_action_audit", "cm_pre");
                    db.insertActionAuditCmPre(actionAuditId, "555", 0, shopCode, processUser, db.getSubId(tmpSub.getIsdn()), ip, "Renew Group Elite for kitBatchId: " + kitBatchId);
                    db.insertKitRenewGroupHis((long) kitBatchId, tmpSub.getIsdn(), tmpSub.getProductCode(), processUser, "",
                            "", "", tmpSub.getMoneyProduct(),
                            totalMoneyProduct, "", "", prepaidMonth, totalDiscount, "" + totalDiscount);
                }

                //                Step 3.5: Make saleTrans
                String lstIsdnReceiveError = Config.getConfig(Config.receiveMessageList, logger);
                String[] arrIsdnReceiveError = lstIsdnReceiveError.split("\\|");
                Long saleTransId = db.getSequence("SALE_TRANS_SEQ", "dbsm");
                String strTempId = db.getShopIdStaffIdByStaffCode(staffCode);
                String[] arrTempId = strTempId.split("\\|");
                long reasonId = 0L;
                int rsMakeSaleTrans = db.insertSaleTrans(saleTransId, Long.valueOf(arrTempId[0]), Long.valueOf(arrTempId[1]),
                        0L, 0L, totalMoneyProduct - totalDiscount, (long) kitBatchId,
                        enterpriseWallet, reasonId, orgRequestId, 1, "", totalDiscount);

                if (rsMakeSaleTrans != 1) {
                    //Insert fail, insert log, send sms
                    db.insertLogMakeSaleTransFail(saleTransId, "RENEW_GROUP", "RENEW_GROUP_ELITE_" + kitBatchId, Long.valueOf(arrTempId[0]),
                            Long.valueOf(arrTempId[1]), 0L, 0L,
                            0L, 0L, 0L, "SALE_TRANS");
                    for (String isdn : arrIsdnReceiveError) {
                        db.sendSms(isdn, "Make saleTrans fail, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                + saleTransId, "86142");
                    }
                }
                Long saleTransDetailSaleService = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
                int rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, "", "", "",
                        "", "", "", "", "", "RENEW_GROUP_ELITE", "RENEW_GROUP_ELITE",
                        "RENEW_GROUP_ELITE", "RENEW_GROUP_ELITE", "17", "",
                        "", String.valueOf(totalMoneyProduct), totalMoneyProduct, totalDiscount);
                if (rsSaleTransDetailSaleServices != 1) {
                    //Insert fail, insert log, send sms
                    db.insertLogMakeSaleTransFail(saleTransId, "RENEW_GROUP", "RENEW_GROUP_ELITE_" + kitBatchId, Long.valueOf(arrTempId[0]),
                            Long.valueOf(arrTempId[1]), 0L, 0L,
                            0L, saleTransDetailSaleService, 0L, "SALE_TRANS_DETAIL");
                    for (String isdn : arrIsdnReceiveError) {
                        db.sendSms(isdn, "Make saleTransDetail for SaleServices fail, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailSaleService, "86142");
                    }
                }
                
//                Send SMS
                String sms = Config.getConfig(Config.msgRenewBatchSuccess, logger);
                sms = sms.replace("%KITBATCHID%", "" + kitBatchId);
                sms = sms.replace("%TOTALSUB%", "" + listSubs.size());
                sms = sms.replace("%TOTALPRICE%", "" + totalMoneyProduct);
                sms = sms.replace("%TOTALDISCOUNT%", "" + totalDiscount);
                sms = sms.replace("%TOTALPAID%", "" + (totalMoneyProduct - totalDiscount));
                db.sendSms(enterpriseWallet, sms, Config.getConfig(Config.smsChannel, logger));
                
                response.setErrorCode(Common.ErrCode.SUCCESS);
                response.setDescription("SUCCESS");
                response.setExtraInfo("" + Math.round(totalMoneyProduct - totalDiscount) + "|" + totalDiscount + "|" + listSubs.size() + "|" + totalSuccess + "|" + totalFailed);
                return response;
            } else {
                logger.info("RequestRenewPackage total subs " + listSubs.size() + ", total success " + totalSuccess + ", total failed " + totalFailed
                        + ", total amount " + totalMoneyProduct + ", total discount " + totalDiscount);
                response.setErrorCode(Vas.ResultCode.NOT_ENOUGH_MONEY);
                response.setDescription("NOT_ENOUGH_MONEY");
                return response;
            }
        } catch (Exception e) {
            logger.error("[!!!] Error RequestRenewPackage ", e);
            logger.error(AppManager.logException(timeStart, e));
            response.setErrorCode(Common.ErrCode.FAIL);
            response.setDescription("ERROR");
            return response;
        }
    }

    @WebMethod(operationName = "AddMemberPackage")
    public ResponseGroupMgt AddMemberPackage(
            @WebParam(name = "dataInfo", targetNamespace = "") String dataInfo,
            @WebParam(name = "prepaidMonth", targetNamespace = "") Integer prepaidMonth,
            @WebParam(name = "kitBatchId", targetNamespace = "") Integer kitBatchId,
            @WebParam(name = "enterpriseWallet", targetNamespace = "") String enterpriseWallet,
            @WebParam(name = "processUser", targetNamespace = "") String processUser,
            @WebParam(name = "isConfirm", targetNamespace = "") boolean isConfirm,
            @WebParam(name = "userName") String wsuser,
            @WebParam(name = "passWord") String wspassword) {
        long timeStart = System.currentTimeMillis();
        ResponseGroupMgt response = new ResponseGroupMgt();
        ProductConnectKit tmpProductInfo;
        ProductConnectKit oldProductInfo;
        int totalSuccess = 0;
        int totalFailed = 0;
        int cugId = 0;
        int validHours = 48;
        double totalMoneyProduct = 0;
        double totalDiscount = 0;
        double discountForUpgradePackage = 0;
        float percentBonus = 0.0f;
        String paramMo = "";
        String staffCode = processUser;
        String cugName = "";
        String strPercentBonus = "";
        String lastEffectiveTime = "";
        String enterpriseProductCode = Config.getConfig(Config.enterpriseProductCode, logger);
        String addMemberPckActionType = Config.getConfig(Config.actionTypeAddPackage, logger);
        String changePckChannel = Config.getConfig(Config.channelChangePackage, logger);
        String changePckChannelType = Config.getConfig(Config.channelTypeChangePackage, logger);
        String discountRateRenewForPaidMonth = Config.getConfig(Config.discountRenewForPaidMonth, logger);
        String renewGroupElitePricePlan = Config.getConfig(Config.renewGroupElitePricePlan, logger);
        String[] arrDiscountRateRenewForPaidMonth = discountRateRenewForPaidMonth.split("\\|");
        String[] arrRenewGroupElitePricePlan = renewGroupElitePricePlan.split("\\|");
        List<String> enterpriseProductCodes = Arrays.asList(enterpriseProductCode.split("\\|"));
        try {
            logger.info("Start process RequestChangePackage for client " + wsuser);
//        step 1: validate input
            if (wsuser == null || "".equals(wsuser.trim())
                    || wspassword == null || "".equals(wspassword.trim())) {
                logger.warn("Invalid input  wsuser " + wsuser + " length " + (wsuser == null ? 0 : wsuser.length())
                        + " wspassword " + wspassword + " length " + (wspassword == null ? 0 : wspassword.length()));
                response.setErrorCode(Vas.ResultCode.INVALID_INPUT);
                response.setDescription("INVALID_INPUT");
                return response;
            }
//        step 2: validate ip
            String ip = getIpClient();
            if (ip == null || "".equals(ip.trim())) {
                logger.warn("Can not get ip ");
                response.setErrorCode(Vas.ResultCode.FAIL_GET_IP);
                response.setDescription("FAIL_GET_IP");
                return response;
            }
            UserInfo user = authenticate(db, wsuser, wspassword, ip);
            if (user == null || user.getId() < 0) {
                logger.warn("Invalid account ");
                response.setErrorCode(Vas.ResultCode.WRONG_ACCOUNT_IP);
                response.setDescription("WRONG_ACCOUNT_IP");
                return response;
            }

            List<HashMap> lstRenewGroupElitePricePlan = new ArrayList<HashMap>();
            for (String tmp : arrRenewGroupElitePricePlan) {
                String[] arrTmp = tmp.split("\\:");
                HashMap map = new HashMap();
                map.put(arrTmp[0].trim().toUpperCase(), arrTmp[1].trim());
                lstRenewGroupElitePricePlan.add(map);
            }

            Gson gson = new Gson();
            Type listType = new TypeToken<ArrayList<KitBatchDetailModel>>() {
            }.getType();
            List<KitBatchDetailModel> listSubs = gson.fromJson(dataInfo, listType);
            List<KitBatchDetailModel> lstKitBatchValid = new ArrayList<KitBatchDetailModel>();

//            //Check prepaidMonth between 1 and 24.
            if (prepaidMonth < 0 || prepaidMonth > 24) {
                logger.info("Prepaid month invalid " + prepaidMonth + " not between 0 and 24");
                response.setErrorCode(17);
                response.setDescription("PREPAID_MONTH_INVALID");
                return response;
            }

            //Check enterprise account
            QueryEwalletInfo enterpriseInfo = EWalletUtil.queryCustomerInfo(enterpriseWallet, logger);
            if (!"1".equals(enterpriseInfo.getStatus())) {
                logger.info("Request change package enterprise wallet not active " + enterpriseWallet + " " + enterpriseInfo.getStatus());
                response.setErrorCode(13);
                response.setDescription("ENTERPRISE_EWALLET_NOT_ACTIVE");
                return response;
            } else if (!"CO".equals(enterpriseInfo.getChannelCode())) {
                logger.info("Request change package wallet type is not enterprise " + enterpriseWallet);
                response.setErrorCode(14);
                response.setDescription("EWALLET_NOT_BE_ENTERPRISE");
                return response;
            } else if (enterpriseInfo.getProviderCode() == null || enterpriseInfo.getProviderCode().isEmpty()) {
                logger.info("Request change package enterprise wallet not have provider code  " + enterpriseWallet);
                response.setErrorCode(15);
                response.setDescription("ENTERPRISE_EWALLET_NOT_HAVE_PROVIDERCODE");
                return response;
            }

            List<String> lstElite = new ArrayList<String>();
            int totalEnterpriseRequest = 0;
            String pricePlanCode = "";
            for (KitBatchDetailModel kitBatch : listSubs) {
                String productCode = db.getProductCodeByIsdn(kitBatch.getIsdn());
                pricePlanCode = "";
                if (productCode == null || productCode.trim().length() <= 0) {
                    logger.info("productCode is empty, cannot connect kit for sim: isdn: " + kitBatch.getIsdn());
                    response.setErrorCode(22);
                    response.setDescription("SUB_INVALID");
                    response.setExtraInfo(kitBatch.getIsdn());
                    return response;
                }
                if (!kitBatch.getProductCode().toUpperCase().equals(productCode.toUpperCase()) && prepaidMonth == 0) {
                    logger.info("Prepaid month invalid " + prepaidMonth + " not between 0 and 24");
                    response.setErrorCode(17);
                    response.setDescription("PREPAID_MONTH_INVALID");
                    return response;
                }

                kitBatch.setCurrenctProductCode(productCode);
                if (db.checkSubExtendOnCurrentGroup(kitBatch.getIsdn(), Long.valueOf(kitBatchId))
                        || db.checkSubExtendOnCurrentGroup2(kitBatch.getIsdn(), Long.valueOf(kitBatchId))) {
                    logger.info("Isdn exist in extend group  " + kitBatch.getIsdn());
                    response.setErrorCode(19);
                    response.setDescription("EXIST_IN_GROUP");
                    response.setExtraInfo(kitBatch.getIsdn());
                    return response;
                }
//                if (db.checkSubExistCUG(kitBatch.getIsdn()) && db.checkSubAlreadyExtendAnotherGroup(kitBatch.getIsdn())) {
                if (db.checkSubAlreadyExtendAnotherGroup(kitBatch.getIsdn()) || db.checkSubAlreadyExtendAnotherGroup2(kitBatch.getIsdn())) {
                    logger.info("Isdn already extentd another group  " + kitBatch.getIsdn());
                    response.setErrorCode(20);
                    response.setDescription("EXIST_IN_GROUP");
                    response.setExtraInfo(kitBatch.getIsdn());
                    return response;
                }
                tmpProductInfo = dbPre.getProductInfo(kitBatch.getIsdn(), kitBatch.getProductCode());
                if (tmpProductInfo == null || tmpProductInfo.getMoneyFee() == null || tmpProductInfo.getPricePlan() == null
                        || tmpProductInfo.getOfferId() <= 0) {
                    logger.warn("New package is not Elite produdct " + kitBatch.getIsdn() + " " + kitBatch.getProductCode());
                    response.setErrorCode(25);
                    response.setDescription("NEW_PACKAGE_NOT_ELITE_PRODUCT");
                    return response;
                }
                for (int i = 0; i < lstRenewGroupElitePricePlan.size(); i++) {
                    if (lstRenewGroupElitePricePlan.get(i).containsKey(tmpProductInfo.getProductCode().toUpperCase())) {
                        pricePlanCode = lstRenewGroupElitePricePlan.get(i).get(tmpProductInfo.getProductCode().toUpperCase()).toString();
                        break;
                    }
                }
                if (pricePlanCode.isEmpty()) {
                    logger.warn("New package is not Elite produdct " + kitBatch.getIsdn() + " " + kitBatch.getProductCode());
                    response.setErrorCode(18);
                    response.setDescription("CURRENT_SUB_NOT_ELITE_PRODUCT");
                    return response;
                }
                //Check if Elite Enterprise package
                if (enterpriseProductCodes.contains(kitBatch.getProductCode().toUpperCase())) {
                    logger.warn("New package is Enterprise Elite produdct " + kitBatch.getIsdn() + " " + kitBatch.getProductCode());
                    totalEnterpriseRequest++;
                } else {
                    logger.warn("New package is not Enterprise Elite produdct " + kitBatch.getIsdn() + " " + kitBatch.getProductCode());
                }
                if (prepaidMonth == 0) {
                    if ((db.checkElitePackage(kitBatch.getIsdn()) || db.checkBranchPromotionSub(kitBatch.getIsdn()))) {
                        lstElite.add(kitBatch.getIsdn());
                        lstKitBatchValid.add(kitBatch);
                        totalSuccess++;
                    } else {
                        response.setErrorCode(21);
                        response.setDescription("EXIST_IN_GROUP");
                        response.setExtraInfo(kitBatch.getIsdn());
                        return response;
                    }

                } else if (prepaidMonth == 0) {
                } else {
                    lstElite.add(kitBatch.getIsdn());
                    lstKitBatchValid.add(kitBatch);
                    totalSuccess++;
                }
            }

            if (totalEnterpriseRequest > 0) {
                //Check prepaid month must be greater than 12 and total sub Elite Enterprise must be greater 100 (both old kitBatch)
                ResponseGroupMgt tmpResponse = checkEnterpriseCondition(kitBatchId, prepaidMonth, enterpriseWallet, totalEnterpriseRequest);
                if (tmpResponse.getErrorCode() != Common.ErrCode.SUCCESS) {
                    return tmpResponse;
                }
            }

            double discountForPrepaid = 0;
            AccountInfo accountInfo = null;
            String expireTime = "";
            boolean isExpire = false;
            Date sysdate = new Date();
            HashMap<String, String> lstParams = null;
            String original = "";
            for (KitBatchDetailModel tmpSub : lstKitBatchValid) {
                tmpProductInfo = null;
                lstParams = null;
                original = "";
                lastEffectiveTime = "";
                discountForPrepaid = 0;
                discountForUpgradePackage = 0;
                isExpire = false;
                String currentProductCode = db.getProductCodeByIsdn(tmpSub.getIsdn());
                if (currentProductCode == null || currentProductCode.trim().length() <= 0) {
                    logger.info("productCode is empty, Sub not exist or inactive: isdn: " + tmpSub.getIsdn());
                    response.setErrorCode(22);
                    response.setDescription("SUB_INVALID");
                    return response;
                }
                tmpSub.setCurrenctProductCode(currentProductCode);
//                Check new product code
                tmpProductInfo = dbPre.getProductInfo(tmpSub.getIsdn(), tmpSub.getProductCode());
                if (tmpProductInfo == null || tmpProductInfo.getMoneyFee() == null || tmpProductInfo.getPricePlan() == null
                        || tmpProductInfo.getOfferId() <= 0) {
                    logger.warn("New package is not Elite produdct " + tmpSub.getIsdn() + " " + tmpProductInfo);
                    response.setErrorCode(18);
                    response.setDescription("NEW_PACKAGE_NOT_ELITE_PRODUCT");
                    return response;
                }
//                Check old product code
                oldProductInfo = dbPre.getProductInfo(tmpSub.getIsdn(), currentProductCode);
                if (prepaidMonth == 0 && (prepaidMonth == 0 && (oldProductInfo == null || oldProductInfo.getMoneyFee() == null || oldProductInfo.getPricePlan() == null
                        || oldProductInfo.getOfferId() <= 0))) {
                    logger.warn("Current product is not Elite produdct " + tmpSub.getIsdn() + " " + oldProductInfo);
                    response.setErrorCode(25);
                    response.setDescription("CURRENT_PRODUCT_NOT_ELITE");
                    return response;
                }

//                Check current product expire
                lstParams = new HashMap<String, String>();
                lstParams.put("MSISDN", tmpSub.getIsdn().startsWith("258") ? tmpSub.getIsdn() : "258" + tmpSub.getIsdn());
                original = exch.getOriginalOfCommand(tmpSub.getIsdn().startsWith("258") ? tmpSub.getIsdn() : "258" + tmpSub.getIsdn(), "OCSHW_INTEGRATIONENQUIRY", lstParams);
                if (!original.isEmpty()) {
                    List<Offer> listOffer = exch.parseListOffer(original);
                    if (listOffer != null && !listOffer.isEmpty()) {
                        for (Offer offer : listOffer) {
                            if (offer.getName() != null && offer.getName().toUpperCase().contains("_ADDON") && "1".equals(offer.getState())
                                    && offer.getName().toUpperCase().contains(currentProductCode.toUpperCase())) {
                                SimpleDateFormat tmpSdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                                if (offer.getRecurringDate() != null && offer.getRecurringDate().length() > 0) {
                                    Date recurringDate = tmpSdf.parse(offer.getRecurringDate());
                                    lastEffectiveTime = sdf.format(recurringDate);
                                    logger.info("LastEffectiveTime of subscriber on vOCS3: " + lastEffectiveTime + ", msisdn: " + tmpSub.getIsdn());
                                }
                                tmpSdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                                if (offer.getExpDate() != null && offer.getExpDate().length() > 0 && tmpSdf.parse(offer.getExpDate()).after(new Date())) {
                                    logger.info("ExpireDate of subscriber on vOCS3: " + offer.getExpDate() + ", msisdn: " + tmpSub.getIsdn());
                                    isExpire = false;
                                }
                                break;
                            } else {
                                isExpire = true;
                            }
                        }
                    } else {
                        logger.warn("Sub dont have list offer " + tmpSub.getIsdn());
                        isExpire = true;
                    }
                } else {
                    isExpire = true;
                    logger.warn("Cannot get original of subscriber " + tmpSub.getIsdn());
                }

                if (prepaidMonth == 0 && isExpire) {
                    logger.warn("Sub have elite product expired " + tmpSub.getIsdn() + " " + tmpProductInfo);
                    response.setErrorCode(18);
                    response.setDescription("NEW_PACKAGE_NOT_ELITE_PRODUCT");
                    return response;
                }
                tmpSub.setIsExpired(isExpire);

                if (!enterpriseProductCodes.contains(tmpSub.getProductCode().toUpperCase())) {
                    percentBonus = dbPre.calculatePercentBonus(arrDiscountRateRenewForPaidMonth, prepaidMonth, tmpSub.getIsdn());
                    discountForPrepaid = Math.round(percentBonus * Long.valueOf(tmpProductInfo.getMoneyFee()) * prepaidMonth);
                    logger.info("Subscriber " + tmpSub.getIsdn() + " have paidMonth: " + prepaidMonth + ", value discount after round: " + discountForPrepaid);
                }
                if (db.checkElitePackage(tmpSub.getIsdn())) {

                    if (!checkRecurring48h(lastEffectiveTime, validHours)
                            || (db.countChangeReg48h(tmpSub.getIsdn(), currentProductCode) != 0)
                            || !db.checkSubChangeBuymore48h(enterpriseWallet, tmpSub.getIsdn().substring(3))) {
                        lastEffectiveTime = "";
                        logger.info("RecurringDate not in 48hours or exists kit_vas tables or exists in mo_his with action_type in (2,99), msisdn: " + tmpSub.getIsdn());
                    } else {
                        logger.info("RecurringDate in 48hours and not exists kit_vas tables and not exists in mo_his with action_type in (2,99), msisdn: " + enterpriseWallet);
                    }
                    if (lastEffectiveTime.length() > 0
                            && (Long.valueOf(tmpProductInfo.getMoneyFee()) > Long.valueOf(oldProductInfo.getMoneyFee()))) {
                        logger.info("Change during 48h after extending so save oldProductMoneyFee " + oldProductInfo.getMoneyFee() + " " + enterpriseWallet);
                        discountForUpgradePackage = Long.valueOf(oldProductInfo.getMoneyFee());
                    }
                }

                logger.info("Subscriber have paidMonth value: " + prepaidMonth + ", percentBonus: "
                        + strPercentBonus + ", bonusForCustomer: " + discountForPrepaid + ", discountForUpgradePackage: " + discountForUpgradePackage + ", sub:  " + tmpSub.getIsdn());

                if (Long.valueOf(tmpProductInfo.getMoneyFee()) * prepaidMonth < discountForPrepaid + discountForUpgradePackage) {
                    discountForUpgradePackage = 0;
                    logger.info("Total paid less than discount, msisdn : " + tmpSub.getIsdn() + (Long.valueOf(tmpProductInfo.getMoneyFee()) * prepaidMonth) + ", discount " + (discountForPrepaid + discountForUpgradePackage) + ", set discountForUpgradePackage = 0");
                }
                tmpSub.setMoneyProduct(Double.valueOf(tmpProductInfo.getMoneyFee()) * prepaidMonth);
                tmpSub.setMoneyDiscount(discountForPrepaid + discountForUpgradePackage);
                totalDiscount += discountForPrepaid + discountForUpgradePackage;
                totalMoneyProduct += Long.valueOf(tmpProductInfo.getMoneyFee()) * prepaidMonth;
            }

            if (lstElite.isEmpty()) {
                logger.info("List elite is empty  ");
                response.setErrorCode(22);
                response.setDescription("LIST_ELITE_EMPTY");
                return response;
            }
            if (lstElite.size() > 1) {
                for (int i = 0; i < lstElite.size(); i++) {
                    for (int j = i + 1; j < lstElite.size(); j++) {
                        if (lstElite.get(i).equals(lstElite.get(j))) {
                            logger.info("Duplicate sub in file " + lstElite.get(i));
                            response.setErrorCode(23);
                            response.setDescription("DUPLICATE_ISDN");
                            response.setExtraInfo(lstElite.get(i));
                            return response;
                        }
                    }
                }
            }
            String transId = sdf.format(new Date()) + kitBatchId;
            long moId = 0;
            int insertLog = 0;
            int insertMo = 0;
            if (enterpriseInfo.getBalance() >= Math.round(totalMoneyProduct - totalDiscount)) {
                if (!isConfirm) {
                    response.setErrorCode(Common.ErrCode.SUCCESS);
                    response.setDescription("SUCCESS");
                    response.setExtraInfo("" + Math.round(totalMoneyProduct - totalDiscount) + "|" + totalDiscount + "|" + listSubs.size() + "|" + totalSuccess + "|" + totalFailed);
                    return response;
                }
//                Get CUG information of batch
                cugName = db.getCUGInformation(Long.valueOf(kitBatchId));
                String orgRequestId = "";
                ResponseWallet responseWallet = null;
                logger.info("cugName: " + cugName + ", kitBatchId: " + kitBatchId + ", staffCode: " + staffCode);
                if (cugName != null && !cugName.isEmpty()) {
                    cugId = Integer.valueOf(cugName.split("\\_")[1]);
                }

                for (KitBatchDetailModel kitBatchDetailModel : lstKitBatchValid) {
                    insertLog = 0;
                    insertMo = 0;
                    orgRequestId = "";
                    responseWallet = null;
                    moId = 0;
//                    Prepaid month > 0: Insert mo and logs
                    if (prepaidMonth > 0) {
                        if (!kitBatchDetailModel.isIsExpired() && kitBatchDetailModel.getProductCode().toUpperCase().equals(kitBatchDetailModel.getCurrenctProductCode().toUpperCase())) {
                            responseWallet = EWalletUtil.chargeEmolaEnterpriseV2(sdf.format(new Date()) + kitBatchDetailModel.getIsdn(), db,
                                    enterpriseWallet, kitBatchDetailModel.getMoneyProduct() - kitBatchDetailModel.getMoneyDiscount(), logger, enterpriseWallet, staffCode, enterpriseInfo.getProviderCode(), enterpriseInfo.getCustomerCode(),
                                    "Deduct emola when renew product " + kitBatchDetailModel.getProductCode() + ", isdn " + kitBatchDetailModel.getIsdn());
                            if (responseWallet == null || !"01".equals(responseWallet.getResponseCode())) {
                                logger.info("Can not debit enterprise: "
                                        + ", enterpriseWallet: " + enterpriseWallet + ", staff: " + processUser + " result " + responseWallet);
//                        Insert groupManagementLog
                                insertLog = dbPre.insertGroupManagementLog(moId, kitBatchId, kitBatchDetailModel.getIsdn(), kitBatchDetailModel.getProductCode(),
                                        enterpriseWallet, 0, 2, processUser, prepaidMonth, transId, kitBatchDetailModel.getMoneyProduct(), kitBatchDetailModel.getMoneyDiscount(), "Debit emola failed", "1", "");
                                if (insertLog == 0) {
                                    logger.info("ConfirmAddPackage insert group_management_log success: isdn " + kitBatchDetailModel.getIsdn() + ", product " + kitBatchDetailModel.getProductCode() + ", prepaidMonth " + prepaidMonth
                                            + ", price " + kitBatchDetailModel.getPrice()
                                            + ", discount " + kitBatchDetailModel.getMoneyDiscount() + " result " + insertLog);
                                } else {
                                    logger.warn("ConfirmAddPackage insert group_management_log failed: isdn " + kitBatchDetailModel.getIsdn() + ", product " + kitBatchDetailModel.getProductCode() + ", prepaidMonth " + prepaidMonth
                                            + ", price " + kitBatchDetailModel.getPrice()
                                            + ", discount " + kitBatchDetailModel.getMoneyDiscount() + " result " + insertLog);
                                }
                            } else {
                                orgRequestId = responseWallet.getRequestId();//RequestId using when revertTransaction

                                if (db.checkKitElitePrepaid(kitBatchDetailModel.getIsdn(), kitBatchDetailModel.getProductCode())) {
                                    //exist record on table...
                                    String tmpProductCode = db.getProductCodeKitElitePrepaid(kitBatchDetailModel.getIsdn(), kitBatchDetailModel.getProductCode());
                                    if (kitBatchDetailModel.getProductCode().equals(tmpProductCode)) {
                                        logger.info("current productCode is the same productCode in batch, paidMonth: " + prepaidMonth + " id: "
                                                + kitBatchId + ", staffCode: " + processUser);
                                        int rsUpdate = db.updateKitElitePrepaid(kitBatchDetailModel.getIsdn(), expireTime, prepaidMonth, isExpire, kitBatchDetailModel.getProductCode());
                                        if (rsUpdate == 1) {
                                            logger.info("update kit_elite_prepaid successfully, paidMonth: " + prepaidMonth + ", id: "
                                                    + kitBatchId + ", staffCode: " + processUser);
                                        } else {
                                            logger.info("update kit_elite_prepaid unsuccessfully, paidMonth: " + prepaidMonth + " id: "
                                                    + kitBatchId + ", staffCode: " + processUser);
                                        }
                                    } else {
                                        logger.info("current productCode isn't the same productCode in batch, paidMonth: " + prepaidMonth + " id: "
                                                + kitBatchId + ", staffCode: " + processUser);
                                        int rsPrepaid = db.insertKitElitePrepaid(kitBatchDetailModel.getIsdn(), prepaidMonth, 0, processUser, kitBatchDetailModel.getProductCode(), expireTime, isExpire);
                                        if (rsPrepaid == 1) {
                                            logger.info("insert kit_elite_prepaid successfully, paidMonth: " + prepaidMonth + ",id: "
                                                    + kitBatchId + ", staffCode: " + processUser);
                                        } else {
                                            logger.info("insert kit_elite_prepaid unsuccessfully, paidMonth: " + prepaidMonth + ", id: "
                                                    + kitBatchId + ", staffCode: " + processUser);
                                        }
                                    }

                                } else {
                                    int rsPrepaid = db.insertKitElitePrepaid(kitBatchDetailModel.getIsdn(), prepaidMonth, 0, processUser, kitBatchDetailModel.getProductCode(), expireTime, isExpire);
                                    if (rsPrepaid == 1) {
                                        logger.info("insert kit_elite_prepaid successfully, paidMonth: " + prepaidMonth + ", id: "
                                                + kitBatchId + ", staffCode: " + processUser);
                                    } else {
                                        logger.info("insert kit_elite_prepaid unsuccessfully, paidMonth: " + prepaidMonth + ", id: "
                                                + kitBatchId + ", staffCode: " + processUser);
                                    }
                                }
//                        Insert groupManagementLog
                                insertLog = dbPre.insertGroupManagementLog(moId, kitBatchId, kitBatchDetailModel.getIsdn(), kitBatchDetailModel.getProductCode(),
                                        enterpriseWallet, 0, 2, processUser, prepaidMonth, transId, kitBatchDetailModel.getMoneyProduct(), kitBatchDetailModel.getMoneyDiscount(), "Same current package, renew insert kit_elite_prepaid", "0", orgRequestId);
                                if (insertLog == 0) {
                                    logger.info("ConfirmAddPackage insert group_management_log success: isdn " + kitBatchDetailModel.getIsdn() + ", product " + kitBatchDetailModel.getProductCode() + ", prepaidMonth " + prepaidMonth
                                            + ", price " + kitBatchDetailModel.getPrice()
                                            + ", discount " + kitBatchDetailModel.getMoneyDiscount() + " result " + insertLog);
                                } else {
                                    logger.warn("ConfirmAddPackage insert group_management_log failed: isdn " + kitBatchDetailModel.getIsdn() + ", product " + kitBatchDetailModel.getProductCode() + ", prepaidMonth " + prepaidMonth
                                            + ", price " + kitBatchDetailModel.getPrice()
                                            + ", discount " + kitBatchDetailModel.getMoneyDiscount() + " result " + insertLog);
                                }

                            }

                        } else {
                            moId = db.getSequence("MO_SEQ", "dbvas");
                            //pt|2-buy for other|des sub|prepaid month|3-pay by emola
                            paramMo = "pt|2|" + kitBatchDetailModel.getIsdn() + "|" + prepaidMonth + "|3";
                            insertMo = db.insertMoV2(moId, enterpriseWallet, kitBatchDetailModel.getProductCode(), paramMo, addMemberPckActionType, changePckChannel, changePckChannelType);
                            if (insertMo == 0) {
                                logger.info("ConfirmAddPackage insert mo success: isdn " + kitBatchDetailModel.getIsdn() + ", product " + kitBatchDetailModel.getProductCode() + ", prepaidMonth " + prepaidMonth
                                        + ", price " + kitBatchDetailModel.getPrice()
                                        + ", discount " + kitBatchDetailModel.getMoneyDiscount() + " result " + insertMo);
                            } else {
                                logger.info("ConfirmAddPackage insert mo failed: isdn " + kitBatchDetailModel.getIsdn() + ", product " + kitBatchDetailModel.getProductCode() + ", prepaidMonth " + prepaidMonth
                                        + ", price " + kitBatchDetailModel.getPrice()
                                        + ", discount " + kitBatchDetailModel.getMoneyDiscount() + " result " + insertMo);
                            }
//                        Insert groupManagementLog
                            insertLog = dbPre.insertGroupManagementLog(moId, kitBatchId, kitBatchDetailModel.getIsdn(), kitBatchDetailModel.getProductCode(),
                                    enterpriseWallet, 0, 2, processUser, prepaidMonth, transId, kitBatchDetailModel.getMoneyProduct(), kitBatchDetailModel.getMoneyDiscount(), "Insert mo", "", "");
                            if (insertLog == 0) {
                                logger.info("ConfirmAddPackage insert group_management_log success: isdn " + kitBatchDetailModel.getIsdn() + ", product " + kitBatchDetailModel.getProductCode() + ", prepaidMonth " + prepaidMonth
                                        + ", price " + kitBatchDetailModel.getPrice()
                                        + ", discount " + kitBatchDetailModel.getMoneyDiscount() + " result " + insertLog);
                            } else {
                                logger.warn("ConfirmAddPackage insert group_management_log failed: isdn " + kitBatchDetailModel.getIsdn() + ", product " + kitBatchDetailModel.getProductCode() + ", prepaidMonth " + prepaidMonth
                                        + ", price " + kitBatchDetailModel.getPrice()
                                        + ", discount " + kitBatchDetailModel.getMoneyDiscount() + " result " + insertLog);
                            }
                        }

                    } else {
//                        Insert groupManagementLog
                        insertLog = dbPre.insertGroupManagementLog(moId, kitBatchId, kitBatchDetailModel.getIsdn(), kitBatchDetailModel.getProductCode(),
                                enterpriseWallet, 0, 2, processUser, prepaidMonth, transId, kitBatchDetailModel.getMoneyProduct(),
                                kitBatchDetailModel.getMoneyDiscount(), "Insert sub to kit_batch_extend", "0", "");
                        if (insertLog == 0) {
                            logger.info("ConfirmAddPackage insert group_management_log success: isdn " + kitBatchDetailModel.getIsdn() + ", product " + kitBatchDetailModel.getProductCode() + ", prepaidMonth " + prepaidMonth
                                    + ", price " + kitBatchDetailModel.getPrice()
                                    + ", discount " + kitBatchDetailModel.getMoneyDiscount() + " result " + insertLog);
                        } else {
                            logger.warn("ConfirmAddPackage insert group_management_log failed: isdn " + kitBatchDetailModel.getIsdn() + ", product " + kitBatchDetailModel.getProductCode() + ", prepaidMonth " + prepaidMonth
                                    + ", price " + kitBatchDetailModel.getPrice()
                                    + ", discount " + kitBatchDetailModel.getMoneyDiscount() + " result " + insertLog);
                        }
                    }
                }

                response.setErrorCode(Common.ErrCode.SUCCESS);
                response.setDescription("SUCCESS");
                response.setExtraInfo("" + Math.round(totalMoneyProduct - totalDiscount) + "|" + totalDiscount + "|" + listSubs.size() + "|" + totalSuccess + "|" + totalFailed);
                return response;
            } else {
                logger.info("ConfirmAddMember total subs " + listSubs.size() + ", total success " + totalSuccess + ", total failed " + totalFailed
                        + ", total amount " + totalMoneyProduct + ", total discount " + totalDiscount);
                response.setErrorCode(Vas.ResultCode.NOT_ENOUGH_MONEY);
                response.setDescription("NOT_ENOUGH_MONEY");
                return response;
            }
        } catch (Exception e) {
            logger.error("[!!!] Error ConfirmAddMember ", e);
            logger.error(AppManager.logException(timeStart, e));
            response.setErrorCode(Common.ErrCode.FAIL);
            response.setDescription("ERROR");
            return response;
        }
    }

    public boolean checkRecurring48h(String lastTimeChange, int hoursValid) {
        try {
            if (lastTimeChange != null && lastTimeChange.length() > 0) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                Date lastTime = sdf.parse(lastTimeChange);
                Date currTime = new Date();
                long hoursDiff = (currTime.getTime() - lastTime.getTime()) / 1000 / 3600;
                if (hoursDiff <= hoursValid) {
                    return Boolean.TRUE;
                }
            }
        } catch (Exception ex) {
            logger.error("EliteChangePck :" + ">>checkValidChangePckTime:" + ex.getMessage());
            return Boolean.FALSE;
        }
        return Boolean.FALSE;
    }

    public ResponseGroupMgt checkEnterpriseCondition(int kitBatchId, int prepaidMonth, String enterpriseWallet, int totalEliteEnterpriseRegist) {
        ResponseGroupMgt response = new ResponseGroupMgt();
        long timeStart = System.currentTimeMillis();
        try {
            logger.info("Start check kitBatch valid " + kitBatchId + ", " + enterpriseWallet);
            Integer limitPrepaidMonth = Config.getIntValue(Config.limitPrepaidMonth, logger);
            Integer limitSubEnterprise = Config.getIntValue(Config.limitSubEnterprise, logger);
            String enterprisePPCode = Config.getConfig(Config.enterprisePricePlanCode, logger);
            HashMap<String, String> lstMapProductPP = new HashMap<String, String>();
            String[] tmpMapPricePlan = enterprisePPCode.split("\\|");
            List<Subscriber> listSubs = null;
            String tmpPricePlan = "";
            int countEnterpriseSub = 0;
            Subscriber tmpSub;
            for (String pp : tmpMapPricePlan) {
                String[] tmp = pp.split("\\:");
                lstMapProductPP.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            }
            if (prepaidMonth < limitPrepaidMonth) {
                logger.info("Check prepaid month " + prepaidMonth + ", prepaid month less than " + limitPrepaidMonth);
                response.setErrorCode(11);
                response.setDescription("LIMIT_PREPAID_MONTH");
                return response;
            }
            listSubs = db.getAllSub(enterpriseWallet);
            if (listSubs != null) {
                for (int i = 0; i < listSubs.size(); i++) {
                    tmpSub = listSubs.get(i);
                    tmpPricePlan = lstMapProductPP.get(tmpSub.getProductCode().toUpperCase());
                    if (tmpPricePlan != null && !tmpPricePlan.isEmpty() && exch.checkValidProductID(tmpSub.getIsdn(), tmpPricePlan)) {
                        countEnterpriseSub++;
                    }
                    if (countEnterpriseSub >= limitSubEnterprise) {
                        break;
                    }
                }
            }
            if (countEnterpriseSub + totalEliteEnterpriseRegist < limitSubEnterprise) {
                logger.info("Check kitBatchId total enterprise add new " + totalEliteEnterpriseRegist + ", current enterprise in batch " + countEnterpriseSub + " less than " + limitSubEnterprise);
                response.setErrorCode(12);
                response.setDescription("LIMIT_ENTERPRISE_SUB");
                return response;
            }
            response.setErrorCode(Common.ErrCode.SUCCESS);
            response.setDescription("SUCCESS");
            return response;
        } catch (Exception e) {
            logger.error(AppManager.logException(timeStart, e));
            response.setErrorCode(Common.ErrCode.FAIL);
            response.setDescription("ERROR");
            return response;
        }
    }
}
