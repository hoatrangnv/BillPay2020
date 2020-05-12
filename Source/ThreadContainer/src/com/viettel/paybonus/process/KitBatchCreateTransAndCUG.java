/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbKitbtachCreateTransAndCUG;
import com.viettel.paybonus.obj.KitBatch;
import com.viettel.paybonus.obj.KitBatchGroup;
import com.viettel.paybonus.service.Exchange;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class KitBatchCreateTransAndCUG extends ProcessRecordAbstract {

    Exchange pro;
    DbKitbtachCreateTransAndCUG db;
    String eliteConfigUserMakeRevenue;
    String lstIsdnReceiveError;
    String[] arrIsdnReceiveError;
    String minSubInBatch;
    String eliteKitBatchGroupFreeCallMsg;
    SimpleDateFormat sdfAddPrice = new SimpleDateFormat("yyyyMMddHHmmss");
    String smsChannel;
    String smsAddGroupMemberSuccess;

    public KitBatchCreateTransAndCUG() {
        super();
        logger = Logger.getLogger(KitBatchCreateTransAndCUG.class);
        lstIsdnReceiveError = ResourceBundle.getBundle("configPayBonus").getString("lstIsdnReceiveError");
        arrIsdnReceiveError = lstIsdnReceiveError.split("\\|");
        minSubInBatch = ResourceBundle.getBundle("configPayBonus").getString("minSubInBatch");
        eliteKitBatchGroupFreeCallMsg = ResourceBundle.getBundle("configPayBonus").getString("eliteKitBatchGroupFreeCallMsg");
        smsChannel = ResourceBundle.getBundle("configPayBonus").getString("smsChannel");
        eliteConfigUserMakeRevenue = ResourceBundle.getBundle("configPayBonus").getString("eliteConfigUserMakeRevenue");
        smsAddGroupMemberSuccess = ResourceBundle.getBundle("configPayBonus").getString("smsAddGroupMemberSuccess");
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbKitbtachCreateTransAndCUG();

    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        long kitBatchId = 0;
        long saleTransId = 0;
        String staffCode = eliteConfigUserMakeRevenue;
        List<String> successNumbers = new ArrayList<String>();
        List<String> failedNumbers = new ArrayList<String>();
        List<KitBatchGroup> listKitBatchGroup = null;
        String successNumberStr = "";
        String failedNumberStr = "";
        double totalPrice = 0;
        double totalDiscount = 0;
        double totalPaid = 0;

        for (Record record : listRecord) {
            KitBatchGroup bn = (KitBatchGroup) record;
            listResult.add(bn);
            kitBatchId = Long.valueOf(bn.getKitBatchId());
            saleTransId = 0;
            successNumberStr = "";
            failedNumberStr = "";
            listKitBatchGroup = db.getKitBatchGroup(kitBatchId, bn.getTransId());
            if (listKitBatchGroup == null || listKitBatchGroup.isEmpty()) {
                continue;
            }
//            Case change single package
            if (listKitBatchGroup.size() == 1 && bn.getActionType() == 1) {
                KitBatchGroup tmpKitBatch = listKitBatchGroup.get(0);
                if ("0".equals(tmpKitBatch.getResultCode())) {
                    //<editor-fold defaultstate="collapsed" desc="CREATE SALE TRANS">
                    saleTransId = db.getSequence("SALE_TRANS_SEQ", "dbsm");
                    String strTempId = db.getShopIdStaffIdByStaffCode(eliteConfigUserMakeRevenue);
                    String[] arrTempId = strTempId.split("\\|");
                    int rsMakeSaleTrans = db.insertSaleTrans(saleTransId, Long.valueOf(arrTempId[0]), Long.valueOf(arrTempId[1]),
                            0L, 0L, tmpKitBatch.getPrice() - tmpKitBatch.getDiscount(), 0L,
                            tmpKitBatch.getEnterpriseWallet(), 201007L, tmpKitBatch.getTransId(), 3, "", tmpKitBatch.getDiscount(), 0);
                    if (rsMakeSaleTrans != 1) {
                        //Insert fail, insert log, send sms
                        db.insertLogMakeSaleTransFail(saleTransId, tmpKitBatch.getEnterpriseWallet(), tmpKitBatch.getEnterpriseWallet(), Long.valueOf(arrTempId[0]),
                                Long.valueOf(arrTempId[1]), 0L, 0L,
                                201007L, 0L, 0L, "SALE_TRANS");
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, "Make saleTrans fail - KitBatchCreateTransAndCUG, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                    + saleTransId, "86142");
                        }
                    }
                    Long saleTransDetailSaleService = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
                    int rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, "208309", "520784", "",//stockModelId = 208309 for emola scratch card connect, priceId = 520784 --> For price of emola scratch card
                            "", "", "", "", "", "", "", "", "", "", "17",
                            "1", "", tmpKitBatch.getPrice(), tmpKitBatch.getDiscount(), tmpKitBatch.getPrice().longValue());
                    if (rsSaleTransDetailSaleServices != 1) {
                        //Insert fail, insert log, send sms
                        db.insertLogMakeSaleTransFail(saleTransId, tmpKitBatch.getEnterpriseWallet(), tmpKitBatch.getEnterpriseWallet(), Long.valueOf(arrTempId[0]),
                                Long.valueOf(arrTempId[1]), 0L, 0L,
                                201007L, saleTransDetailSaleService, 0L, "SALE_TRANS_DETAIL");
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, "Make saleTransDetail - KitBatchCreateTransAndCUG, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailSaleService, "86142");
                        }
                    }
//</editor-fold>
                } else {
                    //save log
                    logger.info("Process transId " + tmpKitBatch.getTransId() + ", ignore process result code " + tmpKitBatch.getResultCode());
                }
            } else if (bn.getActionType() == 2) {   //Case add member
                totalPrice = 0;
                totalDiscount = 0;
                totalPaid = 0;
                for (KitBatchGroup kit : listKitBatchGroup) {
                    if ("0".equals(kit.getResultCode()) && kit.getPrepaidMonth() > 0) {//Add member
                        totalPrice += kit.getPrice();
                        totalDiscount += kit.getDiscount();
                        successNumbers.add(kit.getIsdn());
                    } else {
                        failedNumbers.add(kit.getIsdn());
                    }
                }
                totalPaid = totalPrice - totalDiscount;
                //<editor-fold defaultstate="collapsed" desc="CREATE SALE TRANS">
                if (totalPaid > 0) {

                    saleTransId = db.getSequence("SALE_TRANS_SEQ", "dbsm");
                    String strTempId = db.getShopIdStaffIdByStaffCode(eliteConfigUserMakeRevenue);
                    String[] arrTempId = strTempId.split("\\|");
                    int rsMakeSaleTrans = db.insertSaleTrans(saleTransId, Long.valueOf(arrTempId[0]), Long.valueOf(arrTempId[1]),
                            0L, 0L, totalPaid, 0L,
                            bn.getEnterpriseWallet(), 201007L, bn.getTransId(), 3, "", totalDiscount, 0);
                    if (rsMakeSaleTrans != 1) {
                        //Insert fail, insert log, send sms
                        db.insertLogMakeSaleTransFail(saleTransId, bn.getEnterpriseWallet(), bn.getEnterpriseWallet(), Long.valueOf(arrTempId[0]),
                                Long.valueOf(arrTempId[1]), 0L, 0L,
                                201007L, 0L, 0L, "SALE_TRANS");
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, "Make saleTrans fail - KitBatchCreateTransAndCUG, more detail in table: kit_make_sale_trans_fail. saleTransId: "
                                    + saleTransId, "86142");
                        }
                    }
                    Long saleTransDetailSaleService = db.getSequence("SALE_TRANS_DETAIL_SEQ", "dbsm");
                    int rsSaleTransDetailSaleServices = db.insertSaleTransDetail(saleTransDetailSaleService, saleTransId, "208309", "520784", "",//stockModelId = 208309 for emola scratch card connect, priceId = 520784 --> For price of emola scratch card
                            "", "", "", "", "", "", "", "", "", "", "17",
                            "1", "", totalPrice, totalDiscount, (long) (totalPrice));
                    if (rsSaleTransDetailSaleServices != 1) {
                        //Insert fail, insert log, send sms
                        db.insertLogMakeSaleTransFail(saleTransId, bn.getEnterpriseWallet(), bn.getEnterpriseWallet(), Long.valueOf(arrTempId[0]),
                                Long.valueOf(arrTempId[1]), 0L, 0L,
                                201007L, saleTransDetailSaleService, 0L, "SALE_TRANS_DETAIL");
                        for (String isdn : arrIsdnReceiveError) {
                            db.sendSms(isdn, "Make saleTransDetail - KitBatchCreateTransAndCUG, more detail in table: kit_make_sale_trans_fail. saleTransDetailId: " + saleTransDetailSaleService, "86142");
                        }
                    }
                }

//</editor-fold>

                //<editor-fold defaultstate="collapsed" desc="CREATE CUG">
                //Step 1: Check group already have CUG_ID or not
                String cugName = db.getCUGInformation(Long.valueOf(kitBatchId));
                //SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                long extFromKitBatdId = db.getKitBatchExtendId(kitBatchId);
                if (cugName == null || cugName.isEmpty() || "N/A".equals(cugName)) {
                    List<KitBatch> lstKitElite = db.getListKitBatchDetailElite(kitBatchId);
                    List<KitBatch> lstKitEliteExtend = db.getListKitBatchDetailElite(extFromKitBatdId);
                    List<KitBatch> lstKitExtend = db.getListKitBatchExtend(kitBatchId);
                    List<KitBatch> lstKitExtend2 = db.getListKitBatchExtend(extFromKitBatdId);

                    for (KitBatchGroup kit : listKitBatchGroup) {
                        if ("0".equals(kit.getResultCode())) {
                            db.insertKitBatchExtend(Long.valueOf(kitBatchId), kit.getIsdn(), 0L, "",
                                    "0", "Extend successfully", staffCode);
                        }
                    }
                    if (lstKitElite != null && lstKitEliteExtend != null && lstKitExtend != null && lstKitExtend2 != null
                            && (lstKitElite.size() + lstKitEliteExtend.size() + lstKitExtend.size() + lstKitExtend2.size() + listKitBatchGroup.size()) >= Integer.parseInt(minSubInBatch)) {
                        logger.info("start make group elite for call free..., id: " + kitBatchId);
                        lstKitElite.addAll(lstKitEliteExtend);
                        lstKitElite.addAll(lstKitExtend);
                        lstKitElite.addAll(lstKitExtend2);
                        String ownerOfGroup = lstKitElite.get(0).getIsdn();
                        Long cugId = db.getSequence("CUG_ID_FTTH_MOBILE_SEQ", "cmPos");
                        cugName = ownerOfGroup + "_" + cugId;
                        int addDay = 30;
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(new Date());
                        cal.add(Calendar.DATE, addDay + 1); //20180807 add one more day because OCS trunc to begining of day
                        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
                        String expireTimeGroup = sdf2.format(cal.getTime());

                        //Step 3: Make group on OCS
                        String[] resultCreateGroup = pro.createGroupCUG(String.valueOf(cugId));
                        if (resultCreateGroup == null || resultCreateGroup.length == 0 || (!"0".equals(resultCreateGroup[0]) && !"102010822".equals(resultCreateGroup[0]))) {
                            logger.warn("Fail to create group on OCS "
                                    + " error_code " + resultCreateGroup + " batchId " + kitBatchId);
                        } else {
                            //Step 4: Add owner to group on OCS
                            String[] resultAddOwner = pro.addMemberGroupCUG(String.valueOf(cugId), ownerOfGroup);
                            if (resultAddOwner == null || resultAddOwner.length == 0 || (!"0".equals(resultAddOwner[0]) && !"102010462".equals(resultAddOwner[0]))) {
                                logger.warn(ownerOfGroup + " fail to add owner to group on OCS GroupId: " + cugId
                                        + " error " + resultAddOwner + " batchId " + kitBatchId);

                            } else {
                                //Step 5: Add priceplan for owner on OCS
                                String resultAddPriceOwner = pro.addPriceV2(ownerOfGroup, "11205034", "", expireTimeGroup);
                                if (!"0".equals(resultAddPriceOwner) && !"102010228".equals(resultAddPriceOwner)) {
                                    logger.warn(ownerOfGroup + " fail to add price 11205034 for owner sub on OCS " + cugId
                                            + " error_code " + resultAddPriceOwner + " batchId " + kitBatchId);
                                } else {
                                    //Step 5.1: Set ExpireTime Call free of group
                                    db.updateExpireTimeKitBatchInfo(lstKitElite.get(0).getKitBatchId(), expireTimeGroup);
                                    db.updateGroupEliteKitBatchDetail(lstKitElite.get(0).getKitBatchId(), ownerOfGroup, resultAddPriceOwner, String.valueOf(cugId),
                                            cugName, ownerOfGroup);
                                    //Step 6: Add member...
                                    for (KitBatch kitBatch : lstKitElite) {
                                        if (!ownerOfGroup.equals(kitBatch.getIsdn())) {
                                            //Step 3: Add member to group on OCS
                                            String[] resultAddMember = pro.addMemberGroupCUG(String.valueOf(cugId), kitBatch.getIsdn());
                                            if (resultAddMember == null || resultAddMember.length == 0 || (!"0".equals(resultAddMember[0]) && !"102010462".equals(resultAddMember[0]))) {
                                                logger.warn("Fail to add member " + kitBatch.getIsdn() + " to OCS groupid " + cugId
                                                        + " error " + resultAddMember + " batchId " + kitBatchId);
                                                db.updateGroupEliteKitBatchDetail(kitBatch.getKitBatchId(), kitBatch.getIsdn(), resultAddMember != null ? resultAddMember[0] : null, String.valueOf(cugId), cugName, "");
                                            } else {
                                                logger.warn("Add member " + kitBatch.getIsdn() + " to OCS groupid " + cugId
                                                        + " successfully error " + resultAddMember + " batchId " + kitBatchId);
                                                //Add priceplan for member on OCS
                                                String resultAddPriceMember = pro.addPriceV2(kitBatch.getIsdn(), "11205034", "", expireTimeGroup);
                                                if (!"0".equals(resultAddPriceMember)) {
                                                    if ("102010228".equals(resultAddPriceMember)) {
                                                        logger.warn("Price 11205034 is exist, so remove price and add again...for owner sub on OCS " + cugId
                                                                + " error_code " + resultAddPriceMember);
                                                        pro.removePrice(kitBatch.getIsdn(), "11205034");
                                                        pro.addPriceV2(kitBatch.getIsdn(), "11205034", "", expireTimeGroup);
                                                    } else {
                                                        logger.warn(" fail to add price 11205034 for owner sub on OCS " + cugId
                                                                + " error_code " + resultAddPriceMember);
                                                    }
                                                } else {
                                                    logger.warn(ownerOfGroup + " add price 11205034 for owner sub on OCS " + cugId
                                                            + "successfully error_code " + resultAddPriceMember + " batchId " + kitBatchId);
                                                }
                                                db.updateExpireTimeKitBatchInfo(kitBatch.getKitBatchId(), expireTimeGroup);
                                                db.updateGroupEliteKitBatchDetail(kitBatch.getKitBatchId(), kitBatch.getIsdn(), resultAddPriceMember, String.valueOf(cugId), cugName, "");
                                            }
                                        }
                                    }
                                    for (KitBatchGroup sub : listKitBatchGroup) {
                                        String isdn = sub.getIsdn();
                                        if (!ownerOfGroup.equals(isdn)) {
                                            //Step 3: Add member to group on OCS
                                            String[] resultAddMember = pro.addMemberGroupCUG(String.valueOf(cugId), isdn);
                                            if (resultAddMember == null || resultAddMember.length == 0 || (!"0".equals(resultAddMember[0]) && !"102010462".equals(resultAddMember[0]))) {
                                                logger.warn("Fail to add member " + isdn + " to OCS groupid " + cugId
                                                        + " errorcode " + resultAddMember + " batchId " + kitBatchId);
                                                db.updateKitBatchExtend(Long.valueOf(kitBatchId), isdn, Long.valueOf(cugId), cugName,
                                                        resultAddMember != null ? resultAddMember[0] : null, "Extend unsuccessfully, add member fail.", staffCode);
                                            } else {
                                                logger.warn("Add member " + isdn + " to OCS groupid " + cugId
                                                        + " successfully errorcode " + resultAddMember + " batchId " + kitBatchId);
                                                //Add priceplan for member on OCS
                                                String resultAddPriceMember = pro.addPriceV2(isdn, "11205034", "", expireTimeGroup);
                                                if (!"0".equals(resultAddPriceMember)) {
                                                    if ("102010228".equals(resultAddPriceMember)) {
                                                        logger.warn("Price 11205034 is exist, so remove price and add again...for owner sub on OCS " + cugId
                                                                + " error_code " + resultAddPriceMember);
                                                        pro.removePrice(isdn, "11205034");
                                                        String tmpRs = pro.addPriceV2(isdn, "11205034", "", expireTimeGroup);
                                                        db.updateKitBatchExtend(Long.valueOf(kitBatchId), isdn, Long.valueOf(cugId), cugName,
                                                                tmpRs, "Extend successfully", staffCode);
                                                    } else {
                                                        logger.warn(" fail to add price 11205034 for owner sub on OCS " + cugId
                                                                + " error_code " + resultAddPriceMember);
                                                        db.updateKitBatchExtend(Long.valueOf(kitBatchId), isdn, Long.valueOf(cugId), cugName,
                                                                resultAddPriceMember, "Extend unsuccessfully, add price 11205034 fail.", staffCode);
                                                    }
                                                } else {
                                                    logger.warn(ownerOfGroup + " add price 11205034 for owner sub on OCS " + cugId
                                                            + "successfully error_code " + resultAddPriceMember + " batchId " + kitBatchId);
                                                    db.updateKitBatchExtend(Long.valueOf(kitBatchId), isdn, Long.valueOf(cugId), cugName,
                                                            resultAddPriceMember, "Extend successfully", staffCode);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {

                        lstKitEliteExtend.addAll(lstKitExtend);
                        lstKitEliteExtend.addAll(lstKitExtend2);
                        logger.info("Process transId: " + bn.getTransId() + ", Total sub elite extend: " + lstKitElite.size() + " total sub elite of oldKitBatchId: "
                                + lstKitEliteExtend.size()
                                + "id: " + kitBatchId + ", staffCode: " + staffCode);
                    }
                } else {
                    String cugId = cugName.split("\\_")[1];
                    for (KitBatchGroup kit : listKitBatchGroup) {
                        if ("0".equals(kit.getResultCode())) {
                            db.insertKitBatchExtend(Long.valueOf(kitBatchId), kit.getIsdn(), 0L, "",
                                    "0", "Extend successfully", staffCode);
                        }
                    }
//                Step 4: Add to group call free
                    String expireTime = db.getExpireTimeCUG(Long.valueOf(kitBatchId));
                    if (expireTime == null || expireTime.isEmpty()) {
                        logger.info("Process transId: " + bn.getTransId() + ", expireTime of CUG: " + expireTime);
                        continue;
                    }
//                Step 4.1: Check expire time with sysdate...
                    Date sysdate = new Date();
                    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
                    if (sdf2.parse(expireTime).compareTo(sysdate) < 0) {
                        logger.info("Process transId: " + bn.getTransId() + ", expireTime before sysdate...not accept add to group call free.");
                        continue;
                    } else {
                        logger.info("expireTime: " + expireTime + " after sysdate: "
                                + sysdate.toString() + "...accept add to group call free.");
                    }
                    logger.info("start add member for CUG_ID: " + cugId + ", kitBatchId: " + kitBatchId + ", staffCode: " + staffCode);
//                Exchange pro = new Exchange(ExchangeClientChannel.getInstance("D:\\Restart\\REAL_BUILD_WEB_20140701\\etc\\exchange_client.cfg").getInstanceChannel(), logger);
                    for (KitBatchGroup kit : listKitBatchGroup) {
                        if (kit.getStatus() != 1 && "0".equals(kit.getResultCode())) {//Add member
                            String[] resultAddMember = pro.addMemberGroupCUG(cugId, kit.getIsdn());
                            if (resultAddMember == null || resultAddMember.length == 0 || (!"0".equals(resultAddMember[0]) && !"102010462".equals(resultAddMember[0]))) {
                                logger.warn("Fail to add member " + kit.getIsdn() + " to OCS groupid " + cugId
                                        + " errorcode " + resultAddMember);
                                db.updateKitBatchExtend(Long.valueOf(kitBatchId), kit.getIsdn(), Long.valueOf(cugId), cugName,
                                        resultAddMember != null ? resultAddMember[0] : null, "Extend unsuccessfully, add member fail.", staffCode);
                            } else {
                                logger.warn("Add member " + kit.getIsdn() + " to OCS groupid " + cugId
                                        + " successfully errorcode " + resultAddMember);
                                //Add priceplan for member on OCS
                                String resultAddPriceMember = pro.addPriceV2(kit.getIsdn(), "11205034", "", expireTime);
                                if (!"0".equals(resultAddPriceMember)) {
                                    if ("102010228".equals(resultAddPriceMember)) {
                                        logger.warn("Price 11205034 is exist, so remove price and add again...for owner sub on OCS " + cugId
                                                + " error_code " + resultAddPriceMember);
                                        pro.removePrice(kit.getIsdn(), "11205034");
                                        String tmpRs = pro.addPriceV2(kit.getIsdn(), "11205034", "", expireTime);
                                        db.updateKitBatchExtend(Long.valueOf(kitBatchId), kit.getIsdn(), Long.valueOf(cugId), cugName,
                                                tmpRs, "Extend successfully", staffCode);
                                    } else {
                                        logger.warn(" fail to add price 11205034 for owner sub on OCS " + cugId
                                                + " error_code " + resultAddPriceMember);
                                        db.updateKitBatchExtend(Long.valueOf(kitBatchId), kit.getIsdn(), Long.valueOf(cugId), cugName,
                                                resultAddPriceMember, "Extend unsuccessfully, add price 11205034 fail.", staffCode);
                                    }

                                } else {
                                    db.updateKitBatchExtend(Long.valueOf(kitBatchId), kit.getIsdn(), Long.valueOf(cugId), cugName,
                                            "0", "Extend successfully", staffCode);
//                            db.updateGroupEliteKitBatchDetail(bn.getKitBatchId(), kitBatch.getIsdn(), resultAddPriceMember, String.valueOf(cugId), cugName, "");
                                    logger.warn(" add price 11205034 for owner sub on OCS " + cugId
                                            + "successfully error_code " + resultAddPriceMember);
                                }
                            }
                        }
                    }
                }
//</editor-fold>

                for (String mobile : successNumbers) {
                    successNumberStr += mobile + ";";
                }
                for (String mobile : failedNumbers) {
                    failedNumberStr += mobile + ";";
                }
                String sms = smsAddGroupMemberSuccess;
                sms = sms.replace("%SUB%", successNumberStr);
                sms = sms.replace("%TOTALPRICE%", "" + totalPrice);
                sms = sms.replace("%TOTALDISCOUNT%", "" + totalDiscount);
                sms = sms.replace("%TOTALPAID%", "" + totalPaid);
                sms = sms.replace("%TOTALSUCCESS%", "" + successNumbers.size());
                sms = sms.replace("%TOTALFAILED%", "" + failedNumbers.size());
                sms = sms.replace("%SUBFAILED%", failedNumberStr);
                db.sendSms(bn.getEnterpriseWallet(), sms, smsChannel);
            }
        }
        listRecord.clear();
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        br.setLength(0);
        br.append("\r\n").
                append("|\tID\t|").
                append("|\tKIT_BATCH_ID\t|").
                append("|\tTRANS_ID\t|").
                append("|\tISDN\t|").
                append("|\tPRODUCT_CODE\t|").
                append("|\tENTERPRISE_WALLET\t|").
                append("|\tCREATE_USER\t|").
                append("|\tACTION_TYPE\t|");
        for (Record record : listRecord) {
            KitBatchGroup bn = (KitBatchGroup) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getBatchId()).
                    append("||\t").
                    append(bn.getTransId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getProductCode()).
                    append("||\t").
                    append(bn.getEnterpriseWallet()).
                    append("||\t").
                    append(bn.getCreateUser()).
                    append("||\t").
                    append(bn.getActionType());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
}
