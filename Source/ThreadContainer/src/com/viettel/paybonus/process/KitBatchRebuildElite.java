/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbKitBatchRebuildEliteProcessor;
import com.viettel.paybonus.obj.AccountInfo;
import com.viettel.paybonus.obj.KitBatch;
import com.viettel.paybonus.obj.KitBatchInfo;
import com.viettel.paybonus.service.Exchange;
import com.viettel.paybonus.service.Service;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class KitBatchRebuildElite extends ProcessRecordAbstract {

    Exchange pro;
    Service services;
    DbKitBatchRebuildEliteProcessor db;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String eliteRebuildGroupFreeCall;
    String[] arrEliteRebuildGroupFreeCall;
    ArrayList<HashMap> lstMapEliteRebuildGroupFreeCall;

    public KitBatchRebuildElite() {
        super();
        logger = Logger.getLogger(KitBatchRebuildElite.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        services = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), logger);
        db = new DbKitBatchRebuildEliteProcessor();
        eliteRebuildGroupFreeCall = ResourceBundle.getBundle("configPayBonus").getString("eliteRebuildGroupFreeCall");
        arrEliteRebuildGroupFreeCall = eliteRebuildGroupFreeCall.split("\\|");
        lstMapEliteRebuildGroupFreeCall = new ArrayList<HashMap>();
        for (String tmp : arrEliteRebuildGroupFreeCall) {
            String[] arrTmp = tmp.split("\\:");
            HashMap map = new HashMap();
            map.put(arrTmp[0].trim().toUpperCase(), arrTmp[1].trim().toUpperCase());
            lstMapEliteRebuildGroupFreeCall.add(map);
        }
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            KitBatchInfo moRecord = (KitBatchInfo) record;
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
        Long actionAuditId;
        boolean isSuccess;
        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            staffCode = "";
            actionAuditId = 0L;
            isSuccess = false;
            KitBatchInfo bn = (KitBatchInfo) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                Step 1: Get List KitBatchDetail...
                List<KitBatch> lstKitBatchDetail = db.getListKitBatchDetail(bn.getKitBatchId());
                logger.info("Total record kitBatchDetail: " + lstKitBatchDetail.size() + ", id " + bn.getKitBatchId());
                List<KitBatch> lstKitBatchExtend = db.getListKitBatchExtend(bn.getKitBatchId());
                logger.info("Total record lstKitBatchExtend: " + lstKitBatchDetail.size() + ", id " + bn.getKitBatchId());
                if (lstKitBatchExtend.size() > 0) {
                    for (KitBatch kitBatch : lstKitBatchExtend) {
                        String productCode = db.getProductCode(kitBatch.getIsdn());
                        if (db.checkVipProductConnectKit(productCode)) {
                            kitBatch.setProductCode(productCode);
                            lstKitBatchDetail.add(kitBatch);
                        }
                    }
                }
                logger.info("Total sub in group: " + lstKitBatchDetail.size() + ", id " + bn.getKitBatchId());
                if (lstKitBatchDetail.isEmpty()) {
                    logger.warn("List KitBatchDetail is null or empty, id " + bn.getKitBatchId());
                    bn.setResultCode("01");
                    bn.setDescription("List KitBatchDetail is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.insertKitBatchRebuildHis(bn.getKitBatchId(), "", "", "", "", "", "", "", bn.getResultCode(), bn.getDescription(), 0L);
                    continue;
                }
                staffCode = bn.getCreateUser();
                if (staffCode == null || "".equals(staffCode)) {
                    logger.warn("create_user in kit_batch_info is null or empty, id " + bn.getKitBatchId());
                    bn.setResultCode("02");
                    bn.setDescription("create_user in kit_batch_info is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.insertKitBatchRebuildHis(bn.getKitBatchId(), "", "", "", "", "", "", "", bn.getResultCode(), bn.getDescription(), 0L);
                    continue;
                }
//                Step 2: Check subscriber renew successfully or not...
                Calendar cal = Calendar.getInstance();
                cal.setTime(sdf.parse(bn.getExpireTimeGroup()));
                cal.add(Calendar.DATE, 30);
                String newExpireTime = sdf.format(cal.getTime());
                actionAuditId = db.getSequence("seq_action_audit", "cm_pre");
                for (KitBatch kitBatch : lstKitBatchDetail) {
                    String isdn = kitBatch.getIsdn();
                    String productCode = db.getProductCode(kitBatch.getIsdn());
                    kitBatch.setProductCode(productCode);
                    if (!isdn.startsWith("258")) {
                        isdn = "258" + isdn;
                    }
                    String pricePlanBonus = "";
                    for (int i = 0; i < lstMapEliteRebuildGroupFreeCall.size(); i++) {
                        if (lstMapEliteRebuildGroupFreeCall.get(i).containsKey(kitBatch.getProductCode().trim().toUpperCase())) {
                            pricePlanBonus = lstMapEliteRebuildGroupFreeCall.get(i).get(kitBatch.getProductCode().toUpperCase()).toString();
                            logger.info("ProductCode: " + kitBatch.getProductCode() + ", pricePlan Bonus: " + pricePlanBonus + ", id " + bn.getKitBatchId());
                            break;
                        }
                    }
                    if (pricePlanBonus.isEmpty()) {
                        logger.info("ProductCode: " + kitBatch.getProductCode() + ", pricePlan Bonus: is empty, continue to check..., id " + bn.getKitBatchId());
                        db.insertKitBatchRebuildHis(bn.getKitBatchId(), kitBatch.getIsdn(), kitBatch.getProductCode(), "", "", "", "", "",
                                "03", "Cannot get pricePlanBonus from productCode: " + kitBatch.getProductCode(), 0L);
                        continue;
                    }
                    AccountInfo accInfo = pro.viewAccInfo(isdn);
                    String tmpExpireTime = accInfo.getProductExpires().get(pricePlanBonus);
                    logger.info("ProductCode: " + kitBatch.getProductCode() + ", pricePlan Bonus: " + pricePlanBonus
                            + ", expireTime: " + tmpExpireTime + ", id " + bn.getKitBatchId());
                    if (tmpExpireTime != null && sdf.parse(tmpExpireTime).compareTo(sdf.parse(bn.getExpireTimeGroup())) > 0) {
                        logger.info("ExpireTime: " + tmpExpireTime + "greater than expireTimeGroup" + bn.getExpireTimeGroup()
                                + "that mean sub already renew...,isdn: " + kitBatch.getIsdn() + ",id " + bn.getKitBatchId());
                        //remove price first...
                        String resultRemove = pro.removePrice(isdn, "11205034");
                        logger.info("Result removePricePlan first: " + resultRemove
                                + ",isdn: " + kitBatch.getIsdn() + ",id " + bn.getKitBatchId());
                        //add price call group again...
                        String resultAddPriceMember = pro.addPriceV4(isdn, "11205034", "", newExpireTime);
                        if (!"0".equals(resultAddPriceMember) && !"102010228".equals(resultAddPriceMember)) {
                            logger.warn("Fail to add price 11205034 for sub on OCS " + isdn
                                    + " error_code " + resultAddPriceMember + " batchId " + bn.getKitBatchId());
                        } else {
                            logger.warn("Add price 11205034 for sub on OCS " + isdn
                                    + "successfully error_code " + resultAddPriceMember + " batchId " + bn.getKitBatchId());
                        }
                        db.insertKitBatchRebuildHis(bn.getKitBatchId(), kitBatch.getIsdn(), kitBatch.getProductCode(), pricePlanBonus, bn.getExpireTimeGroup(),
                                newExpireTime, resultRemove, resultAddPriceMember,
                                "0", "Successfully", actionAuditId);
                        isSuccess = true;
                    } else {
                        logger.info("Subscriber not yet renew, countProcess increase and check again, isdn: " + kitBatch.getIsdn() + ",id " + bn.getKitBatchId());
                        db.insertKitBatchRebuildHis(bn.getKitBatchId(), kitBatch.getIsdn(), kitBatch.getProductCode(), pricePlanBonus, bn.getExpireTimeGroup(), "", "", "",
                                "05", "Subscriber not yet renew, expireTime of pricePlanBonus: " + tmpExpireTime, 0L);
                        continue;
                    }
                }
                if (isSuccess) {
                    logger.info("Rebuild group successfully, check more detail on kit_batch_rebuild_his, id " + bn.getKitBatchId());
                    db.insertActionAudit(actionAuditId, bn.getKitBatchId(), "Rebuild KitBatch successfully.");
                    Calendar newCal = Calendar.getInstance();
                    newCal.setTime(sdf.parse(newExpireTime));
                    newCal.add(Calendar.DATE, -1);
                    String tmpNewExpireTime = sdf.format(newCal.getTime());
                    db.updateExpireTimeGroup(bn.getKitBatchId(), tmpNewExpireTime);
                } else {
                    logger.info("Rebuild group unsuccessfully, increase countProcess..., id " + bn.getKitBatchId());
                }
            } else {
                logger.warn("After validate respone code is fail "
                        + " id " + bn.getKitBatchId() + " staffCode: " + bn.getCreateUser()
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
                append("|\tKIT_BATCH_ID|").
                append("|\tCREATE_USER|").
                append("|\tCREATE_TIME|").
                append("|\tUNIT_CODE\t|").
                append("|\tCUST_ID\t|").
                append("|\tPAY_TYPE\t|").
                append("|\tBANK_NAME\t|").
                append("|\tBANK_TRAN_CODE\t|").
                append("|\tBANK_TRAN_AMOUNT\t|").
                append("|\tEMOLA_ACCOUNT\t|").
                append("|\tEMOLA_VOUCHER_CODE\t|").
                append("|\tADD_MONTH\t|");
        for (Record record : listRecord) {
            KitBatchInfo bn = (KitBatchInfo) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getKitBatchId()).
                    append("||\t").
                    append(bn.getCreateUser()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
                    append("||\t").
                    append(bn.getUnitCode()).
                    append("||\t").
                    append(bn.getCustId()).
                    append("||\t").
                    append((bn.getPayType())).
                    append("||\t").
                    append(bn.getBankName() != null ? bn.getBankName() : "").
                    append("||\t").
                    append(bn.getBankTransCode() != null ? bn.getBankTransCode() : "").
                    append("||\t").
                    append(bn.getBankTransAmount() != null ? bn.getBankTransAmount() : "").
                    append("||\t").
                    append(bn.getEmolaAccount() != null ? bn.getEmolaAccount() : "").
                    append("||\t").
                    append(bn.getEmolaVoucherCode() != null ? bn.getEmolaVoucherCode() : "").
                    append("||\t").
                    append(bn.getAddMonth() != null ? bn.getAddMonth() : "");
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
