/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbBonusConnectKit;
import com.viettel.paybonus.obj.Agent;
import com.viettel.paybonus.obj.BonusConnectKit;
import com.viettel.paybonus.obj.Comission;
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
import java.util.concurrent.TimeUnit;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class PayBonusConnectKit extends ProcessRecordAbstract {

    Exchange pro;
    DbBonusConnectKit db;
    String comissionConnectSuccessfully;
    String comissionForOwner;
    String eliteMsgProductMap;
    String[] arrEliteMsgProductMap;
    ArrayList<HashMap> lstEliteMsgProductMap;
    String agentEmola;
    String bonusBrDirector;
    String[] arrBonusBrDirector;
    ArrayList<HashMap> lstBonusBrDirector;

    public PayBonusConnectKit() {
        super();
        logger = Logger.getLogger(PayBonusConnectKit.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbBonusConnectKit();
        comissionConnectSuccessfully = ResourceBundle.getBundle("configPayBonus").getString("comissionConnectSuccessfully");
        comissionForOwner = ResourceBundle.getBundle("configPayBonus").getString("comissionForOwner");
        agentEmola = ResourceBundle.getBundle("configPayBonus").getString("KitConnectSpecialAgentEmola");
        eliteMsgProductMap = ResourceBundle.getBundle("configPayBonus").getString("eliteMsgProductMap");
        arrEliteMsgProductMap = eliteMsgProductMap.split("\\|");
        lstEliteMsgProductMap = new ArrayList<HashMap>();
        for (String tmp : arrEliteMsgProductMap) {
            String[] arrTmp = tmp.split("\\:");
            HashMap map = new HashMap();
            map.put(arrTmp[0].trim(), arrTmp[1].trim());
            lstEliteMsgProductMap.add(map);
        }
        bonusBrDirector = ResourceBundle.getBundle("configPayBonus").getString("bonusBrDirector");
        arrBonusBrDirector = bonusBrDirector.split("\\|");
        lstBonusBrDirector = new ArrayList<HashMap>();
        for (String tmp : arrBonusBrDirector) {
            String[] arrTmp = tmp.split("\\:");
            HashMap map = new HashMap();
            map.put(arrTmp[0], arrTmp[1]);
            lstBonusBrDirector.add(map);
        }
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            BonusConnectKit moRecord = (BonusConnectKit) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        List<BonusConnectKit> listUpdate = new ArrayList<BonusConnectKit>();
        long timeSt;
        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
        long days;
        String tmpProduct;

        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            BonusConnectKit bn = (BonusConnectKit) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
//                Check over 30 day from create time
                long diff = new Date().getTime() - bn.getCreateTime().getTime();
                days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                days++; //add one more day
                if (days > 7) {
                    logger.warn("Over 7 days sub " + bn.getIsdn()
                            + " not yet active so ignore now id " + bn.getId());
                    bn.setResultCode("E1");
                    bn.setDescription("Over 7 days sub not yet active");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.deleteBonusConnectKit(bn);
                    db.insertBonusConnectKitHis(bn);
                    continue;
                }
//                Check to make sure not yet processing this record, not duplicate process for each record (base on action_audit_id)
//                logger.info("Start check duplicate process actionId " + bn.getActionAuditId()
//                        + " id " + bn.getId() + " isdn: " + bn.getIsdn());
                if (db.checkAlreadyProcessRecord(bn.getActionAuditId())) {
                    logger.warn("Already process record actionId " + bn.getActionAuditId());
                    bn.setResultCode("E2");
                    bn.setDescription("Already process record");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    db.deleteBonusConnectKit(bn);
                    db.insertBonusConnectKitHis(bn);
                    continue;
                }
                tmpProduct = "";
                for (int i = 0; i < lstEliteMsgProductMap.size(); i++) {
                    if (lstEliteMsgProductMap.get(i).containsKey(bn.getProductCode())) {
                        tmpProduct = lstEliteMsgProductMap.get(i).get(bn.getProductCode()).toString();
                        logger.info("Product will be replace: " + tmpProduct + ", newProduct: " + bn.getProductCode());
                        break;
                    }
                }
                if (tmpProduct.isEmpty()) {
                    tmpProduct = bn.getProductCode().toUpperCase();
                }
                if (!db.checkCorrectProfile(bn.getIsdn())) {
                    logger.warn("Not correct profile so not pay bonus connect kit for actionId " + bn.getActionAuditId()
                            + " id " + bn.getId() + " isdn: " + bn.getIsdn());
                    bn.setCountProcess(bn.getCountProcess() + 1);
                    listUpdate.add(bn);
                    continue;
                }
//                Check active and not pay for first time                
                String activeStatus = pro.checkActiveStatusOnOCS(bn.getIsdn());
                if (activeStatus == null || "1".equals(activeStatus.trim()) || "5".equals(activeStatus.trim()) || "-1".equals(activeStatus.trim())) {
                    logger.warn("Not active so not pay bonus connect kit for actionId " + bn.getActionAuditId()
                            + " id " + bn.getId() + " isdn: " + bn.getIsdn());
                    bn.setCountProcess(bn.getCountProcess() + 1);
                    listUpdate.add(bn);
                    continue;
                } else {
//                    LinhNBV 20190416: addMoney for customer have prepaidMonth...
                    if (bn.getBonusForCustomer() > 0) {
                        logger.warn("start to pay bonus for customer value: " + bn.getBonusForCustomer() + ", actionId " + bn.getActionAuditId()
                                + " id " + bn.getId() + " isdn: " + bn.getIsdn());
                        String eWalletResponse = pro.callEwalletV2(bn.getActionAuditId(), bn.getChannelTypeId(),
                                bn.getIsdn(), (long) bn.getBonusForCustomer(),
                                bn.getActionCode(), bn.getCreateStaff(), sdf.format(new Date()), 6, db);//6 means bonus for customer: cost
                        if ("01".equals(eWalletResponse)) {
                            logger.info("Pay Bonus for Customer successfully for actionId " + bn.getActionAuditId() + " isdnEmola "
                                    + bn.getIsdn() + " amount " + bn.getBonusForCustomer());
                            db.sendSms(bn.getIsdn(), "Recebeu desconto de " + bn.getBonusForCustomer() + " MT via e-Mola por registar o servico Elite pre-pago. Por favor, digite *898# para verificar o seu saldo. Obrigado!", "86904");
                        } else {
                            logger.warn("Pay Bonus for Customer unsuccessfully for actionId " + bn.getActionAuditId() + " isdnEmola "
                                    + bn.getIsdn() + " amount " + bn.getBonusForCustomer() + ", errorCode: " + eWalletResponse);
                        }
                    } else {
                        logger.info("Don't have value bonusForCustomer and handsetSerial, actionId " + bn.getActionAuditId()
                                + " id " + bn.getId() + " isdn: " + bn.getIsdn());
                    }
                    logger.warn("Already active so now pay bonus connect kit for actionId " + bn.getActionAuditId()
                            + " id " + bn.getId() + " isdn: " + bn.getIsdn());
//                    Check connect time between 25/03 and 25/4 then send sms to say thank you for supporting victim in SOF                      
//                    Date begin = sdf.parse("26032019000000000");
//                    Date end = sdf.parse("27042019000000000");
//                    if (bn.getCreateTime().before(end) && bn.getCreateTime().after(begin) && !"DATA_SIM".equals(bn.getProductCode())) {
//                        db.sendSms(bn.getIsdn(), "Obrigado por participar do Programa da Movitel de Apoio as Vitimas do Ciclone IDAI. A sua doacao: 50 MT. Para detalhes acesse o nosso Website ou Pagina Facebook", "86904");
//                    }
                    //Calculate bonus.
                    Comission comissionVas = null;
                    Comission comission = db.getComissionStaff(bn.getCreateStaff(), bn.getProductCode(), bn.getIsdn());
                    String vasCode = db.getVasCode(bn.getSubProfileId());
                    if (comission == null) {
                        if (vasCode != null && !vasCode.isEmpty()) {
                            logger.info("don't have comission for product, start getComission for vasCode: " + vasCode + ", isdn: " + bn.getIsdn());
                            comission = db.getComissionStaff(bn.getCreateStaff(), vasCode, bn.getIsdn());
                        }
                    } else {
                        if (vasCode != null && !vasCode.isEmpty()) {
                            logger.info("have comission for product, start getComission for vasCode: " + vasCode + ", isdn: " + bn.getIsdn());
                            comissionVas = db.getComissionStaff(bn.getCreateStaff(), vasCode, bn.getIsdn());
                        }
                    }
                    if (comission != null) {
//                        if (comission.getMoneyBrDirector() > 0) {
//                            String shopPath = db.getShopPathByStaffCode(bn.getCreateStaff());
//                            if (shopPath.length() > 0) {
//                                String[] arrShopPath = shopPath.split("\\_");//_7282_shopConfig_shopChildren
//                                String shopId = "";
//                                if (arrShopPath.length > 2) {
//                                    shopId = arrShopPath[2];
//                                } else {
//                                    shopId = arrShopPath[1];
//                                }
//                                String shopCode = db.getShopCodeByShopId(shopId);
//                                if (shopCode.length() > 0) {
//                                    for (int i = 0; i < lstBonusBrDirector.size(); i++) {
//                                        if (lstBonusBrDirector.get(i).containsKey(shopCode)) {
//                                            String isdnWalletBr = lstBonusBrDirector.get(i).get(shopCode).toString();
//                                            String eWalletResponse = pro.callEwalletV2(bn.getActionAuditId(), bn.getChannelTypeId(),
//                                                    isdnWalletBr, comission.getMoneyBrDirector(),
//                                                    bn.getActionCode(), bn.getCreateStaff(), sdf.format(new Date()), 6,db);
//                                            if (!"01".equals(eWalletResponse)) {
//                                                logger.info("Pay Bonus fail for branch director amount: " + comission.getMoneyBrDirector()
//                                                        + " isdnEmola " + isdnWalletBr + " errCode " + eWalletResponse + ", BR: " + shopCode);
//                                            } else {
//                                                logger.info("Pay Bonus success for branch director amount: " + comission.getMoneyBrDirector()
//                                                        + " isdnEmola " + isdnWalletBr + " errCode " + eWalletResponse + ", BR: " + shopCode);
//                                            }
//                                        }
//                                    }
//                                } else {
//                                    logger.info("Cannot get shopCode, staffCode: " + bn.getCreateStaff() + ", cannot payMoney for Br Director"
//                                            + " shopId " + shopId + " isdn: " + bn.getIsdn());
//                                }
//                            } else {
//                                logger.info("Cannot get shopPath, staffCode: " + bn.getCreateStaff() + ", cannot payMoney for Br Director"
//                                        + " id " + bn.getId() + " isdn: " + bn.getIsdn());
//                            }
//
//                        }
                        Agent staffInfo = db.getAgentInfoByUser(bn.getCreateStaff());
                        if (staffInfo.getIsdnWallet() != null && staffInfo.getIsdnWallet().length() > 0) {
                            //Step 1. Check Movitel Staff
                            if (!db.checkMovitelStaff(bn.getCreateStaff())) {
                                //Channel here
                                long valueBonusChannel = 0;
                                long valueBonusOwner = 0;
                                String ownerStaffCode = db.getOwnerStaffOfChannel(bn.getCreateStaff());
//                                Check belong S99_VIP_AGENT
                                if (ownerStaffCode != null && agentEmola.contains(ownerStaffCode)) {
                                    logger.info(bn.getCreateStaff() + " is special Enterprise Agent, so get bonus value base on BONUS_AGENT_STAFF");
                                    String strBonusChannel = comission.getBonusAgentStaff();
                                    String strBonusOwner = comission.getBonusAgentOwner();
                                    if (strBonusChannel != null && strBonusOwner != null) {
                                        String[] arrBonusChannel = strBonusChannel.split("\\|");
                                        String[] arrBonusOwner = strBonusOwner.split("\\|");
                                        ArrayList<HashMap> lstMapBonusChannel = new ArrayList<HashMap>();
                                        ArrayList<HashMap> lstMapBonusOwner = new ArrayList<HashMap>();
                                        for (String channelAmount : arrBonusChannel) {
                                            String[] tmp = channelAmount.split("\\:");
                                            HashMap map = new HashMap();
                                            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
                                            lstMapBonusChannel.add(map);
                                        }
                                        for (String ownerAmount : arrBonusOwner) {
                                            String[] tmp = ownerAmount.split("\\:");
                                            HashMap map = new HashMap();
                                            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
                                            lstMapBonusOwner.add(map);
                                        }
                                        for (int i = 0; i < lstMapBonusChannel.size(); i++) {
                                            if (lstMapBonusChannel.get(i).containsKey(ownerStaffCode)) {
                                                valueBonusChannel = Long.valueOf(lstMapBonusChannel.get(i).get(ownerStaffCode).toString())
                                                        + Math.round(0.9 * bn.getBonusForPrepaid());
                                                if (comissionVas != null) {
                                                    logger.info("partner connect product with vas, add more comission for vasCode: " + vasCode
                                                            + ", isdn: " + bn.getIsdn() + ", comissionChannel: " + comissionVas.getBonusChannel());
                                                    valueBonusChannel += comissionVas.getBonusChannel();
                                                }
                                                break;
                                            }
                                        }
                                        for (int i = 0; i < lstMapBonusOwner.size(); i++) {
                                            if (lstMapBonusOwner.get(i).containsKey(ownerStaffCode)) {
                                                valueBonusOwner = Long.valueOf(lstMapBonusOwner.get(i).get(ownerStaffCode).toString())
                                                        + Math.round(0.1 * bn.getBonusForPrepaid());
                                                if (comissionVas != null) {
                                                    logger.info("partner connect product with vas, add more comission for vasCode: " + vasCode
                                                            + ", isdn: " + bn.getIsdn() + ", bonusOwner: " + comissionVas.getBonusChannelCtv());
                                                    valueBonusOwner += comissionVas.getBonusChannelCtv();
                                                }
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    valueBonusChannel = comission.getBonusChannel() + Math.round(0.9 * bn.getBonusForPrepaid());
                                    valueBonusOwner = comission.getBonusChannelCtv() + Math.round(0.1 * bn.getBonusForPrepaid());
                                    if (comissionVas != null) {
                                        logger.info("channel connect product with vas, add more comission for vasCode: " + vasCode
                                                + ", isdn: " + bn.getIsdn() + ", comissionChannel: " + comissionVas.getBonusChannel() + ", bonusOwner: " + comissionVas.getBonusChannelCtv());
                                        valueBonusChannel += comissionVas.getBonusChannel();
                                        valueBonusOwner += comissionVas.getBonusChannelCtv();
                                    }
                                }
                                String eWalletResponse = pro.callEwalletV2(bn.getActionAuditId(), bn.getChannelTypeId(),
                                        staffInfo.getIsdnWallet(), valueBonusChannel,
                                        bn.getActionCode(), bn.getCreateStaff(), sdf.format(new Date()), 4, db);
                                bn.seteWalletErrCode(eWalletResponse);
                                if ("01".equals(eWalletResponse)) {
                                    logger.info("Pay Bonus for Channel success for actionId " + bn.getActionAuditId() + " isdnEmola "
                                            + staffInfo.getIsdnWallet() + " amount " + valueBonusChannel + " isdn " + bn.getIsdn());
                                    String msgBonus = comissionConnectSuccessfully.replace("%PCKG%", tmpProduct).
                                            replace("%SERIAL%", bn.getSerial()).replace("%PHONE%",
                                            bn.getIsdn()).replace("%BONUS%", valueBonusChannel + "");
                                    String telChannel = db.getTelByStaffCode(bn.getCreateStaff());
                                    db.sendSms(telChannel, msgBonus, "86142");
                                    logger.info("Message will send to channel isdnWallet " + staffInfo.getIsdnWallet() + " msg: "
                                            + msgBonus + " isdnCus " + bn.getIsdn());
                                    //Get owner staff
//                                    String ownerStaffCode = db.getOwnerStaffOfChannel(bn.getCreateStaff());
                                    if (ownerStaffCode != null) {
                                        Agent staffOwner = db.getAgentInfoByUser(ownerStaffCode);
                                        if (staffOwner.getIsdnWallet() != null && staffOwner.getIsdnWallet().length() > 0) {
                                            eWalletResponse = pro.callEwalletV2(bn.getActionAuditId(), bn.getChannelTypeId(), staffOwner.getIsdnWallet(),
                                                    valueBonusOwner, bn.getActionCode(), ownerStaffCode, sdf.format(new Date()), 4, db);
                                            if ("01".equals(eWalletResponse)) {
                                                logger.info("Pay Bonus success for actionId " + bn.getActionAuditId() + " isdnEmola "
                                                        + staffOwner.getIsdnWallet() + " amount " + valueBonusOwner
                                                        + " staff " + ownerStaffCode + " owner of " + bn.getCreateStaff()
                                                        + " isdn " + bn.getIsdn());
                                                String telOwner = db.getTelByStaffCode(ownerStaffCode);
                                                String msgBonusOwner = comissionForOwner.replace("%CHANNEL%", bn.getCreateStaff()).
                                                        replace("%PCKG%", bn.getProductCode()).replace("%BONUS%", valueBonusOwner + "")
                                                        .replace("%SERIAL%", bn.getSerial());
                                                db.sendSms(telOwner, msgBonusOwner, "86142");
                                                logger.info("Message send to owner: " + msgBonusOwner + " isdnCus " + bn.getIsdn());
                                                bn.setResultCode("0");
                                                bn.setDescription("Pay bonus success for channel " + bn.getCreateStaff() + " and Owner " + ownerStaffCode);
                                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                            } else {
                                                logger.info("Fail to add Emola staff " + ownerStaffCode + " owner of " + bn.getCreateStaff()
                                                        + " isdn " + bn.getIsdn());
                                                bn.setResultCode("0");
                                                bn.setDescription("Pay bonus success for channel " + bn.getCreateStaff() + " but fail to pay bonus to Owner");
                                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                            }
                                        } else {
                                            logger.info("Don't have isdn wallet of staff " + ownerStaffCode + " owner of " + bn.getCreateStaff()
                                                    + " isdn " + bn.getIsdn());
                                            bn.setResultCode("0");
                                            bn.setDescription("Pay bonus success for channel " + bn.getCreateStaff() + " but don't have isdn wallet of Owner");
                                            bn.setDuration(System.currentTimeMillis() - timeSt);
                                        }
                                    } else {
                                        logger.warn("Can not find owner of channel " + bn.getCreateStaff() + " isdn " + bn.getIsdn());
                                        bn.setResultCode("0");
                                        bn.setDescription("Pay bonus success for channel " + bn.getCreateStaff() + " but not found Owner");
                                        bn.setDuration(System.currentTimeMillis() - timeSt);
                                    }
                                    db.deleteBonusConnectKit(bn);
                                    db.insertBonusConnectKitHis(bn);
                                    continue;
                                } else {
                                    logger.error("Pay Bonus fail for channel actionId " + bn.getActionAuditId()
                                            + " isdnEmola " + staffInfo.getIsdnWallet() + " amount " + valueBonusChannel);
                                    bn.setResultCode("E3");
                                    bn.setDescription("Pay Emola fail for channel isdnEmola " + staffInfo.getIsdnWallet() + " amount " + valueBonusChannel);
                                    bn.setDuration(System.currentTimeMillis() - timeSt);
                                    db.deleteBonusConnectKit(bn);
                                    db.insertBonusConnectKitHis(bn);
                                    continue;
                                }
                            } else {
                                long bonusStaff = comission.getBonusCtv() + bn.getBonusForPrepaid();
                                if (comissionVas != null) {
                                    logger.info("staff connect product with vas, add more comission for vasCode: " + vasCode
                                            + ", isdn: " + bn.getIsdn() + ", comission: " + comissionVas.getBonusCtv());
                                    bonusStaff += comissionVas.getBonusCtv();
                                }
                                String eWalletResponse = pro.callEwalletV2(bn.getActionAuditId(), bn.getChannelTypeId(),
                                        staffInfo.getIsdnWallet(), bonusStaff,
                                        bn.getActionCode(), bn.getCreateStaff(), sdf.format(new Date()), 4, db);
                                bn.seteWalletErrCode(eWalletResponse);
                                if ("01".equals(eWalletResponse)) {
                                    logger.info("Pay Bonus success for actionId " + bn.getActionAuditId() + " isdnEmola "
                                            + " amount " + comission.getBonusChannelCenter()
                                            + " staff " + bn.getCreateStaff()
                                            + " isdn " + bn.getIsdn());
                                    String telStaff = db.getTelByStaffCode(bn.getCreateStaff());
                                    String msgBonusStaff = comissionConnectSuccessfully.replace("%PCKG%", tmpProduct).
                                            replace("%SERIAL%", bn.getSerial()).replace("%PHONE%",
                                            bn.getIsdn()).replace("%BONUS%", bonusStaff + "");
                                    db.sendSms(telStaff, msgBonusStaff, "86142");
                                    bn.setDescription("Bonus to staff success " + bn.getCreateStaff() + " money " + bonusStaff);
                                    bn.setResultCode("0");
                                    bn.setDuration(System.currentTimeMillis() - timeSt);
                                    db.deleteBonusConnectKit(bn);
                                    db.insertBonusConnectKitHis(bn);
                                    logger.info("Bonus to staff success: " + msgBonusStaff + " isdnCus " + bn.getIsdn());
                                    continue;
                                } else {
                                    logger.info("Fail to bonus to staff " + bn.getCreateStaff() + " isdn " + bn.getIsdn());
                                    bn.setDescription("Fail to bonus to staff " + bn.getCreateStaff());
                                    bn.setResultCode("E4");
                                    bn.setDuration(System.currentTimeMillis() - timeSt);
                                    db.deleteBonusConnectKit(bn);
                                    db.insertBonusConnectKitHis(bn);
                                    continue;
                                }
                            }
                        } else {
                            logger.warn(bn.getCreateStaff() + " Don't have isdn wallet, cannot pay comission " + bn.getActionAuditId());
                            bn.setDescription("Don't have isdn wallet " + bn.getCreateStaff());
                            bn.setResultCode("E5");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            db.deleteBonusConnectKit(bn);
                            db.insertBonusConnectKitHis(bn);
                            continue;
                        }
                    } else {
                        //Product not VIP, not receive bonus
                        logger.info("Don't pay comission because product not VIP.");
                        bn.setDescription("Don't pay comission because product not VIP.");
                        bn.setResultCode("E6");
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        db.deleteBonusConnectKit(bn);
                        db.insertBonusConnectKitHis(bn);
                        continue;
                    }
                }
            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getActionAuditId()
                        + " id " + bn.getId() + " isdn: " + bn.getIsdn()
                        + " so continue with other transaction");
                bn.setDescription("After validate respone code is fail");
                bn.setDuration(System.currentTimeMillis() - timeSt);
                db.deleteBonusConnectKit(bn);
                db.insertBonusConnectKitHis(bn);
                continue;
            }
        }
        db.updateBonusConectKit(listUpdate);
        listRecord.clear();
        Thread.sleep(1000 * 60 * 1);
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tACTION_AUDIT_ID|").
                append("|\tisdn\t|").
                append("|\tcreate_time\t|").
                append("|\tcreate_staff\t|").
                append("|\tproductcode\t|").
                append("|\tsim_serial\t|").
                append("|\tcount_process\t|").
                append("|\tlast_process\t|");
        for (Record record : listRecord) {
            BonusConnectKit bn = (BonusConnectKit) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdf.format(bn.getCreateTime()) : null)).
                    append("||\t").
                    append(bn.getCreateStaff()).
                    append("||\t").
                    append(bn.getProductCode()).
                    append("||\t").
                    append(bn.getSerial()).
                    append("||\t").
                    append(bn.getCountProcess()).
                    append("||\t").
                    append((bn.getLastProcess() != null ? sdf.format(bn.getLastProcess()) : null));
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

    public static void main(String[] args) {
        String result = "-1";
        if (result == null || result.trim().equals("1") || result.trim().equals("5") || result.trim().equals("-1")) {
            System.out.println("One");
        } else {
            System.out.println("Second");
        }
    }
}
