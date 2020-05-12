/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.emola.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.Config;
import com.viettel.paybonus.obj.EmolaPromotion;
import com.viettel.paybonus.obj.EmolaPromotionPackage;
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
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class EmolaServiceBaseBtsScaner extends ProcessRecordAbstract {

    Exchange pro;
    EmolaDbProcessor db;
    String countryCode;
    List<Config> lstConfig;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String emolaPromSeq = "emola_promotion_seq";
    String emolaPromDB = "dbEmolaPromotion";
    String emolaPromPrefix = "PROM_1";

    public EmolaServiceBaseBtsScaner() {
        super();
        logger = Logger.getLogger(EmolaServiceBaseBtsScaner.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
        //pro = new Exchange(ExchangeClientChannel.getInstance("D:\\1.Movitel\\1.BCCS_SVN\\ThreadContainer\\etc\\exchange_client.cfg").getInstanceChannel(), logger);
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new EmolaDbProcessor();
        if (Config.listMessage == null || Config.listMessage.isEmpty()) {
            Config.listMessage = db.getEmolaConfig();
        }
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            EmolaPromotion moRecord = (EmolaPromotion) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        for (Record record : listRecord) {
            EmolaPromotion bn = (EmolaPromotion) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {

//                Step 1: Check in the time of promotion
                Date now = new Date();
                String expireTime = Config.getConfig(Config.expireTime, logger);
                if (expireTime != null && !expireTime.isEmpty() && now.after(sdf.parse(expireTime))) {
                    logger.info("Already expire time for promotion " + bn.getMobile());
                    bn.setResultCode("E1");
                    bn.setDescription("Already expire time");
                    continue;
                }
//                Step 2: Check already have promotion code for this invoice_id
                if (db.checkAlreadyHaveCode(bn.getMobile(), bn.getInvoiceId())) {
                    logger.info("This trans invoiceId " + bn.getInvoiceId() + " already have promotion code " + bn.getMobile());
                    bn.setResultCode("E2");
                    bn.setDescription("This trans already got promotion code");
                    continue;
                }
//                Step 3: Check BTS in list support promotion
                String misdn = bn.getMobile();
                if (!misdn.startsWith("258")) misdn = "258" + misdn;
                String mscNumChannel = pro.getMSCInfor(misdn, "");
                if (mscNumChannel.trim().length() <= 0) {
                    logger.warn("Can not get mscNumChannel for channel with ISDN= " + bn.getMobile());
                    bn.setResultCode("E3");
                    bn.setDescription("Can not get mscNumChannel to determine which BTS for support promotion");
                    continue;
                } else {
                    String cellIdChannel = pro.getCellIdRsString(bn.getMobile(), mscNumChannel, "");
                    if (cellIdChannel.trim().length() <= 0) {
                        logger.warn("Can not get cellIdChannel with mscNumChannel " + mscNumChannel + " mobile " + bn.getMobile());
                        bn.setResultCode("E4");
                        bn.setDescription("Can not get cellIdChannel to determine which BTS for support promotion");
                        continue;
                    } else {
                        String[] arrCellId = cellIdChannel.split("\\|");
                        if ((arrCellId != null) && (arrCellId.length == 2)) {
//                            String cellCodeChannel = db.getCell("", arrCellId[0].trim(), arrCellId[1].trim());
                            String btsCodeChannel = db.getBts("", arrCellId[0].trim(), arrCellId[1].trim());
                            bn.setBtsReg(btsCodeChannel);
                            if (btsCodeChannel.trim().length() <= 0) {
                                logger.warn("Can not map cell and lac with BTS " + bn.getMobile());
                                bn.setResultCode("E5");
                                bn.setDescription("Can not map cell and lac with BTS");
                                continue;
                            } else {
                                String listBts = Config.getConfig(Config.listBts, logger);
                                if (listBts != null && !listBts.isEmpty() && !listBts.trim().toUpperCase().contains(btsCodeChannel)) {
                                    logger.warn("Not belong list BTS support promotion " + bn.getMobile() + " current BTS attached " + btsCodeChannel);
                                    bn.setResultCode("E7");
                                    bn.setDescription("Not belong list BTS support promotion" + listBts + ", current BTS attached " + btsCodeChannel);
                                    continue;
                                }
                            }
                        } else {
                            logger.warn("Invalid cellIdChannel " + bn.getMobile());
                            bn.setResultCode("E6");
                            bn.setDescription("Invalid cellIdChannel so can not get BTS");
                            continue;
                        }
                    }
                }
//                Step 4: Check special promotion
                Long ranking = db.getPromRankingOfTrans(1);
                if (ranking == null) {
                    logger.warn("Can not get ranking: " + bn.getMobile() + " invoice " + bn.getInvoiceId());
                    bn.setResultCode("E8");
                    bn.setDescription("Can not get ranking: " + bn.getMobile() + " invoice " + bn.getInvoiceId());
                    continue;
                }
                
                EmolaPromotionPackage promPackage = getPromotionPackage(ranking);
                if (promPackage == null) {
                    logger.warn("Can not get promotion package: " + bn.getMobile() + " invoice " + bn.getInvoiceId() + " ranking " + ranking);
                    bn.setResultCode("E9");
                    bn.setDescription("Can not get promotion package: " + bn.getMobile() + " invoice " + bn.getInvoiceId() + " ranking " + ranking);
                    continue;
                }
                bn.setId(ranking);
                bn.setPromotionType(promPackage.getPromotionType());
                //Decrease remain TV or Phone on group
                decreaseRemainGift(promPackage.getGift());
                
//                Step 5: Generate promotion code
                String promotionCode = db.getPromotionCode(ranking, emolaPromPrefix);
                bn.setPromotionCode(promotionCode);
                
                String msg = getPromotionMessage(bn);
                bn.setMessage(msg);
                
                int rsInsert = db.insertEmolaPromotion(bn);
                if (rsInsert <= 0) {
                    logger.warn("Fail to insert EmolaPromotion " + bn.getMobile()+ "" + bn.getInvoiceId()+ " errcode rsInsert " + rsInsert);
                    bn.setResultCode("E10");
                    bn.setDescription("Can not insert EmolaPromotion: " + bn.getMobile() + " invoice " + bn.getInvoiceId());
                    continue;
                }
//                Step 6: Build response sms
                String smsChannelCode = Config.getConfig(Config.smsChannelCode, logger);
                if (smsChannelCode == null || "".equals(smsChannelCode)) {
                    smsChannelCode = "86904";
                }
                db.sendSms(bn.getMobile(), bn.getMessage(), smsChannelCode);
                
//                Step 7: Write log and finish
            } else {
                logger.warn("After validate respone code is fail id " + bn.getInvoiceId()
                        + " so continue with other transaction");
                continue;
            }
        }
        listRecord.clear();
        Thread.sleep(60000);
        return listResult;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdfs = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tINVOICE_ID|").
                append("|\tQUANTITY\t|").
                append("|\tMOBILE\t|").
                append("|\tSERVICE_ID\t|").
                append("|\tCREATE_TIME\t|");
        for (Record record : listRecord) {
            EmolaPromotion bn = (EmolaPromotion) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getInvoiceId()).
                    append("||\t").
                    append(bn.getQuantity()).
                    append("||\t").
                    append(bn.getMobile()).
                    append("||\t").
                    append(bn.getServiceId()).
                    append("||\t").
                    append((bn.getCreateTime() != null ? sdfs.format(bn.getCreateTime()) : null));
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
//        logger.warn("TEMPLATE process exception record: " + ex.toString());
//        for (Record record : listRecord) {
//            logger.info("TEMPLATE let convert to recort type you want and then set errCode, errDesc at here");
////            MoRecord moRecord = (MoRecord) record;
////            moRecord.setMessage("Thao tac that bai!");
////            moRecord.setErrCode("-5");
//        }
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
    
    public EmolaPromotionPackage getPromotionPackage(long ranking) {
        EmolaPromotionPackage promPackage = null;
        StringBuilder brBuilder = new StringBuilder();
        long startTime = System.currentTimeMillis();
        try {
            String promPolicy = Config.getConfig(Config.promPolicy, logger);
            String promPolicySpec = Config.getConfig(Config.promPolicySpec, logger);
            String promPolicyMultiple = Config.getConfig(Config.promPolicyMultiple, logger);
            String[] arrPolicySpec = promPolicySpec.split(",");
            Map<Long, EmolaPromotionPackage> mapRankingProm = new HashMap<Long, EmolaPromotionPackage>();
            //CHECK POLICY SPECIAL
            for (String pps : arrPolicySpec) {
                String[] detailProm = pps.split("\\:");
                if (detailProm.length > 2) {
                    Long dRanking = Long.parseLong(detailProm[0]);
                    int promType = Integer.parseInt(detailProm[1]);
                    String gift = detailProm[2];

                    EmolaPromotionPackage promPk = new EmolaPromotionPackage();
                    promPk.setRanking(dRanking);
                    promPk.setPromotionType(promType);
                    promPk.setGift(gift);
                    mapRankingProm.put(dRanking, promPk);
                }
            }

            promPackage = mapRankingProm.get(ranking);
            if (promPackage != null) {
                logger.info("getPromotionPackage ranking " + ranking + " has special policy result " + promPackage.toString() + " time "
                    + (System.currentTimeMillis() - startTime));
                return promPackage;
            }

            String[] arrPolicyMultiple = promPolicyMultiple.split(",");
            for (String pps : arrPolicyMultiple) {
                String[] detailProm = pps.split("\\:");
                if (detailProm.length > 2) {
                    Double price = Double.parseDouble(detailProm[0]);
                    int promType = Integer.parseInt(detailProm[1]);
                    String gift = detailProm[2];

                    if (ranking % price == 0) {
                        promPackage = new EmolaPromotionPackage();
                        promPackage.setRanking(ranking);
                        promPackage.setPromotionType(promType);
                        promPackage.setGift(gift);
                        break;
                    }
                }
            }       

            if (promPackage == null) {
                String[] detailProm = promPolicy.split("\\:");
                if (detailProm.length > 1) {
                    Long dBonusFloat = Long.parseLong(detailProm[0]);
                    int promType = Integer.parseInt(detailProm[1]);

                    promPackage = new EmolaPromotionPackage();
                    promPackage.setBonusFloat(dBonusFloat);
                    promPackage.setRanking(ranking);
                    promPackage.setPromotionType(promType);
                }
            }

            logger.info("getPromotionPackage ranking " + ranking + " result " + promPackage.toString() + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(new Date()).
                    append("\nERROR getPromotionPackage: ")
                    .append(" Ranking ")
                    .append(ranking)
                    .append(" result ")
                    .append(promPackage);
            logger.error(brBuilder + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        }
        
        return promPackage;
    }
    
    public String getPromotionMessage(EmolaPromotion emolaProm) {
        String promotionMessage = null;
        StringBuilder brBuilder = new StringBuilder();
        long startTime = System.currentTimeMillis();
        String expireTime = Config.getConfig(Config.expireTime, logger);
        try {
            //Get plan date
            Date currentDate = emolaProm.getCreateTime();
            Calendar c = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
            c.setTime(currentDate);
            int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
            if (dayOfWeek == 7) {
                c.add(Calendar.DATE, dayOfWeek);
            }
            else {
                c.add(Calendar.DATE, 7 - dayOfWeek);
            }
            String planDate = sdf.format(c.getTime());
            Date expireDate = new SimpleDateFormat("yyyyMMddHHmmss").parse(expireTime);
            c.setTime(expireDate);
            String strExpireDate = sdf.format(c.getTime());
            
            
            //Get sms by promotion type
            promotionMessage = Config.getConfig(Config.promSms + emolaProm.getPromotionType(), logger);
            String remainTV = db.getEmolaConfig(Config.remainTV + emolaPromPrefix);
            String remainFloat = db.getEmolaConfig(Config.remainFloat + emolaPromPrefix);
            String remainPhone = db.getEmolaConfig(Config.remainPhone + emolaPromPrefix);
            
            //Format SMS [RANKING], [EMOLA_NUMBER], [EXPIRE_DATE], [PLAN_DATE],[PROMOTION_CODE] 
            promotionMessage = promotionMessage.replace("[RANKING]", Long.toString(emolaProm.getId()));
            promotionMessage = promotionMessage.replace("[EMOLA_NUMBER]", emolaProm.getMobile());
            promotionMessage = promotionMessage.replace("[PLAN_DATE]", planDate);
            promotionMessage = promotionMessage.replace("[EXPIRE_DATE]", strExpireDate);
            promotionMessage = promotionMessage.replace("[PROMOTION_CODE]", emolaProm.getPromotionCode());
            promotionMessage = promotionMessage.replace("[TV]", remainTV);
            promotionMessage = promotionMessage.replace("[PHONE]", remainPhone);
            promotionMessage = promotionMessage.replace("[FLOAT]", remainFloat);
            
            logger.info("getPromotionMessage mobile " + emolaProm.getMobile()+ " result " + promotionMessage + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(new Date()).
                    append("\nERROR getPromotionMessage: ")
                    .append(" promotionType ")
                    .append(emolaProm.getPromotionType())
                    .append(" result ")
                    .append(promotionMessage);
            logger.error(brBuilder + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        }
        
        return promotionMessage;
    }
    
    public void decreaseRemainGift(String gift) {
        StringBuilder brBuilder = new StringBuilder();
        long startTime = System.currentTimeMillis();
        String remainTV = db.getEmolaConfig(Config.remainTV + emolaPromPrefix);
        String remainPhone = db.getEmolaConfig(Config.remainPhone + emolaPromPrefix);
        String remainFloat = db.getEmolaConfig(Config.remainFloat + emolaPromPrefix);
        int actionResult = 0;
        try {
            Integer remainTVQt = Integer.parseInt(remainTV);
            Integer remainPhoneQt = Integer.parseInt(remainPhone);
            Integer remainFloatQt = Integer.parseInt(remainFloat);
            if (gift == null) {
                return;
            }
            else if (gift.equals("TV") && remainTVQt > 0) {
                remainTVQt--;
                actionResult = db.updateEmolaConfig(Config.remainTV + emolaPromPrefix, remainTVQt.toString());
            }
            else if (gift.equals("PHONE") && remainPhoneQt > 0) {
                remainPhoneQt--;
                actionResult = db.updateEmolaConfig(Config.remainPhone + emolaPromPrefix, remainPhoneQt.toString());
            }
            else if (gift.equals("FLOAT") && remainFloatQt > 0) {
                remainFloatQt--;
                actionResult = db.updateEmolaConfig(Config.remainFloat + emolaPromPrefix, remainFloatQt.toString());
            }
            logger.info("decreaseRemainGift gift " + gift + " result " + actionResult + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(new Date()).
                    append("\nERROR decreaseRemainGift: ")
                    .append(" gift ")
                    .append(gift)
                    .append(" result ")
                    .append(actionResult);
            logger.error(brBuilder + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        }
        
        Config.listMessage = db.getEmolaConfig();
    }

}
