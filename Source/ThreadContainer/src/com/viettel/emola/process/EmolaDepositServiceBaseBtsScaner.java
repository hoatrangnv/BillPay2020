/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.emola.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.Config;
import com.viettel.paybonus.obj.EmolaPromotion;
import com.viettel.paybonus.obj.EmolaPromotionPackage;
import com.viettel.paybonus.obj.Staff;
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
import java.util.Arrays;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class EmolaDepositServiceBaseBtsScaner extends ProcessRecordAbstract {

    Exchange pro;
    EmolaDbProcessor db;
    String countryCode;
    List<Config> lstConfig;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String emolaPromSeq = "emola_promotion_seq";
    String emolaPromDB = "dbEmolaPromotion";
    String emolaPromPrefix = "PROM_2";

    public EmolaDepositServiceBaseBtsScaner() {
        super();
        logger = Logger.getLogger(EmolaDepositServiceBaseBtsScaner.class);
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
//                Step 2: Check already have promotion code for this isdn
                if (db.checkIsdnAlreadyHaveCode(bn.getMobile())) {
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
//          
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
                Long ranking = db.getPromRankingOfTrans(3);
                if (ranking == null) {
                    logger.warn("Can not get ranking: " + bn.getMobile() + " invoice " + bn.getInvoiceId());
                    bn.setResultCode("E8");
                    bn.setDescription("Can not get ranking: " + bn.getMobile() + " invoice " + bn.getInvoiceId());
                    continue;
                }
                
                EmolaPromotionPackage promPackage = new EmolaPromotionPackage();
                Long maxRankingProm2 = Config.getLongValue(Config.maxRankingProm + 2, logger);
                if (ranking <= maxRankingProm2) {
                    promPackage = getPromotionPackage(ranking);
                }
                
                if (promPackage == null) {
                    logger.warn("Can not get promotion package: " + bn.getMobile() + " invoice " + bn.getInvoiceId() + " ranking " + ranking);
                    bn.setResultCode("E9");
                    bn.setDescription("Can not get promotion package: " + bn.getMobile() + " invoice " + bn.getInvoiceId() + " ranking " + ranking);
                    continue;
                }
                bn.setId(ranking);
                bn.setPromotionType(promPackage.getPromotionType());
                
//                Step 5: Generate promotion code
                String promotionCode = "";
                if (ranking <= maxRankingProm2) {
                    promotionCode = db.getPromotionCode(ranking, emolaPromPrefix);
                }
                bn.setPromotionCode(promotionCode);
                
                String msg = getPromotionMessage(bn);
                bn.setMessage(msg);
                
//                Check channel develop this customer
                String enablePromChannel = Config.getConfig(Config.enablePromChannel, logger);
                if ("ENABLED".equals(enablePromChannel)) {
                    bn = checkPromChannel(bn);
                }
                
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
                
//                Check Customer reach to max ranking of Promotion 2 will not sent SMS
                if (bn.getPromotionType() > 0) {
                    db.sendSms(bn.getMobile(), bn.getMessage(), smsChannelCode);
                }
                
                if (bn.getAgenthasProm()) {
                    db.sendSms(bn.getAgentWallet(), bn.getAgentMessage(), smsChannelCode);
                }
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
                append("|\tAGENT_MOBILE\t|").
                append("|\tAGENT_NAME\t|").
                append("|\tTRANS_REF\t|").
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
                    append(bn.getAgentWallet()).
                    append("||\t").
                    append(bn.getAgentName()).
                    append("||\t").
                    append(bn.getTransRef()).
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
            String promPolicy = Config.getConfig(Config.prom2Policy, logger);
            String promPolicySpec = Config.getConfig(Config.prom2PolicySpec, logger);
            String promPolicyMultiple = Config.getConfig(Config.prom2PolicyMultiple, logger);
            String[] arrPolicySpec = promPolicySpec.split(",");
            Map<Long, EmolaPromotionPackage> mapRankingProm = new HashMap<Long, EmolaPromotionPackage>();
            //CHECK POLICY SPECIAL
            for (String pps : arrPolicySpec) {
                String[] detailProm = pps.split("\\:");
                if (detailProm.length > 1) {
                    Long dRanking = Long.parseLong(detailProm[0]);
                    Long dBonusFloat = Long.parseLong(detailProm[1]);
                    int promType = Integer.parseInt(detailProm[2]);
                    String gift = detailProm[3];

                    EmolaPromotionPackage promPk = new EmolaPromotionPackage();
                    promPk.setBonusFloat(dBonusFloat);
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
                if (detailProm.length > 1) {
                    Double price = Double.parseDouble(detailProm[0]);
                    Long dBonusFloat = Long.parseLong(detailProm[1]);
                    int promType = Integer.parseInt(detailProm[2]);
                    String gift = detailProm[3];

                    if (ranking % price == 0) {
                        promPackage = new EmolaPromotionPackage();
                        promPackage.setBonusFloat(dBonusFloat);
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
    
    public String getPromotionMessageForChannel(EmolaPromotion emolaProm) {
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
            promotionMessage = Config.getConfig(Config.promSms3 + emolaProm.getAgentPromType(), logger);
            String moneyFloat = db.getEmolaConfig(Config.remainFloat + "PROM_3");
            String remainPhone3G = db.getEmolaConfig(Config.remainPhone3G + "PROM_3");
            String remainPhone4G = db.getEmolaConfig(Config.remainPhone4G + "PROM_3");

            promotionMessage = promotionMessage.replace("[PLAN_DATE]", planDate);
            promotionMessage = promotionMessage.replace("[CHANNEL_CODE]", emolaProm.getAgentChannelCode());
            promotionMessage = promotionMessage.replace("[EXPIRE_DATE]", strExpireDate);
            promotionMessage = promotionMessage.replace("[PHONE3G]", remainPhone3G);
            promotionMessage = promotionMessage.replace("[PHONE4G]", remainPhone4G);
            promotionMessage = promotionMessage.replace("[FLOAT]", moneyFloat);
            promotionMessage = promotionMessage.replace("[RANKING]", Long.toString(emolaProm.getAgentRanking()));
            
            logger.info("getPromotionMessage mobile " + emolaProm.getMobile()+ " result " + promotionMessage + " time "
                    + (System.currentTimeMillis() - startTime));
        } catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(new Date()).
                    append("\nERROR getPromotionMessage: ")
                    .append(" promotionType ")
                    .append(emolaProm.getAgentPromType())
                    .append(" result ")
                    .append(promotionMessage);
            logger.error(brBuilder + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        }
        
        return promotionMessage;
    }
        
    public String getPromotionMessage(EmolaPromotion emolaProm) {
        String promotionMessage = null;
        StringBuilder brBuilder = new StringBuilder();
        long startTime = System.currentTimeMillis();
        String expireTime = Config.getConfig(Config.expireTime, logger);
        String planHour = "16:00";
        try {
            //Get plan date
            Date currentDate = emolaProm.getCreateTime();
            Calendar c = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
            c.setTime(currentDate);
            int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
            int hourOfDay = c.get(Calendar.HOUR_OF_DAY);
            int minute = c.get(Calendar.MINUTE);
            //Saturday and sunday (11h Saturday => sunday)
            if (dayOfWeek == 7 && hourOfDay * 100 + minute >= 1100) {
                c.add(Calendar.DATE, 2);
            }
            else if ((dayOfWeek == 6 && hourOfDay * 100 + minute >= 1500) || (dayOfWeek == 7 && hourOfDay * 100 + minute < 1100)) {
                c.add(Calendar.DATE, 1);
                planHour = "12:00";
            }
            else if (dayOfWeek == 1) {
                c.add(Calendar.DATE, 1);
            }
            else if (hourOfDay * 100 + minute > 1500) {
                c.add(Calendar.DATE, 1);
            }
            
            String planDate = sdf.format(c.getTime());
            Date expireDate = new SimpleDateFormat("yyyyMMddHHmmss").parse(expireTime);
            c.setTime(expireDate);
            String strExpireDate = sdf.format(c.getTime());
            
            
            //Get sms by promotion type
            promotionMessage = Config.getConfig(Config.promSms2 + emolaProm.getPromotionType(), logger);

            promotionMessage = promotionMessage.replace("[PLAN_DATE]", planDate);
            promotionMessage = promotionMessage.replace("[PLAN_HOUR]", planHour);
            promotionMessage = promotionMessage.replace("[PROMOTION_CODE]", emolaProm.getPromotionCode());
            promotionMessage = promotionMessage.replace("[EXPIRE_DATE]", strExpireDate);
            
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
    
    public EmolaPromotion checkPromChannel(EmolaPromotion bn) throws Exception {
        String targetChannel = Config.getConfig(Config.targetChannelCode, logger);
        List<String> listChannelId = new ArrayList<String>();
        if (targetChannel != null) {
            listChannelId = Arrays.asList(targetChannel.split(","));
        }
        StringBuilder brBuilder = new StringBuilder();
        long startTime = System.currentTimeMillis();
//                Step 1: Check in the time of promotion
        Date now = new Date();
        try {
            bn.setAgenthasProm(false);
            String expireTime = Config.getConfig(Config.expireTime, logger);
            if (expireTime != null && !expireTime.isEmpty() && now.after(sdf.parse(expireTime))) {
                logger.info("Check promotion of channel already expire time for promotion " + bn.getAgentWallet());
                bn.setResultCode("E1");
                bn.setDescription("AGENT:Check promotion of channel already expire time");
                return bn;
            }
    //                Step 2: Check channel code of Agent
            Staff staff = db.getChannelInfo(bn.getAgentWallet());
            if (staff != null) {
                bn.setAgentChannelCode(staff.getStaffCode());
            }
            
            if (staff == null || !listChannelId.contains(staff.getStaffCode())) {
                logger.info("This agent " + bn.getAgentWallet()+ " isn't channel have promotion!");
                bn.setResultCode("E11");
                bn.setDescription("AGENT:This mobile sn't channel have promotion!");
                return bn;
            }
    //                Step 3: Check BTS in list support promotion
            String misdn = bn.getAgentWallet();
            if (!misdn.startsWith("258")) misdn = "258" + misdn;
            String mscNumChannel = pro.getMSCInfor(misdn, "");
            if (mscNumChannel.trim().length() <= 0) {
                logger.warn("Can not get mscNumChannel for channel with ISDN= " + bn.getMobile());
                bn.setResultCode("E3");
                bn.setDescription("AGENT:Can not get mscNumChannel to determine which BTS for support promotion");
                return bn;
            } else {
                String cellIdChannel = pro.getCellIdRsString(bn.getMobile(), mscNumChannel, "");
                if (cellIdChannel.trim().length() <= 0) {
                    logger.warn("Can not get cellIdChannel with mscNumChannel " + mscNumChannel + " mobile " + bn.getMobile());
                    bn.setResultCode("E4");
                    bn.setDescription("AGENT:Can not get cellIdChannel to determine which BTS for support promotion");
                return bn;
                } else {
                    String[] arrCellId = cellIdChannel.split("\\|");
                    if ((arrCellId != null) && (arrCellId.length == 2)) {
                        String cellCodeChannel = db.getBts("", arrCellId[0].trim(), arrCellId[1].trim());
                        if (cellCodeChannel.trim().length() <= 0) {
                            logger.warn("Can not map cell and lac with BTS " + bn.getMobile());
                            bn.setResultCode("E5");
                            bn.setDescription("AGENT:Can not map cell and lac with BTS");
                            return bn;
                        } else {
                            String listBts = Config.getConfig(Config.listBts, logger);
                            if (listBts != null && !listBts.isEmpty() && !listBts.trim().toUpperCase().contains(cellCodeChannel)) {
                                logger.warn("Agent not belong list BTS support promotion " + bn.getMobile() + " current BTS attached " + cellCodeChannel);
                                bn.setResultCode("E7");
                                bn.setDescription("AGENT:Agent not belong list BTS support promotion " + listBts + ", current BTS attached " + cellCodeChannel);
                                return bn;
                            }
                        }
                    } else {
                        logger.warn("Invalid cellIdChannel " + bn.getMobile());
                        bn.setResultCode("E6");
                        bn.setDescription("AGENT:Invalid cellIdChannel so can not get BTS");
                        return bn;
                    }
                }
            }

            EmolaPromotionPackage promPackage = getPromotionPackageForChannel(bn.getId());
            if (promPackage == null) {
                logger.warn("Can not get promotion package: " + bn.getMobile() + " invoice " + bn.getInvoiceId() + " ranking " + bn.getId());
                bn.setResultCode("E9");
                bn.setDescription("AGENT:Can not get promotion package: " + bn.getMobile() + " invoice " + bn.getInvoiceId() + " ranking " + bn.getId());
                return bn;
            }
            bn.setAgentPromType(promPackage.getPromotionType());
            bn.setAgentRanking(promPackage.getRankingMarked());
            //Decrease remain TV or Phone on group
            decreaseRemainGift(promPackage.getGift());
//            Get sms send to Agent
            String msg = getPromotionMessageForChannel(bn);
            bn.setAgentMessage(msg);
            bn.setAgenthasProm(true);
        }
        catch (Exception ex) {
            brBuilder.setLength(0);
            brBuilder.append(new Date()).
                    append("\nERROR checkPromChannel: ")
                    .append(" error ");
            logger.error(brBuilder + ex.toString());
            logger.error(AppManager.logException(startTime, ex));
        }
        
        return bn;
    }
    
    public EmolaPromotionPackage getPromotionPackageForChannel(long ranking) {
        EmolaPromotionPackage promPackage = null;
        StringBuilder brBuilder = new StringBuilder();
        long startTime = System.currentTimeMillis();
        try {
            String promPolicy = Config.getConfig(Config.prom3Policy, logger);
            String promPolicySpec = Config.getConfig(Config.prom3PolicySpec, logger);
            String promPolicyMultiple = Config.getConfig(Config.prom3PolicyMultiple, logger);
            String promMarked = db.getEmolaConfig(Config.prom3Marked);
            Long maxRankingProm3 = Config.getLongValue(Config.maxRankingProm + 3, logger);
            List<String> listPromMarked = new ArrayList<String>();
            if (promMarked != null) {
                listPromMarked = Arrays.asList(promMarked.split(","));
            }
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
                    promPk.setRankingMarked(dRanking);
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
                if (detailProm.length > 4) {
                    Double price = Double.parseDouble(detailProm[0]);
                    int promType = Integer.parseInt(detailProm[1]);
                    String gift = detailProm[2];
                    int min = Integer.parseInt(detailProm[3]);
                    int max = Integer.parseInt(detailProm[4]);
                    int mod = (int)(ranking % price);
                    int multiple = (int)(ranking / price);
                    String crrPromMarked = "" + (ranking - mod);
                    
                    if (min <= mod && mod <= max && multiple > 0 && !listPromMarked.contains(crrPromMarked)) {
                        if (maxRankingProm3 - ranking < 9) {
                            db.updateEmolaConfig(Config.maxRankingProm + 3, Long.toString(ranking));
                        }
                        
                        promMarked += "," + crrPromMarked;
                        db.updateEmolaConfig(Config.prom3Marked, promMarked);
                        promPackage = new EmolaPromotionPackage();
                        promPackage.setRanking(ranking);
                        promPackage.setRankingMarked(ranking - mod);
                        promPackage.setPromotionType(promType);
                        promPackage.setGift(gift);
                        break;
                    }
                }
            }       

            if (promPackage == null) {
                String[] detailProm = promPolicy.split("\\:");
                if (detailProm.length > 1) {
                    Double price = Double.parseDouble(detailProm[0]);
                    int promType = Integer.parseInt(detailProm[1]);

                    promPackage = new EmolaPromotionPackage();
                    promPackage.setRanking(ranking);
                    promPackage.setRankingMarked(ranking);
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
     
    public void decreaseRemainGift(String gift) {
        if (gift == null || "".equals(gift)) return;
        
        StringBuilder brBuilder = new StringBuilder();
        long startTime = System.currentTimeMillis();
        String remainFloat = db.getEmolaConfig(Config.remainFloat + "PROM_3");
        String remainPhone3G = db.getEmolaConfig(Config.remainPhone3G + "PROM_3");
        String remainPhone4G = db.getEmolaConfig(Config.remainPhone4G + "PROM_3");
        int actionResult = 0;
        try {
            Integer remainFloatQt = Integer.parseInt(remainFloat);
            Integer remainPhone3GQt = Integer.parseInt(remainPhone3G);
            Integer remainPhone4GQt = Integer.parseInt(remainPhone4G);
            if (gift.equals("PHONE3G") && remainPhone3GQt > 0) {
                remainPhone3GQt--;
                actionResult = db.updateEmolaConfig(Config.remainPhone3G + "PROM_3", remainPhone3GQt.toString());
            }
            else if (gift.equals("PHONE4G") && remainPhone4GQt > 0) {
                remainPhone4GQt--;
                actionResult = db.updateEmolaConfig(Config.remainPhone4G + "PROM_3", remainPhone4GQt.toString());
            }
            else if (gift.equals("FLOAT") && remainFloatQt > 0) {
                remainFloatQt--;
                actionResult = db.updateEmolaConfig(Config.remainFloat + "PROM_3", remainFloatQt.toString());
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
