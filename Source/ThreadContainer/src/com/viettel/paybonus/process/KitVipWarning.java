/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbKitVipWarning;
import com.viettel.paybonus.obj.KitWarning;
import com.viettel.paybonus.obj.Offer;
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
import java.util.Date;
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class KitVipWarning extends ProcessRecordAbstract {

    Exchange pro;
    Service services;
    DbKitVipWarning db;
    String msgCusBeforeExpire;
    String msgCusAfterExpire;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
    SimpleDateFormat sdf3 = new SimpleDateFormat("dd/MM");
    String msgKitVipWarnCusBefore3or2DaysExpire;

    public KitVipWarning() {
        super();
        logger = Logger.getLogger(KitVipWarning.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbKitVipWarning("dbElite", logger);
        services = new Service(ExchangeClientChannel.getInstance("../etc/service_client.cfg").getInstanceChannel(), logger);
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        msgCusBeforeExpire = ResourceBundle.getBundle("configPayBonus").getString("msgKitVipWarnCusBeforeExpire");
        msgCusAfterExpire = ResourceBundle.getBundle("configPayBonus").getString("msgKitVipWarnCusAfterExpire");
        msgKitVipWarnCusBefore3or2DaysExpire = ResourceBundle.getBundle("configPayBonus").getString("msgKitVipWarnCusBefore3or2DaysExpire");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String msgWarnCus;
        Long money;
        String productName;
        for (Record record : listRecord) {
            msgWarnCus = "";
            money = 0L;
            KitWarning bn = (KitWarning) record;
            listResult.add(bn);
            productName = "";
            if (bn.getIsdn() == null || bn.getEffectDate() == null || bn.getProductCode() == null || bn.getPpCode() == null) {
                logger.warn("isdn " + bn.getIsdn() + " getEffectDate " + bn.getEffectDate() + " getProductCode " + bn.getProductCode()
                        + " getPpCode " + bn.getPpCode());
                continue;
            }
//            Step 1: Get sta_datetime of vip isdn
            if (bn.getEffectDate() != null) {
                Date nextExtDateFull = sdf.parse(bn.getEffectDate());
                String nextExtDateStr = sdf2.format(nextExtDateFull);
                Date nextExtDate = sdf2.parse(nextExtDateStr);
                String currDateSrt = sdf2.format(new Date());
                Date currDate = sdf2.parse(currDateSrt);
//            Step 3: Get money         
                money = db.getPriceProduct(bn.getProductCode());
                if (money == null) {
                    logger.info("Product code is invalid, get product in sub_mb: " + bn.getProductCode());
                    String productCode = db.getProductCode(bn.getIsdn());
                    if (productCode.isEmpty()) {
                        logger.warn("Not send sms warning Elite package because can not get product code in sub_mb of isdn " + bn.getIsdn()
                                + " package " + bn.getProductCode());
                        bn.setResultCode("E01");
                        bn.setDescription("Can not get product code in sub_mb");
                        continue;
                    }
                    money = db.getPriceProduct(productCode);
                    if (money == null) {
                        money = 0L;
                    }
                    bn.setProductCode(productCode);

                }
                productName = db.getProductName(bn.getProductCode());
                if (compareDate(currDate, nextExtDate) == -3 || compareDate(currDate, nextExtDate) == -2) {
                    msgWarnCus = msgKitVipWarnCusBefore3or2DaysExpire;
                    msgWarnCus = msgWarnCus.replace("%PACKAGE%", productName).replace("%MONEY%", String.valueOf(money))
                            .replace("%EXPIRE_DATE%", sdf3.format(nextExtDateFull));
                    logger.info("Send sms for warning Elite to customer " + bn.getIsdn()
                            + " package " + bn.getProductCode()
                            + " message " + msgWarnCus);
                    db.sendSms(bn.getIsdn(), msgWarnCus, "155");
                    bn.setResultCode("0");
                    bn.setDescription("SEND_SMS_SUCCESS");
                    bn.setSms(msgWarnCus);
                    continue;
                } else if (compareDate(currDate, nextExtDate) == -1) {
                    msgWarnCus = msgCusBeforeExpire;
                    msgWarnCus = msgWarnCus.replace("%PACKAGE%", productName).replace("%MONEY%", String.valueOf(money));
                    logger.info("Send sms for warning Elite to customer " + bn.getIsdn()
                            + " package " + bn.getProductCode()
                            + " message " + msgWarnCus);
                    db.sendSms(bn.getIsdn(), msgWarnCus, "155");
                    bn.setResultCode("0");
                    bn.setDescription("SEND_SMS_SUCCESS");
                    bn.setSms(msgWarnCus);
                    continue;
                } else if (compareDate(currDate, nextExtDate) == 0) {
                    msgWarnCus = msgCusAfterExpire;
                    msgWarnCus = msgWarnCus.replace("%PACKAGE%", productName).replace("%MONEY%", String.valueOf(money));
                    logger.info("Send sms for warning Elite to customer " + bn.getIsdn()
                            + " package " + bn.getProductCode()
                            + " message " + msgWarnCus);
                    db.sendSms(bn.getIsdn(), msgWarnCus, "155");
                    bn.setResultCode("0");
                    bn.setDescription("SEND_SMS_SUCCESS");
                    bn.setSms(msgWarnCus);
                    continue;
                } else if (compareDate(currDate, nextExtDate) == 1) {
                    logger.info("Alert promotion expired day but not yet renew, isdn" + bn.getIsdn()
                            + " package " + bn.getProductCode());
                    String tmpEffectDate = "";
                    HashMap<String, String> lstParams = new HashMap<String, String>();
                    lstParams.put("MSISDN", "258" + bn.getIsdn());
                    String original = pro.getOriginalOfCommand(bn.getIsdn(), "OCSHW_INTEGRATIONENQUIRY", lstParams);
                    if (!original.isEmpty()) {
                        List<Offer> listOffer = pro.parseListOffer(original);
                        if (listOffer != null && listOffer.size() > 0) {
                            for (Offer offer : listOffer) {
                                if (offer.getName() != null && offer.getName().contains("_ADDON") && "1".equals(offer.getState())) {
                                    SimpleDateFormat tmpSdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                                    if (offer.getRecurringDate() != null && offer.getRecurringDate().length() > 0) {
                                        Date recurringDate = tmpSdf.parse(offer.getRecurringDate());
                                        Calendar cal = Calendar.getInstance();
                                        cal.setTime(recurringDate);
                                        cal.add(Calendar.DATE, 30);
                                        tmpEffectDate = sdf.format(cal.getTime());
                                        logger.info("tmpEffectDate of subscriber on vOCS3: " + tmpEffectDate + ", msisdn: " + bn.getIsdn());
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    Calendar calNow = Calendar.getInstance();
                    String tmpDate = sdf2.format(calNow.getTime());
                    logger.info("Current date: " + tmpDate + ", effectDate: " + tmpEffectDate + ", isdn: " + bn.getIsdn());
                    if (tmpEffectDate.isEmpty() || sdf.parse(tmpEffectDate).before(sdf2.parse(tmpDate))
                            || sdf.parse(tmpEffectDate).equals(sdf2.parse(tmpDate))) {
                        msgWarnCus = msgCusAfterExpire;
//                        Not yet renew...send sms
                        logger.info("Begin send sms alert promotion expired, isdn" + bn.getIsdn()
                                + " package " + bn.getProductCode());
                        msgWarnCus = msgWarnCus.replace("%PACKAGE%", productName).replace("%MONEY%", String.valueOf(money));
                        db.sendSms(bn.getIsdn(), msgWarnCus, "155");
                        bn.setResultCode("0");
                        bn.setDescription("SEND_SMS_SUCCESS");
                        bn.setSms(msgWarnCus);
                        continue;
                    } else {
                        logger.info("Already renewed, no need send sms, result compare: " + compareDate(currDate, nextExtDate));
                        bn.setResultCode("E06");
                        bn.setDescription("No need send SMS.");
                    }
                } else {
                    logger.warn("Not send sms warning Elite package because not in warning time " + bn.getIsdn()
                            + " package " + bn.getProductCode() + " createStaff " + bn.getPpCode());
                    bn.setResultCode("E06");
                    bn.setDescription("Not send sms warning Elite package because not in warning time: " + compareDate(currDate, nextExtDate) + " days");
                    continue;
                }
            } else {
                logger.warn("Not send sms warning Elite package because can not get sta_datetime of isdn " + bn.getIsdn()
                        + " package " + bn.getProductCode() + " createStaff " + bn.getPpCode());
                bn.setResultCode("E05");
                bn.setDescription("Not send sms warning Elite package because can not get sta_datetime of isdn");
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
                append("|\tISDN|").
                append("|\tproduct_code\t|").
                append("|\teffect_date\t|");
        for (Record record : listRecord) {
            KitWarning bn = (KitWarning) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append(bn.getPpCode()).
                    append("||\t").
                    append(bn.getEffectDate());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        logger.warn("TEMPLATE process exception record: " + ex.toString());
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }

    private long compareDate(Date date1, Date date2) {
        if (date1 != null && date2 != null) {
            long secondDate1 = date1.getTime();
            long secondDate2 = date2.getTime();
            return ((secondDate1 - secondDate2) / 1000 / 60 / 60 / 24);
        } else {
            return -10000;
        }
    }
}
