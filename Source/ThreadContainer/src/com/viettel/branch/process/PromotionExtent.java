/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.branch.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.BranchPromotionBts;
import com.viettel.paybonus.obj.BranchPromotionConfig;
import com.viettel.paybonus.obj.BranchPromotionSub;
import com.viettel.paybonus.service.Exchange;
import com.viettel.paybonus.database.DbPromotionExtent;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class PromotionExtent extends ProcessRecordAbstract {

    Exchange pro;
    DbPromotionExtent db;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    Calendar calmonth = Calendar.getInstance();
    ArrayList<String> listErrNotEnoughBalance;
    String packageKey;
    List<BranchPromotionConfig> config = new ArrayList<BranchPromotionConfig>();
    String branchNotBalanceRenew;
    String branchSmsSpecial;
    String prefixRoutingvOCS;
    String[] arrPrefixRoutingvOCS;

    public PromotionExtent() {
        super();
        logger = Logger.getLogger(PromotionExtent.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbPromotionExtent();
        config = db.getConfig();
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        listErrNotEnoughBalance = new ArrayList<String>(Arrays.asList(ResourceBundle.getBundle("configPayBonus").getString("ERR_BALANCE_NOT_ENOUGH").split("\\|")));
        branchNotBalanceRenew = ResourceBundle.getBundle("configPayBonus").getString("branchNotBalanceRenew");
        branchSmsSpecial = ResourceBundle.getBundle("configPayBonus").getString("branchSmsSpecial");

        try {
            prefixRoutingvOCS = ResourceBundle.getBundle("configPayBonus").getString("prefixRoutingvOCS");
            arrPrefixRoutingvOCS = prefixRoutingvOCS.split("\\|");
        } catch (MissingResourceException ex) {
            ex.printStackTrace();
            prefixRoutingvOCS = null;
        }
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String mscNumCus;
        String cellIdCus;
        String cellCodeCus;
        BranchPromotionConfig bpc;
        String extendMode;
        BranchPromotionBts checkBtsOnline;
        String description;
        String resultChargeMoney;
        String smsContent;
        boolean isRoutingvOCS;

        for (Record record : listRecord) {
            BranchPromotionSub bps = (BranchPromotionSub) record;
            mscNumCus = "";
            cellIdCus = "";
            cellCodeCus = "";
            bpc = null;
            extendMode = "";
            checkBtsOnline = null;
            resultChargeMoney = "";
            description = "";
            smsContent = "";
            isRoutingvOCS = false;
            String tmpIsdn = bps.getIsdn();
            if (!tmpIsdn.startsWith("258")) {
                tmpIsdn = "258" + bps.getIsdn();
            }
            for (String tmpPrefix : arrPrefixRoutingvOCS) {
                if (tmpIsdn.startsWith(tmpPrefix)) {
                    logger.info("Routing number to vOCS, prefix: " + tmpPrefix + ", isdn: " + tmpIsdn);
                    isRoutingvOCS = true;
                    break;
                }
            }
            if (!isRoutingvOCS && prefixRoutingvOCS != null && prefixRoutingvOCS.isEmpty()) {
                logger.info("Don't have config for prefix routing isdn to ocs, default is routing to vOCS, isdn: " + tmpIsdn);
                isRoutingvOCS = true;
            }


            listResult.add(bps);

//          B1: Voi moi thue bao thuc hien lay ra tram BTS aang online.
            mscNumCus = pro.getMSCInfor(bps.getIsdn(), "");
            if (mscNumCus.trim().length() <= 0) {
                logger.warn("Can not get mscNumCus with ISDN= " + bps.getIsdn() + " System will count-up count_process to waiting");
                bps.setResultCode("PE01");
                bps.setDescription("Can not get mscNumCus with ISDN= " + bps.getIsdn() + " System will count-up count_process to waiting");
                bps.setMoneyFee(0L);
                continue;
            }
            cellIdCus = pro.getCellIdRsString(bps.getIsdn(), mscNumCus, "");
            if (cellIdCus.trim().length() <= 0) {
                logger.warn("Can not get Cell with mscNumCus " + mscNumCus + " " + bps.getID());
                bps.setResultCode("PE02");
                bps.setDescription("Can not get cellIdCus");
                bps.setMoneyFee(0L);
                continue;
            }
//          B3: Kiem tra neu khong lay duoc BTS thi ghi log de duyet kiem tra lại o ky quet tiep theo
            String[] arrCellId = cellIdCus.split("\\|");
            if ((arrCellId != null) && (arrCellId.length == 2)) {
                cellCodeCus = db.getCell("", arrCellId[0], arrCellId[1]);
                if (cellCodeCus.trim().length() <= 0) {
                    logger.warn("Can not get BTS Code " + bps.getIsdn() + " ID  " + bps.getID());
                    bps.setResultCode("PE03");
                    bps.setDescription("Can not get BTS Code");
                    bps.setMoneyFee(0L);
                    continue;
                }
            } else {
                logger.warn("Invalid BTS Code " + bps.getIsdn() + " ID  " + bps.getID());
                bps.setResultCode("PE04");
                bps.setDescription("Invalid BTS Code");
                bps.setMoneyFee(0L);
                continue;
            }
//          B4: Kiem tra che do gia han o truong extend_mode trong bang branch_promotion_config là 1 hay 2.
            for (BranchPromotionConfig bpcTmp : config) {
                if (bpcTmp.getPromotionCode().equals(bps.getProductCode()) && bpcTmp.getExtendMode() != null) {
                    extendMode = bpcTmp.getExtendMode();
                    bpc = bpcTmp;
                    break;
                }
            }
            description = "Isdn: " + bps.getIsdn() + "|";
            if (extendMode == null || "".equals(extendMode) || bpc == null) {
                logger.warn("extend_mode is empty");
                bps.setResultCode("PE05");
                bps.setDescription(description + "extend_mode is empty");
                bps.setMoneyFee(0L);
                continue;
            }
            if ("1".equals(extendMode)) {
//           B5: Neu che do la 1 tuc gia han lai chinh goi dang dung thi kiem tra tram BTS dang online (da lay o B2) 
//           co ho tro goi dang dung hay khong, kiem tra trong bang branch_promotion_bts. 
//           Neu khong ho tro thi ghi lich su de cho den lan quet sau kiem tra lại. 
//           Neu co thi thuc hien nghiep vu gia han (tru tien, cong cac chinh sach lien quan, luu lich su, cap nhat ngay gia han moi, …)                
                if (!db.checkZone(cellCodeCus, bps.getProductCode())) {
                    logger.warn("Can not mapping BTS:" + cellCodeCus + " with product_code:" + bps.getProductCode());
                    bps.setResultCode("PE06");
                    bps.setDescription(description + "Can not mapping BTS:" + cellCodeCus + " with product_code:" + bps.getProductCode());
                    bps.setMoneyFee(0L);
                    continue;
                } else {
//                  Tru tien tk goc
                    if (bpc.getMoneyFee() != null) {
                        if (!isRoutingvOCS) {
                            resultChargeMoney = pro.addMoney(bps.getIsdn(), "-" + bpc.getMoneyFee(), "2000");
                        } else {
                            resultChargeMoney = pro.addMoney(bps.getIsdn(), "-" + bpc.getMoneyFee(), "1");
                        }
                        if (listErrNotEnoughBalance.contains(resultChargeMoney)) {
                            logger.warn("Not enough money to play " + bps.getIsdn() + " errcode chargemoney " + resultChargeMoney);
                            bps.setResultCode("PE07");
                            bps.setDescription(description + "Not enough money to play");
                            bps.setMoneyFee(0L);
                            smsContent = branchNotBalanceRenew;
                            smsContent = smsContent.replace("%PNAME%", bps.getProductCode());
                            smsContent = smsContent.replace("%MONEY%", bpc.getMoneyFee().toString());
                            if (!db.checkAlreadyWarningInDay(bps.getIsdn(), smsContent)
                                    && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), smsContent)) {
                                db.sendSms(bps.getIsdn(), smsContent, "86904");
                            }
                            continue;
                        } else if (!"0".equals(resultChargeMoney)) {
                            logger.warn("Fail to charge money " + bps.getIsdn() + " errcode chargemoney " + resultChargeMoney);
                            bps.setResultCode("PE08");
                            bps.setDescription(description + "Fail to charge money");
                            bps.setMoneyFee(0L);
                            continue;
                        }
                        description = description + " Charge money succsess|";
                        bps.setMoneyFee(bpc.getMoneyFee());
                    } else {
                        logger.warn("Money for Package not exist in branch_promotion_config isdn :  " + bps.getIsdn());
                        bps.setResultCode("PE09");
                        bps.setDescription(description + "Money for Package not exist in branch_promotion_config isdn:  " + bps.getIsdn());
                        bps.setMoneyFee(0L);
                        continue;
                    }
                    description = addPolicy(bpc, bps, description, isRoutingvOCS);
                    smsContent = bpc.getMessageRenew();
                    smsContent = smsContent.replace("%PNAME%", bps.getProductCode());
                    if (!db.checkAlreadyWarningInDay(bps.getIsdn(), smsContent)
                            && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), smsContent)) {
                        db.sendSms(bps.getIsdn(), smsContent, "86904");
                    }
                }
            }
            if ("2".equals(extendMode)) {
//          B6.1: Kiem tra tram BTS dang online (ở B2) có nam trong 5 tinh MAC, MAT, GAZ, INH, SOF khong, 
//          neu co thi nhan tin thong bao khong ho tro gia han 
//          (chu y chi nhan 1 lan, nhan luon cung duoc khong can tu 8h sang toi 8h toi dau,
//          nhung khong duoc nhan lai sau vai phut quet lai o ky moi nua, tranh spam), luu lai log de cho ky quet sau.
                if (cellCodeCus.trim().toUpperCase().contains("MAC") || cellCodeCus.trim().toUpperCase().contains("MAT")
                        || cellCodeCus.trim().toUpperCase().contains("GAZ") || cellCodeCus.trim().toUpperCase().contains("INH")
                        || cellCodeCus.trim().toUpperCase().contains("SOF")) {
                    logger.error("Subscriber in MAC,MAT,GAZ,INH,SOF system not support renew, BTS: " + cellCodeCus.trim().toUpperCase());
                    description = description + "Subscriber in MAC,MAT,GAZ,INH,SOF system not support renew, BTS: " + cellCodeCus.trim().toUpperCase();
                    bps.setDescription(description);
                    smsContent = branchSmsSpecial;
                    smsContent = smsContent.replace("%PNAME%", bps.getProductCode());
                    if (!db.checkAlreadyWarningInDay(bps.getIsdn(), smsContent)
                            && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), smsContent)) {
                        db.sendSms(bps.getIsdn(), smsContent, "86904");
                    }
                    bps.setMoneyFee(0L);
                    continue;
                } else {
//                Kiem tra BTS maping voi goi cuoc trong bang branch_promotion_bts
                    checkBtsOnline = db.getBtsOnline(cellCodeCus);
                    if (checkBtsOnline == null) {
//                    Neu khong tim thay BTS nao => gia han ve goi dang dung
                        //Tru tien tk goc
                        if (bpc.getMoneyFee() != null) {
                            if (!isRoutingvOCS) {
                                resultChargeMoney = pro.addMoney(bps.getIsdn(), "-" + bpc.getMoneyFee(), "2000");
                            } else {
                                resultChargeMoney = pro.addMoney(bps.getIsdn(), "-" + bpc.getMoneyFee(), "1");
                            }
                            if (listErrNotEnoughBalance.contains(resultChargeMoney)) {
                                logger.warn("Not enough money to play " + bps.getIsdn() + " errcode chargemoney " + resultChargeMoney);
                                bps.setResultCode("PE10");
                                bps.setDescription(description + "Not enough money to play");
                                bps.setMoneyFee(0L);
                                smsContent = branchNotBalanceRenew;
                                smsContent = smsContent.replace("%PNAME%", bps.getProductCode());
                                smsContent = smsContent.replace("%MONEY%", bpc.getMoneyFee().toString());
                                if (!db.checkAlreadyWarningInDay(bps.getIsdn(), smsContent)
                                        && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), smsContent)) {
                                    db.sendSms(bps.getIsdn(), smsContent, "86904");
                                }
                                continue;
                            } else if (!"0".equals(resultChargeMoney)) {
                                logger.warn("Fail to charge money " + bps.getIsdn() + " errcode chargemoney " + resultChargeMoney);
                                bps.setResultCode("PE11");
                                bps.setDescription(description + "Fail to charge money");
                                bps.setMoneyFee(0L);
                                continue;
                            }
                            description = description + " Charge money succsess|";
                            bps.setMoneyFee(bpc.getMoneyFee());
                        } else {
                            logger.warn("Money for Package not exist in branch_promotion_config isdn :  " + bps.getIsdn());
                            bps.setResultCode("PE12");
                            bps.setDescription(description + "Money for Package not exist in branch_promotion_config isdn:  " + bps.getIsdn());
                            bps.setMoneyFee(0L);
                            continue;
                        }
                        description = addPolicy(bpc, bps, description, isRoutingvOCS);
                        smsContent = bpc.getMessageRenew();
                        smsContent = smsContent.replace("%PNAME%", bps.getProductCode());
                        if (!db.checkAlreadyWarningInDay(bps.getIsdn(), smsContent)
                                && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), smsContent)) {
                            db.sendSms(bps.getIsdn(), smsContent, "86904");
                        }
                    } else {
//                    Neu tim thay BTS 
//                      B6.1: Kiem tra xem BTS dang online co phai thuoc BTS giau hay không      
                        if ("1".equals(checkBtsOnline.getBtsType())) {
                            logger.error("Subscriber in special BTS: " + cellCodeCus.trim().toUpperCase());
                            description = description + "Subscriber in special BTS: " + cellCodeCus.trim().toUpperCase();
                            bps.setDescription(description);
                            smsContent = branchSmsSpecial;
                            smsContent = smsContent.replace("%PNAME%", bps.getProductCode());
                            if (!db.checkAlreadyWarningInDay(bps.getIsdn(), smsContent)
                                    && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), smsContent)) {
                                db.sendSms(bps.getIsdn(), smsContent, "86904");
                            }
                            bps.setMoneyFee(0L);
                            continue;
                        } else {
                            if ("".equals(checkBtsOnline.getPromotionCode()) || checkBtsOnline.getPromotionCode() == null) {
//                              BTS khong ho tro goi nao het => Gia han lai goi dang dung
                                //Tru tien tk goc
                                if (bpc.getMoneyFee() != null) {
                                    if (!isRoutingvOCS) {
                                        resultChargeMoney = pro.addMoney(bps.getIsdn(), "-" + bpc.getMoneyFee(), "2000");
                                    } else {
                                        resultChargeMoney = pro.addMoney(bps.getIsdn(), "-" + bpc.getMoneyFee(), "1");
                                    }
                                    if (listErrNotEnoughBalance.contains(resultChargeMoney)) {
                                        logger.warn("Not enough money to play " + bps.getIsdn() + " errcode chargemoney " + resultChargeMoney);
                                        bps.setResultCode("PE13");
                                        bps.setDescription(description + "Not enough money to play");
                                        bps.setMoneyFee(0L);
                                        smsContent = branchNotBalanceRenew;
                                        smsContent = smsContent.replace("%PNAME%", bps.getProductCode());
                                        smsContent = smsContent.replace("%MONEY%", bpc.getMoneyFee().toString());
                                        if (!db.checkAlreadyWarningInDay(bps.getIsdn(), smsContent)
                                                && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), smsContent)) {
                                            db.sendSms(bps.getIsdn(), smsContent, "86904");
                                        }
                                        continue;
                                    } else if (!"0".equals(resultChargeMoney)) {
                                        logger.warn("Fail to charge money " + bps.getIsdn() + " errcode chargemoney " + resultChargeMoney);
                                        bps.setResultCode("PE14");
                                        bps.setDescription(description + "Fail to charge money");
                                        bps.setMoneyFee(0L);
                                        continue;
                                    }
                                    description = description + " Charge money succsess|";
                                    bps.setMoneyFee(bpc.getMoneyFee());
                                } else {
                                    logger.warn("Money for Package not exist in branch_promotion_config isdn :  " + bps.getIsdn());
                                    bps.setResultCode("PE15");
                                    bps.setDescription(description + "Money for Package not exist in branch_promotion_config isdn:  " + bps.getIsdn());
                                    bps.setMoneyFee(0L);
                                    continue;
                                }
                                description = addPolicy(bpc, bps, description, isRoutingvOCS);
                                smsContent = bpc.getMessageRenew();
                                smsContent = smsContent.replace("%PNAME%", bps.getProductCode());
                                if (!db.checkAlreadyWarningInDay(bps.getIsdn(), smsContent)
                                        && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), smsContent)) {
                                    db.sendSms(bps.getIsdn(), smsContent, "86904");
                                }
                            } else {
//                             BTS ho tro goi cuoc => lay ra goi cuoc co gia tien cao nhat
                                String[] promotionCode = checkBtsOnline.getPromotionCode().split("\\|");
                                long maxMoney = 0L;
                                BranchPromotionConfig bpcMax = new BranchPromotionConfig();
                                for (String cpmb : promotionCode) {
                                    long moneyTemp = 0L;
                                    BranchPromotionConfig bpcTemp = db.getConfigByProduct(cpmb);
                                    moneyTemp = bpcTemp.getMoneyFee();
                                    if (moneyTemp > maxMoney) {
                                        maxMoney = moneyTemp;
                                        bpcMax = bpcTemp;
                                    }
                                }
                                if (maxMoney <= bpc.getMoneyFee()) {
//                                    Gia han lai goi dang dung
                                    //Tru tien tk goc
                                    if (bpc.getMoneyFee() != null) {
                                        if (!isRoutingvOCS) {
                                            resultChargeMoney = pro.addMoney(bps.getIsdn(), "-" + bpc.getMoneyFee(), "2000");
                                        } else {
                                            resultChargeMoney = pro.addMoney(bps.getIsdn(), "-" + bpc.getMoneyFee(), "1");
                                        }
                                        if (listErrNotEnoughBalance.contains(resultChargeMoney)) {
                                            logger.warn("Not enough money to play " + bps.getIsdn() + " errcode chargemoney " + resultChargeMoney);
                                            bps.setResultCode("PE16");
                                            bps.setDescription(description + "Not enough money to play");
                                            bps.setMoneyFee(0L);
                                            smsContent = branchNotBalanceRenew;
                                            smsContent = smsContent.replace("%PNAME%", bps.getProductCode());
                                            smsContent = smsContent.replace("%MONEY%", bpc.getMoneyFee().toString());
                                            if (!db.checkAlreadyWarningInDay(bps.getIsdn(), smsContent)
                                                    && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), smsContent)) {
                                                db.sendSms(bps.getIsdn(), smsContent, "86904");
                                            }
                                            continue;
                                        } else if (!"0".equals(resultChargeMoney)) {
                                            logger.warn("Fail to charge money " + bps.getIsdn() + " errcode chargemoney " + resultChargeMoney);
                                            bps.setResultCode("PE17");
                                            bps.setDescription(description + "Fail to charge money");
                                            bps.setMoneyFee(0L);
                                            continue;
                                        }
                                        description = description + " Charge money succsess|";
                                        bps.setMoneyFee(bpc.getMoneyFee());
                                    } else {
                                        logger.warn("Money for Package not exist in branch_promotion_config isdn :  " + bps.getIsdn());
                                        bps.setResultCode("PE18");
                                        bps.setDescription(description + "Money for Package not exist in branch_promotion_config isdn:  " + bps.getIsdn());
                                        bps.setMoneyFee(0L);
                                        continue;
                                    }
                                    description = addPolicy(bpc, bps, description, isRoutingvOCS);
                                    smsContent = bpc.getMessageRenew();
                                    smsContent = smsContent.replace("%PNAME%", bps.getProductCode());
                                    if (!db.checkAlreadyWarningInDay(bps.getIsdn(), smsContent)
                                            && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), smsContent)) {
                                        db.sendSms(bps.getIsdn(), smsContent, "86904");
                                    }
                                } else {
//                                    Gia han theo goi co gia tien lon nhat
                                    //Tru tien tk goc
                                    bps.setProductCode(bpcMax.getPromotionCode());
                                    bps.setMoneyFee(bpcMax.getMoneyFee());
                                    if (bpcMax.getMoneyFee() != null) {
                                        if (!isRoutingvOCS) {
                                            resultChargeMoney = pro.addMoney(bps.getIsdn(), "-" + bpc.getMoneyFee(), "2000");
                                        } else {
                                            resultChargeMoney = pro.addMoney(bps.getIsdn(), "-" + bpc.getMoneyFee(), "1");
                                        }
                                        if (listErrNotEnoughBalance.contains(resultChargeMoney)) {
                                            logger.warn("Not enough money to play " + bps.getIsdn() + " errcode chargemoney " + resultChargeMoney);
                                            bps.setResultCode("PE19");
                                            bps.setDescription(description + "Not enough money to play");
                                            bps.setMoneyFee(0L);
                                            smsContent = branchNotBalanceRenew;
                                            smsContent = smsContent.replace("%PNAME%", bpcMax.getPromotionCode());
                                            smsContent = smsContent.replace("%MONEY%", bpcMax.getMoneyFee().toString());
                                            if (!db.checkAlreadyWarningInDay(bps.getIsdn(), smsContent)
                                                    && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), smsContent)) {
                                                db.sendSms(bps.getIsdn(), smsContent, "86904");
                                            }
                                            continue;
                                        } else if (!"0".equals(resultChargeMoney)) {
                                            logger.warn("Fail to charge money " + bps.getIsdn() + " errcode chargemoney " + resultChargeMoney);
                                            bps.setResultCode("PE20");
                                            bps.setDescription(description + "Fail to charge money");
                                            bps.setMoneyFee(0L);
                                            continue;
                                        }
                                        description = description + " Charge money succsess|";
                                        bps.setMoneyFee(bpcMax.getMoneyFee());
                                    } else {
                                        logger.warn("Money for Package not exist in branch_promotion_config isdn :  " + bps.getIsdn());
                                        bps.setResultCode("PE21");
                                        bps.setDescription(description + "Money for Package not exist in branch_promotion_config isdn:  " + bps.getIsdn());
                                        bps.setMoneyFee(0L);
                                        continue;
                                    }
                                    description = addPolicy(bpcMax, bps, description, isRoutingvOCS);
                                    smsContent = bpcMax.getMessageRenew();
                                    smsContent = smsContent.replace("%PNAME%", bpcMax.getPromotionCode());
                                    if (!db.checkAlreadyWarningInDay(bps.getIsdn(), smsContent)
                                            && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), smsContent)) {
                                        db.sendSms(bps.getIsdn(), smsContent, "86904");
                                    }
                                }
                            }
                        }
                    }
                }
            }

            bps.setDescription(description);
        }
        listRecord.clear();
        return listResult;
    }

    private String addPolicy(BranchPromotionConfig bpc, BranchPromotionSub bps, String description, boolean isRoutingvOCS) {
        calmonth.setTime(new Date());
        calmonth.add(Calendar.DATE, 30);
        calmonth.set(Calendar.HOUR_OF_DAY, 23);
        calmonth.set(Calendar.MINUTE, 59);
        calmonth.set(Calendar.SECOND, 59);
        calmonth.set(Calendar.MILLISECOND, 999);
        String rsAddPrice = "";
        String rsVoiceOut = "";
        String rsSmsOut = "";
        String rsData = "";

        BigInteger dataHeigh = new BigInteger("1048576");
        Date date = new Date();
        Calendar calendar = new GregorianCalendar(/* remember about timezone! */);
        calendar.setTime(date);
        calendar.add(Calendar.DATE, 30);
        date = calendar.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

//      Cong chinh sach goi va nhan tin mien phi noi mang
        if (bpc.getCallSmsOnNetPlan() != null) {
            rsAddPrice = pro.addPrice(bps.getIsdn(), bpc.getCallSmsOnNetPlan(), "", "30");
            if ("0".equals(rsAddPrice)) {
                logger.info("Successfully add Price " + bpc.getCallSmsOnNetPlan() + " isdn " + bps.getIsdn());
                description = description + " add free call sms price: " + bpc.getCallSmsOnNetPlan() + " success|";
                bps.setDescription(description);
            } else {
                logger.error("fail to add Price " + bpc.getCallSmsOnNetPlan() + " isdn " + bps.getIsdn());
                description = description + " add free call sms price: " + bpc.getCallSmsOnNetPlan() + " fail|";
                bps.setDescription(description);
            }
        }
//     Cong so phut goi ngoai mang
        if (bpc.getCallOutNetValue() != null) {
            rsVoiceOut = pro.addSmsDataVoice(bps.getIsdn().trim(), bpc.getCallOutNetValue(), "5005", sdf.format(calmonth.getTime()));
            if ("0".equals(rsVoiceOut)) {
                logger.info("successfully add sms accountid " + bpc.getCallOutNetValue() + " isdn " + bps.getIsdn());
                description = description + " add minute callOut price: " + bpc.getCallOutNetValue() + " success|";
                bps.setDescription(description);
            } else {
                logger.error("fail to add sms accountid " + bpc.getCallOutNetValue() + " isdn " + bps.getIsdn());
                description = description + " add minute callOut price: " + bpc.getCallOutNetValue() + " fail|";
                bps.setDescription(description);
            }
        }
//      Cong so sms ngoai mang
        if (bpc.getSmsOutNetValue() != null) {
            rsSmsOut = pro.addSmsDataVoice(bps.getIsdn().trim(), bpc.getSmsOutNetValue(), "4200", sdf.format(calmonth.getTime()));
            if ("0".equals(rsSmsOut)) {
                logger.info("successfully add sms accountid 4200 isdn " + bps.getIsdn());
                description = description + " add freeSmsOutNet price: " + bpc.getSmsOutNetValue() + " success|";
                bps.setDescription(description);
            } else {
                logger.error("fail to add sms accountid 4200 isdn " + bps.getIsdn());
                description = description + " add freeSmsOutNet price: " + bpc.getSmsOutNetValue() + " fail|";
                bps.setDescription(description);
            }
        }
//      Chinh sach goi quoc te gia re
        if (bpc.getInterCallPlan() != null) {
            rsAddPrice = pro.addPrice(bps.getIsdn(), bpc.getInterCallPlan(), "", "30");
            if ("0".equals(rsAddPrice)) {
                logger.info("Successfully add Price " + bpc.getInterCallPlan() + " isdn " + bps.getIsdn());
                description = description + " add freeCallOnNet price: " + bpc.getInterCallPlan() + " success|";
                bps.setDescription(description);
            } else {
                logger.error("fail to add Price " + bpc.getInterCallPlan() + " isdn " + bps.getIsdn());
                description = description + " add freeCallOnNet price: " + bpc.getInterCallPlan() + " fail|";
                bps.setDescription(description);
            }
        }
//      Cong Data
        if (bpc.getDataValue() != null && !"pcrf".equals(bpc.getDataId())) {
            BigInteger dataValue = new BigInteger(bpc.getDataValue());
            dataValue = dataValue.multiply(dataHeigh);
            rsData = pro.addSmsDataVoice(bps.getIsdn(), String.valueOf(dataValue), "5300", sdf.format(date));
            if ("0".equals(rsData)) {
                logger.info("Successfully add data value " + bpc.getDataValue() + "MB isdn " + bps.getIsdn());
                description = description + " add data value: " + bpc.getDataValue() + "MB success|";
                bps.setDescription(description);
            } else {
                logger.error("fail to add data value " + bpc.getDataValue() + "MB isdn " + bps.getIsdn());
                description = description + " add data value: " + bpc.getDataValue() + "MB fail|";
                bps.setDescription(description);
            }
        }
//      Cong Data toc do cao sau do bop bang thong
        if (bpc.getDataValue() != null && "pcrf".equals(bpc.getDataId())) {
            if (!isRoutingvOCS) {
                String pcrfInfo = pro.querySubPCRF(bps.getIsdn());
                BigInteger dataValue = new BigInteger(bpc.getDataValue());
                dataValue = dataValue.multiply(dataHeigh);
                if ("0".equals(pcrfInfo)) {
                    //neu co thong tin thue bao tren PCRF roi
                    logger.error("Isdn " + bps.getIsdn() + " Already Exist on PCRF with result: " + pcrfInfo);
                    String rsCheckService = pro.getServiceOfSubPCRFV2(bps.getIsdn(), "Diamond_Unlimited");
                    if ("0".equals(rsCheckService)) {
                        //neu da co service ==> update exprire_time
                        logger.error("Isdn " + bps.getIsdn() + " Already Exist Service on PCRF with result: " + rsCheckService);
                        String rsUpdateExpTime = pro.updateServicesPCRF(bps.getIsdn(), "Diamond_Unlimited", sdf.format(calmonth.getTime()), sdf.format(bps.getExprieTime()));
                        if ("0".equals(rsUpdateExpTime)) {
                            //update exprie_time thanh cong
                            logger.error("Isdn " + bps.getIsdn() + " Update successfully Exprie-time on PCRF with result: " + rsUpdateExpTime);
                            String rsRechargeQuota = pro.rechargeSubQuotaPCRF(bps.getIsdn(), String.valueOf(dataValue), "Quota_Diamond6000");
                            if ("0".equals(rsRechargeQuota)) {
                                //recharge quota thanh cong
                                logger.error("Isdn " + bps.getIsdn() + " Recharge Quota successfully on PCRF with result: " + rsRechargeQuota);
                                description = description + " recharge Quota PCRF success|";
                                bps.setDescription(description);
                            } else {
                                //recharge quota that bai
                                logger.error("Isdn " + bps.getIsdn() + " Recharge Quota successfully on PCRF with result: " + rsRechargeQuota);
                                description = description + " recharge Quota PCRF fail|";
                                bps.setDescription(description);
                            }
                        } else {
                            //update exprie_time that bai
                            logger.error("Isdn " + bps.getIsdn() + " Update Fail Exprie-time on PCRF with result: " + rsUpdateExpTime);
                            description = description + " update exprie_time PCRF fail|";
                            bps.setDescription(description);
                        }
                    } else {
                        //neu chua co service ==> add service
                        String addServiceToPcrf = pro.addSubServiceViaPCRF(bps.getIsdn(), "Diamond_Unlimited", sdf.format(calmonth.getTime()));
                        if ("0".equals(addServiceToPcrf)) {
                            //Add service thanh cong
                            logger.error("Isdn " + bps.getIsdn() + " Add service Succsessfully PCRF with result: " + addServiceToPcrf);
                            String rsUpdateQuota = pro.updateQuotaPCRF(bps.getIsdn(), "Quota_Diamond6000", String.valueOf(dataValue));
                            if ("0".equals(rsUpdateQuota)) {
                                //Update quota thanh cong.
                                logger.error("Isdn " + bps.getIsdn() + " Update Quota Successfully On PCRF with result: " + rsUpdateQuota);
                                description = description + " update quota PCRF succsess|";
                                bps.setDescription(description);
                            } else {
                                //Update quota that bai
                                logger.error("Isdn " + bps.getIsdn() + " Update Quota Fail On PCRF with result: " + rsUpdateQuota);
                                description = description + " update quota PCRF fail|";
                                bps.setDescription(description);
                            }
                        } else {
                            //Add service that bai
                            logger.error("Isdn " + bps.getIsdn() + " Add service Fail PCRF with result: " + addServiceToPcrf);
                            description = description + " add service PCRF fail|";
                            bps.setDescription(description);
                        }
                    }
                } else {
                    //neu chua co thong tin thue bao tren PCRF thi add Sub
                    logger.error("Isdn " + bps.getIsdn() + " Not Yet Exist on PCRF with result: " + pcrfInfo);
                    String addSubToPcrf = pro.addSubViaPCRF(bps.getIsdn());
                    if ("0".endsWith(addSubToPcrf)) {
                        //neu add Sub thanh cong thi add service
                        logger.error("Isdn " + bps.getIsdn() + " Add Successfully on PCRF with result: " + addSubToPcrf);
                        String addServiceToPcrf = pro.addSubServiceViaPCRF(bps.getIsdn(), "Diamond_Unlimited", sdf.format(calmonth.getTime()));
                        if ("0".equals(addServiceToPcrf)) {
                            //Add service thanh cong
                            logger.error("Isdn " + bps.getIsdn() + " Add service Succsessfully PCRF with result: " + addServiceToPcrf);
                            String rsUpdateQuota = pro.updateQuotaPCRF(bps.getIsdn(), "Quota_Diamond6000", String.valueOf(dataValue));
                            if ("0".equals(rsUpdateQuota)) {
                                //Update quota thanh cong.
                                logger.error("Isdn " + bps.getIsdn() + " Update Quota Successfully On PCRF with result: " + rsUpdateQuota);
                                description = description + " Update Quota PCRF succsess|";
                                bps.setDescription(description);
                            } else {
                                //Update quota that bai
                                logger.error("Isdn " + bps.getIsdn() + " Update Quota Fail On PCRF with result: " + rsUpdateQuota);
                                description = description + " Update Quota PCRF fail|";
                                bps.setDescription(description);
                            }
                        } else {
                            //Add service that bai
                            logger.error("Isdn " + bps.getIsdn() + " Add service Fail PCRF with result: " + addServiceToPcrf);
                            description = description + " Add service PCRF fail|";
                            bps.setDescription(description);
                        }
                    } else {
                        //Add Service that bai
                        logger.error("Isdn " + bps.getIsdn() + " Add Sub fail by command PCRF_ADDSUB_NO_SERVICE with result: " + addSubToPcrf);
                        description = description + " Add Sub PCRF fail|";
                        bps.setDescription(description);
                    }
                }
            } else {
                String result = pro.addPrice(bps.getIsdn(), "6000", "", "30");
                if (!"0".equals(result)) {
                    logger.error("Isdn " + bps.getIsdn() + " Fail to add price plan data unlimited for OLA500 result: " + result);
                    description = description + " Fail to add price plan data unlimited for OLA500|";
                    bps.setDescription(description);
                } else {
                    logger.info("Isdn " + bps.getIsdn() + " add price plan data unlimited for OLA500 success, result: " + result);
                    description = description + " Add price plan data unlimited for OLA500 success|";
                    bps.setDescription(description);
                }
            }
        }
        return description;
    }

    @Override
    public void printListRecord(List<Record> listRecord) throws Exception {
        StringBuilder br = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        br.setLength(0);
        br.append("\r\n").
                append("|\tACTION_AUDIT_ID|").
                append("|\tISDN\t|").
                append("|\tCREATE_USER\t|").
                append("|\tCREATE_TIME\t|").
                append("|\tPRODUCT_CODE\t|").
                append("|\tEXPIRE_TIME\t|");
        for (Record record : listRecord) {
            BranchPromotionSub bps = (BranchPromotionSub) record;
            br.append("\r\n").
                    append("|\t").
                    append(bps.getActionAuditId()).
                    append("||\t").
                    append(bps.getIsdn()).
                    append("||\t").
                    append(bps.getCreateUser()).
                    append("||\t").
                    append((bps.getCreateTime() != null ? sdf.format(bps.getCreateTime()) : null)).
                    append("||\t").
                    append(bps.getProductCode()).
                    append("||\t").
                    append(bps.getExprieTime());
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
