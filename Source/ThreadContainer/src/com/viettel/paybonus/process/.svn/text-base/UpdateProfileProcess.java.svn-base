/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbSubProfileUpdateProcessor;
import com.viettel.paybonus.obj.Agent;
import com.viettel.paybonus.obj.Bonus;
import com.viettel.paybonus.obj.ProductConnectKit;
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
import java.util.concurrent.TimeUnit;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class UpdateProfileProcess extends ProcessRecordAbstract {

    Exchange pro;
    DbSubProfileUpdateProcessor db;
    String msgChannelUpdateCorrectInfo;
    String msgCusUpdateCorrectInfo;
    String msgChannelIncorrectImage;
    String msgChannelIncorrectName;
    String msgChannelIncorrectIdNo;
    String msgChannelDimImage;
    String msgChannelMissingStudentCard;
    String msgChannelMissingImage;
    String msgChannelInvalidStudentCode;
    String msgChannelDocumentCopy;
    String msgCusIncorrectImage;
    String msgCusIncorrectName;
    String msgCusIncorrectIdNo;
    String msgCusDimImage;
    String msgCusMissingStudentCard;
    String msgCusMissingImage;
    String msgCusInvalidStudentCode;
    String msgCusDocumentCopy;
    SimpleDateFormat sdf2;
    String kitConnectActiveSaleHandset;

    public UpdateProfileProcess() {
        super();
        logger = Logger.getLogger(UpdateProfileProcess.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        msgChannelUpdateCorrectInfo = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpdateCorrectInfo");
        msgCusUpdateCorrectInfo = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpdateCorrectInfo");
        msgChannelIncorrectImage = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpIncorrectImage");
        msgChannelIncorrectName = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpIncorrectName");
        msgChannelIncorrectIdNo = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpIncorrectIdNo");
        msgChannelDimImage = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpDimImage");
        msgChannelMissingStudentCard = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpMissingStudentCard");
        msgChannelMissingImage = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpMissingImage");
        msgChannelInvalidStudentCode = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpInvalidStudentCode");
        msgChannelDocumentCopy = ResourceBundle.getBundle("configPayBonus").getString("msgChannelUpDocumentCopy");
        msgCusIncorrectImage = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpIncorrectImage");
        msgCusIncorrectName = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpIncorrectName");
        msgCusIncorrectIdNo = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpIncorrectIdNo");
        msgCusDimImage = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpDimImage");
        msgCusMissingStudentCard = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpMissingStudentCard");
        msgCusMissingImage = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpMissingImage");
        msgCusInvalidStudentCode = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpInvalidStudentCode");
        msgCusDocumentCopy = ResourceBundle.getBundle("configPayBonus").getString("msgCusUpDocumentCopy");
//        msg = ResourceBundle.getBundle("configPayBonus").getString("message_add_money");
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbSubProfileUpdateProcessor();
        sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
        kitConnectActiveSaleHandset = ResourceBundle.getBundle("configPayBonus").getString("kitConnectActiveSaleHandset");
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String openFlagPLMResult;
        String openFlagBAOCResult;
        String activeOCS;
        String openFlagGPRSResult;
        String openFlagBAICResult;
        String serial;
        long comission;
        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
        for (Record record : listRecord) {
            openFlagPLMResult = "";
            openFlagBAOCResult = "";
            activeOCS = "";
            openFlagGPRSResult = "";
            openFlagBAICResult = "";
            serial = "";
            Bonus bn = (Bonus) record;
            listResult.add(bn);
            String logOpenFlags = "";
            comission = 0;
            String isdn = bn.getIsdnCustomer().startsWith("258") ? bn.getIsdnCustomer().trim().substring(3) : bn.getIsdnCustomer().trim();
            if (isdn == null || isdn.trim().length() == 0) {
                logger.warn("Can not get ISDN " + bn.getIsdnCustomer() + " cancel transaction");
                bn.setResultCode("E25");
                bn.setDescription("Can not get ISDN");
                continue;
            }
            if ("1".equals(bn.getCheckInfo().trim())) {
                if ("0".equals(bn.getResultCode())) {
                    String actStatus = db.getIsdnStatus(isdn);
                    if (!"00".equals(actStatus) && !"03".equals(actStatus)) {
                        logger.warn("This isdn is not normal(00) or wating to active(03),so don't open any flags " + bn.getIsdnCustomer() + " so continue with other transaction");
                        logOpenFlags = "Not open flag for blocked number.";
                    } else {
                        //                        Flow step...
//                        HLR_HW_MOD_BAOC
//                        HLR_HW_MOD_BAIC
//                        HLR_HW_MOD_PLMNSS
//                        4G: HLR_HW_MODI_SUB_LOCK_4G/ 3G: HLR_HW_MODI_SUB_GPRS
//                        HLR_HW_MOD_ODBROAM

                        openFlagBAOCResult = pro.activeFlagBAOC("258" + bn.getIsdnCustomer());
                        if ("0".equals(openFlagBAOCResult)) {
                            logger.info("Open flag BAOC successfully for sub when update info " + bn.getIsdnCustomer());
                        } else {
                            logger.warn("Fail to open flag BAOC for sub when update info " + bn.getIsdnCustomer());
                        }
                        openFlagBAICResult = pro.activeFlagBAIC("258" + bn.getIsdnCustomer());
                        if ("0".equals(openFlagBAICResult)) {
                            logger.info("Open flag BAIC successfully for sub when update info " + bn.getIsdnCustomer());
                        } else {
                            logger.warn("Fail to open flag BAIC for sub when update info " + bn.getIsdnCustomer());
                        }
                        openFlagPLMResult = pro.activeFlagPLMNSS("258" + bn.getIsdnCustomer());
                        if ("0".equals(openFlagPLMResult)) {
                            logger.info("Open flag PLMNSS successfully for sub when update info " + bn.getIsdnCustomer());
                        } else {
                            logger.warn("Fail to open flag PLMNSS for sub when update info " + bn.getIsdnCustomer());
                        }
//                        LinhNBV 20190117: Unlock Flag for SIM 4G.
                        serial = db.getSerialByIsdnCustomer(isdn);
                        bn.setSimSerial(serial);
                        if (bn.getSimSerial() != null && bn.getSimSerial().length() > 0) {
                            boolean isSim4G = db.isSim4G(bn.getSimSerial(), 207607L);
                            if (isSim4G) {
                                logger.info("Begin open FLAG for 4G subscriber..." + bn.getIsdnCustomer());
                                String flagData = pro.modDataFlag("258" + bn.getIsdnCustomer(), "FALSE", "FALSE");
                                if ("0".equals(flagData)) {
                                    logger.info("Open flag DATA 3G/4G successfully for sub when update info " + bn.getIsdnCustomer());
                                } else {
                                    logger.warn("Fail to open flag DATA 3G/4G for sub when update info " + bn.getIsdnCustomer());
                                }
                            } else {
                                openFlagGPRSResult = pro.activeFlagGPRSLCK("258" + bn.getIsdnCustomer());
                                if ("0".equals(openFlagGPRSResult)) {
                                    logger.info("Open flag GPRS successfully for sub when update info " + bn.getIsdnCustomer());
                                } else {
                                    logger.warn("Fail to open flag GPRS for sub when update info " + bn.getIsdnCustomer());
                                }
                            }
                        } else {
                            openFlagGPRSResult = pro.activeFlagGPRSLCK("258" + bn.getIsdnCustomer());
                            if ("0".equals(openFlagGPRSResult)) {
                                logger.info("Open flag GPRS successfully for sub when update info " + bn.getIsdnCustomer());
                            } else {
                                logger.warn("Fail to open flag GPRS for sub when update info " + bn.getIsdnCustomer());
                            }
                        }
                        String flagRoaming = pro.modRoamingFlag("258" + bn.getIsdnCustomer(), "NOBAR");
                        if ("0".equals(flagRoaming)) {
                            logger.info("Open flag ROAMING successfully for sub when update info " + bn.getIsdnCustomer());
                        } else {
                            logger.warn("Fail to open flag ROAMING for sub when update info " + bn.getIsdnCustomer());
                        }
                        logOpenFlags = "Open flag for this number";

//               Update block_ocs_hlr=0 is unblock one way
//                    updateSubProfileInfo = db.updateSubProfileInfo(bn.getIsdnCustomer());
//                    if (updateSubProfileInfo == 1) {
//                        logger.info("Update open block one way on OCS_HLR " + bn.getIsdnCustomer());
//                    } else {
//                        logger.warn("Fail to Update open block one way on OCS_HLR" + bn.getIsdnCustomer());
//                    }
//               Active on OCS OCSHW_MODI_VALIDITY to request time block two way
                        activeOCS = pro.activeOCS("258" + bn.getIsdnCustomer());
                        if ("0".equals(activeOCS)) {
                            logger.info("Active flag OCSHW_MODI_VALIDITY successfully for sub " + bn.getIsdnCustomer());
                        } else {
                            logger.warn("Fail to Active flag OCSHW_MODI_VALIDITY successfully for sub  " + bn.getIsdnCustomer());
                        }

                        openFlagGPRSResult = pro.activeFlagGPRSLCK("258" + bn.getIsdnCustomer());
                        if ("0".equals(openFlagGPRSResult)) {
                            logger.info("Open flag GPRS successfully for sub when register info " + bn.getIsdnCustomer());
                        } else {
                            logger.warn("Fail to open flag GPRS for sub when register info " + bn.getIsdnCustomer());
                        }
                        logger.info("Update information success for isdn "
                                + bn.getIsdnCustomer());
                        logOpenFlags = logOpenFlags + ". Update success for cus. ";
                        bn.setResultCode("0");
                        bn.setDescription(logOpenFlags + ".Update success for " + bn.getIsdnCustomer());
                        bn.setMessage(msgCusUpdateCorrectInfo.replace("%XYZ%", bn.getIsdnCustomer()));

//                        logger.info("Message will send to isdnEmola " + bn.getIsdnCustomer() + " msg: " + msg);
                        String phoneChannel = db.getTelByStaffCode(bn.getUserName());
//                        String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                        String tempMsg = msgChannelUpdateCorrectInfo.replace("%XYZ%", bn.getIsdnCustomer());
                        db.sendSms(phoneChannel, tempMsg, "86142");
                        logger.info("Message send to staff: " + tempMsg + " isdn " + phoneChannel);
                    }
                    //                LinhNBV 20200307: pay money for user connect with handset
                    if (bn.getHandsetModel() != null && !bn.getHandsetModel().isEmpty()) {
                        if (!db.checkChangeProductToStudent(bn.getActionProfileId())) {
                            logger.info("Doesn't exist sub_profile_id: " + bn.getActionProfileId() + " in table request_change_product, start pay money for user connect with handset: " + bn.getHandsetModel()
                                    + ", isdn: " + bn.getIsdnCustomer());
                            long diff = new Date().getTime() - bn.getIssueDateTime().getTime();
                            long days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                            days++; //add one more day
                            if (days <= 7) {
                                String mainProductConnect = "";
                                if (bn.getMainProduct() != null && !bn.getMainProduct().isEmpty()) {
                                    mainProductConnect = db.getMainProductConnectKit(bn.getMainProduct());
                                }
                                if (!mainProductConnect.isEmpty()) {
                                    if (!db.checkAlreadyPayBonus(bn.getActionAuditId())) {
                                        long priceHandsetSms = 0;
                                        long priceSpecial = 0;
                                        String[] arrHandsetInfo = bn.getHandsetModel().split("\\|");
                                        String stockModelCode = arrHandsetInfo[0].trim();
                                        String priceType = arrHandsetInfo[1].trim();
                                        String userCreated = db.getUserCreatedSubMb(bn.getIsdnCustomer());
                                        String shopPath = db.getShopPathByStaffCode(userCreated);
                                        String[] arrShopPath = shopPath.split("\\_");//_7282_shopConfig_shopChildren
                                        String shopId = "";
                                        if (arrShopPath.length > 2) {
                                            shopId = arrShopPath[2];
                                        } else {
                                            shopId = arrShopPath[1];
                                        }
                                        String pricePolicy = db.getPricePolicyHandset(mainProductConnect, stockModelCode, shopId);
                                        Agent staffInfo = db.getAgentInfoByUser(userCreated);
                                        if (staffInfo != null && staffInfo.getIsdnWallet() != null && staffInfo.getIsdnWallet().length() > 0) {
                                            logger.info("start pay money for user connect with handset: " + stockModelCode
                                                    + ", user: " + userCreated + ", isdn: " + bn.getIsdnCustomer());
                                            String priceTypeConfig = db.getBasedConfigConnectKit(userCreated, "revenue_type_handset");
                                            String[] arrPriceTypeConfig = priceTypeConfig.split("\\|");
                                            for (String tmp : arrPriceTypeConfig) {
                                                String[] arrTmp = tmp.split("\\-");
                                                if (arrTmp[1].trim().equals(mainProductConnect)) {
                                                    long comissionHandset = 0;
                                                    long discount = db.getDiscountForHandset(stockModelCode, mainProductConnect, shopId);
                                                    if ("1".equals(arrTmp[0].trim())) {
                                                        priceSpecial = db.getPriceByStockModelCode(stockModelCode, priceType, pricePolicy);
                                                        logger.info("Main product: " + mainProductConnect + ", Price and discount is Special, "
                                                                + "calculate money and pay..., isdn: " + bn.getIsdnCustomer());
                                                        priceType = "1";
                                                        pricePolicy = "1";
                                                        long priceHandset = db.getPriceByStockModelCode(stockModelCode, priceType, pricePolicy);
                                                        comissionHandset = priceHandset - priceSpecial + discount;
                                                        priceHandsetSms = priceSpecial;
                                                        logger.info("Handset have special price, "
                                                                + "comission for sale handset = priceRetail(" + priceHandset + ") - priceSpecial(" + priceSpecial + ") + discount(" + discount + ") "
                                                                + "result: " + comissionHandset + ", mainProduct: " + mainProductConnect + ", handset: " + stockModelCode + ", isdn: " + bn.getIsdnCustomer());
                                                    } else {
                                                        priceHandsetSms = db.getPriceByStockModelCode(stockModelCode, priceType, pricePolicy);
                                                        logger.info("Handset have promotion price: " + priceHandsetSms
                                                                + "comission for sale handset = discount(" + discount + ") "
                                                                + "result: " + comissionHandset + ", mainProduct: " + mainProductConnect + ", handset: " + stockModelCode + ", isdn: " + bn.getIsdnCustomer());
                                                        comissionHandset = discount;
                                                    }
                                                    if (comissionHandset > 0) {
                                                        String eWalletResponse = pro.callEwalletV2(bn.getActionAuditId(), bn.getChannelTypeId(), staffInfo.getIsdnWallet(), comissionHandset,
                                                                bn.getActionCode(), userCreated, sdf.format(new Date()), 6, db);
                                                        String tel = db.getTelByStaffCode(userCreated);
                                                        db.sendSms("258" + tel, "Voce recebeu o total de " + comissionHandset + " MT por vender o telemovel " + stockModelCode + ". Obrigado!", "86142");
                                                        logger.info("end pay commission for connect with handset, response: " + eWalletResponse + ", isdn: " + bn.getIsdnCustomer());
                                                    }

                                                    break;
                                                }
                                            }
                                        } else {
                                            logger.info("don't have isdnWallet, can't paid money when connect with handset: " + stockModelCode
                                                    + ", user: " + userCreated + ", isdn: " + bn.getIsdnCustomer());
                                        }
                                        String tmpMessage = kitConnectActiveSaleHandset.replace("XXX", mainProductConnect)
                                                .replace("ZZZ", stockModelCode).replace("WWW", String.valueOf(priceHandsetSms));
                                        if (bn.getVasCode() != null && !bn.getVasCode().isEmpty()) {
                                            tmpMessage = tmpMessage.replace("YYY", bn.getVasCode().trim());
                                        } else {
                                            String productCode = db.getProductCode(bn.getIsdnCustomer());
                                            tmpMessage = tmpMessage.replace("YYY", productCode.trim());
                                        }
                                        db.sendSms(bn.getIsdnCustomer(), tmpMessage, "86142");
                                    } else {
                                        logger.info("Already paybonus for handset, no need bonus more..., actionAuditId: " + bn.getActionAuditId());
                                    }

                                }
                            } else {
                                logger.info("Over 7 days, no need paid money when connect with handset, isdn: " + bn.getIsdnCustomer());
                            }
                        } else {
                            logger.info("Exist sub_profile_id: " + bn.getActionProfileId() + " in table request_change_product, pay money in another process"
                                    + ", isdn: " + bn.getIsdnCustomer());
                        }

                    }
                    boolean isActive = false;
                    if (bn.getMainPrice() != null && !bn.getMainPrice().isEmpty()) {
                        String mainPriceConfig = db.getMainPriceConfig(bn.getMainPrice(), bn.getMainProduct());
                        if (!mainPriceConfig.isEmpty()) {
                            String[] arrConfig = mainPriceConfig.split("\\^");
                            if (arrConfig.length == 2) {
                                comission += Long.valueOf(arrConfig[1]);
                                if (!arrConfig[0].trim().isEmpty() && !"NA".equals(arrConfig[0].trim())) {
                                    logger.info("start add benefit : " + arrConfig[0] + ", isdn: " + bn.getIsdnCustomer());
                                    if (!isActive) {
                                        String activeOnOCS = pro.activeOCSACTIVEFIRST("258" + bn.getIsdnCustomer());
                                        if ("0".equals(activeOnOCS)) {
                                            isActive = true;
                                            logger.info("Open flag OCSHW_ACTIVEFIRST successfully for sub: " + bn.getIsdnCustomer());
                                            String[] arrBenifit = arrConfig[0].split("\\|");
                                            Calendar calendar = Calendar.getInstance();
                                            calendar.setTime(new Date());
                                            calendar.add(Calendar.DATE, 30);
                                            for (String tmpBenifit : arrBenifit) {
                                                String[] arrTmp = tmpBenifit.split("\\:");
                                                if (bn.getVasCode() != null && !bn.getVasCode().isEmpty()) {
                                                    String result = pro.addMoney(bn.getIsdnCustomer(), arrTmp[1], arrTmp[0]);
                                                    logger.info("Customer register vas, only add basic account result: " + result + ", balanceId: " + arrTmp[0] + ", value: " + arrTmp[1] + ", isdn: " + bn.getIsdnCustomer());
                                                    break;
                                                } else {
                                                    String result = "";
                                                    if ("1".equals(arrTmp[0])) {
                                                        result = pro.addMoney(bn.getIsdnCustomer(), arrTmp[1], arrTmp[0]);
                                                    } else {
                                                        result = pro.addSmsDataVoice(bn.getIsdnCustomer(), arrTmp[1], arrTmp[0], sdf2.format(calendar.getTime()));
                                                    }
                                                    logger.info("result add benefit: " + result + ", balanceId: " + arrTmp[0] + ", value: " + arrTmp[1] + ", isdn: " + bn.getIsdnCustomer());
                                                }
                                            }
                                        } else {
                                            logger.warn("Open flag OCSHW_ACTIVEFIRST unsuccessfully, can not add benifit: " + arrConfig[0] + ", for sub: " + bn.getIsdnCustomer() + ", result: " + activeOnOCS);
                                        }
                                    }
                                } else {
                                    logger.info("don't configure benefit for mainPrice : " + bn.getMainPrice() + ", isdn: " + bn.getIsdnCustomer());
                                }
                                if (Long.valueOf(arrConfig[1]) > 0) {
                                    logger.info("start pay comission for refill, mainPrice: " + bn.getMainPrice() + ", isdn: " + bn.getIsdnCustomer());
                                    Agent staffInfo = db.getAgentInfoByUser(bn.getUserName());
                                    boolean payEmola = false;
                                    if (staffInfo == null) {
                                        logger.warn("Can not get Staff Info, user: " + bn.getUserName() + " actionId " + bn.getActionAuditId()
                                                + " id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                                    } else if (staffInfo.getIsdnWallet() != null && staffInfo.getIsdnWallet().length() > 0) {
                                        if (db.checkChannelHaveContract(bn.getUserName()) || db.checkMovitelStaff(bn.getUserName())) {
                                            payEmola = true;
                                        }
                                    }
                                    if (payEmola && (staffInfo != null && staffInfo.getIsdnWallet() != null)) {
                                        String eWalletResponse = pro.callEwallet(bn.getActionAuditId(), bn.getChannelTypeId(), staffInfo.getIsdnWallet(), comission,
                                                bn.getActionCode(), bn.getUserName(), sdf.format(new Date()), db);
                                        logger.info("end pay commission for refill, response: " + eWalletResponse + ", isdn: " + bn.getIsdnCustomer());
                                    } else {
                                        logger.info("Don't have isdnWallet or profile of channel is not check or not Movitel Staff, staffCode: " + bn.getUserName() + ", isdn: " + bn.getIsdnCustomer());
                                    }

                                } else {
                                    logger.info("don't configure comission for mainPrice : " + bn.getMainPrice() + ", isdn: " + bn.getIsdnCustomer());
                                }

                            } else {
                                logger.info("length of array config for mainPrice : " + bn.getMainPrice() + " is invalid, isdn: " + bn.getIsdnCustomer());
                            }
                        } else {
                            logger.info("configure for mainPrice : " + bn.getMainPrice() + " is empty, isdn: " + bn.getIsdnCustomer());
                        }
                    }
                    if (bn.getVasCode() != null && !bn.getVasCode().isEmpty()) {
                        logger.info("Customer register vas with package: " + bn.getVasCode() + " isdn " + bn.getIsdnCustomer());
                        db.updateSubMbWhenActive(bn.getIsdnCustomer()); // Huynq13 20190904 add to support register vas after connecting kit by Staff (not Channel)
                        ProductConnectKit productConnectKit = db.getProductConnectKit(bn.getVasCode());
                        if (productConnectKit != null && productConnectKit.getMoneyFee() != null && !productConnectKit.getMoneyFee().isEmpty()) {
                            if (db.checkMovitelStaff(bn.getUserName())) {
                                comission += productConnectKit.getBonusCtv();
                            } else {
                                comission += productConnectKit.getBonusChannel();
                            }
                            String activeOnOCS = "";
                            if (!isActive) {
//                        Active subscriber first, after that topup
                                activeOnOCS = pro.activeOCSACTIVEFIRST("258" + bn.getIsdnCustomer());
                            } else {
                                activeOnOCS = "0";
                            }
                            if ("0".equals(activeOnOCS)) {
                                logger.info("Open flag OCSHW_ACTIVEFIRST successfully for sub: " + bn.getIsdnCustomer());
                                //                    Calling webservice
                                SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss");
                                Date now = new Date();
                                String strDate = sdfDate.format(now).replace("-", "").replace(".", "").replace(" ", "");
                                String requestIdTopup = "KIT_" + strDate + bn.getId();
                                String rsTopup = pro.topupWS(requestIdTopup, bn.getIsdnCustomer(), productConnectKit.getMoneyFee());
                                if ("0".equals(rsTopup)) {
                                    logger.info("Topup successfully for sub: " + bn.getIsdnCustomer() + ", vas_code: " + bn.getVasCode() + ", amount: " + productConnectKit.getMoneyFee());
                                    String connName = productConnectKit.getVasConnection();
                                    String param = productConnectKit.getVasParam();
                                    String actionType = productConnectKit.getVasActionType();
                                    String channel = productConnectKit.getVasChannel();
//                            Insert MO
                                    if (param != null && "?".equals(param)) {
                                        param = bn.getUserName();
                                    }
                                    int rsInsertMo = db.insertMO(bn.getIsdnCustomer(), bn.getVasCode(), connName, param, actionType, channel);
                                    if (rsInsertMo == 1) {
                                        logger.info("Insert MO successfully for sub: " + bn.getIsdnCustomer() + ", vas_code: " + bn.getVasCode() + ", amount: " + productConnectKit.getMoneyFee());
                                    } else {
                                        logger.info("Topup successfully but insert MO unsuccessfully for sub: " + bn.getIsdnCustomer() + ", vas_code: " + bn.getVasCode() + ", amount: " + productConnectKit.getMoneyFee());
                                    }
                                } else {
                                    logger.info("Topup unsuccessfully for sub: " + bn.getIsdnCustomer() + ", vas_code: " + bn.getVasCode() + ", amount: " + productConnectKit.getMoneyFee());
                                }
                            } else {
                                logger.warn("Open flag OCSHW_ACTIVEFIRST unsuccessfully, cannot topup for sub: " + bn.getIsdnCustomer() + ", result: " + activeOnOCS);
                            }

                        } else {
                            logger.info("Cannot get amount topup from VAS_CODE: " + bn.getVasCode() + ", isdn: " + bn.getIsdnCustomer());
                        }
                    } else {
                        logger.info("Customer register without vas, isdn " + bn.getIsdnCustomer());
                    }
                } else {
                    logger.warn("After validate respone code is fail isdn " + bn.getIsdnCustomer()
                            + " so continue with other transaction");
                    continue;
                }
            } else {
                //Profile not correct...send sms to customer to re-update again.
                if (bn.getSimSerial() != null) {
                    serial = bn.getSimSerial();
                } else {
                    serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                }
                logger.warn("Profile not correct  isdn " + bn.getIsdnCustomer());
                if ("2".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Incorrect Image, id " + bn.getId() + " isdn " + bn.getIsdnCustomer());
                    bn.setResultCode("E05");
                    bn.setDescription("Invalid profile Image");
                    String smsForCus = msgCusIncorrectImage.replace("%XYZ%", bn.getIsdnCustomer());
                    db.sendSms(bn.getIsdnCustomer(), smsForCus, "86904");

                    String phoneChannel = db.getTelByStaffCode(bn.getUserName());
                    String tempMsg = msgChannelIncorrectImage.replace("%XYZ%", serial);
                    db.sendSms(phoneChannel, tempMsg, "86142");
                    logger.info("Message send to staff: " + tempMsg + " isdn " + phoneChannel);
                    continue;
                }
                if ("3".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Incorrect Name, id " + bn.getId() + " isdn " + bn.getIsdnCustomer());
                    bn.setResultCode("E06");
                    bn.setDescription("Invalid profile Name");
                    String smsForCus = msgCusIncorrectName.replace("%XYZ%", bn.getIsdnCustomer());
                    db.sendSms(bn.getIsdnCustomer(), smsForCus, "86904");

                    String phoneChannel = db.getTelByStaffCode(bn.getUserName());
                    String tempMsg = msgChannelIncorrectName.replace("%XYZ%", serial);
                    db.sendSms(phoneChannel, tempMsg, "86142");
                    logger.info("Message send to staff: " + tempMsg + " isdn " + phoneChannel);
                    continue;
                }
                if ("4".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Incorrect ID No, id " + bn.getId() + " isdn " + bn.getIsdnCustomer());
                    bn.setResultCode("E07");
                    bn.setDescription("Invalid profile IDNo");
                    String smsForCus = msgCusIncorrectIdNo.replace("%XYZ%", bn.getIsdnCustomer());
                    db.sendSms(bn.getIsdnCustomer(), smsForCus, "86904");

                    String phoneChannel = db.getTelByStaffCode(bn.getUserName());
                    String tempMsg = msgChannelIncorrectIdNo.replace("%XYZ%", serial);
                    db.sendSms(phoneChannel, tempMsg, "86142");
                    logger.info("Message send to staff: " + tempMsg + " isdn " + phoneChannel);
                    continue;
                }
                if ("5".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Dim image, id " + bn.getId() + " isdn " + bn.getIsdnCustomer());
                    bn.setResultCode("E08");
                    bn.setDescription("Dim Image");
                    String smsForCus = msgCusDimImage.replace("%XYZ%", bn.getIsdnCustomer());
                    db.sendSms(bn.getIsdnCustomer(), smsForCus, "86904");

                    String phoneChannel = db.getTelByStaffCode(bn.getUserName());
                    String tempMsg = msgChannelDimImage.replace("%XYZ%", serial);
                    db.sendSms(phoneChannel, tempMsg, "86142");
                    logger.info("Message send to staff: " + tempMsg + " isdn " + phoneChannel);
                    continue;
                }
                if ("6".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Miss student card, id " + bn.getId() + " isdn " + bn.getIsdnCustomer());
                    bn.setResultCode("E09");
                    bn.setDescription("Miss student card");
                    String smsForCus = msgCusMissingStudentCard.replace("%XYZ%", bn.getIsdnCustomer());
                    db.sendSms(bn.getIsdnCustomer(), smsForCus, "86904");

                    String phoneChannel = db.getTelByStaffCode(bn.getUserName());
                    String tempMsg = msgChannelMissingStudentCard.replace("%XYZ%", serial);
                    db.sendSms(phoneChannel, tempMsg, "86142");
                    logger.info("Message send to staff: " + tempMsg + " isdn " + phoneChannel);
                    continue;
                }
                if ("7".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Not load Image, id " + bn.getId() + " isdn " + bn.getIsdnCustomer());
                    bn.setResultCode("E10");
                    bn.setDescription("Missing Image");
                    String smsForCus = msgCusMissingImage.replace("%XYZ%", bn.getIsdnCustomer());
                    db.sendSms(bn.getIsdnCustomer(), smsForCus, "86904");

                    String phoneChannel = db.getTelByStaffCode(bn.getUserName());
                    String tempMsg = msgChannelMissingImage.replace("%XYZ%", serial);
                    db.sendSms(phoneChannel, tempMsg, "86142");
                    logger.info("Message send to staff: " + tempMsg + " isdn " + phoneChannel);
                    continue;
                }
                if ("8".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Invalid student code, id " + bn.getId() + " isdn " + bn.getIsdnCustomer());
                    bn.setResultCode("E11");
                    bn.setDescription("Invalid profile student code");
                    String smsForCus = msgCusInvalidStudentCode.replace("%XYZ%", bn.getIsdnCustomer());
                    db.sendSms(bn.getIsdnCustomer(), smsForCus, "86904");

                    String phoneChannel = db.getTelByStaffCode(bn.getUserName());
                    String tempMsg = msgChannelInvalidStudentCode.replace("%XYZ%", serial);
                    db.sendSms(phoneChannel, tempMsg, "86142");
                    logger.info("Message send to staff: " + tempMsg + " isdn " + phoneChannel);
                    continue;
                }
                if ("11".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Document copy, id " + bn.getId() + " isdn " + bn.getIsdnCustomer());
                    bn.setResultCode("E12");
                    bn.setDescription("Document copy");
                    String smsForCus = msgCusDocumentCopy.replace("%XYZ%", bn.getIsdnCustomer());
                    db.sendSms(bn.getIsdnCustomer(), smsForCus, "86904");

                    String phoneChannel = db.getTelByStaffCode(bn.getUserName());
                    String tempMsg = msgChannelDocumentCopy.replace("%XYZ%", serial);
                    db.sendSms(phoneChannel, tempMsg, "86142");
                    logger.info("Message send to staff: " + tempMsg + " isdn " + phoneChannel);
                    continue;
                } else {
                    logger.warn("Invalid result check, id " + bn.getId() + " isdn " + bn.getIsdnCustomer());
                    bn.setResultCode("E13");
                    bn.setDescription("Invalid result check");
                    continue;
                }
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
                append("|\tACTION_AUDIT_ID|").
                append("|\tPK_ID|").
                append("|\tCREATE_STAFF\t|").
                append("|\tCREATE_SHOP\t|").
                append("|\tCREATE_TIME\t|").
                append("|\tCHECK_STAFF\t|").
                append("|\tCHECK_INFO\t|").
                append("|\tCHECK_TIME\t|").
                append("|\tISDN\t|").
                append("|\tBONUS_STATUS\t|").
                append("|\tREASON_ID\t|").
                append("|\tBLOCK_OCS_HLR\t|");
        for (Record record : listRecord) {
            Bonus bn = (Bonus) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getActionAuditId()).
                    append("||\t").
                    append(bn.getPkId()).
                    append("||\t").
                    append(bn.getUserName()).
                    append("||\t").
                    append(bn.getShopCode()).
                    append("||\t").
                    append((bn.getIssueDateTime() != null ? sdf.format(bn.getIssueDateTime()) : null)).
                    append("||\t").
                    append(bn.getStaffCheck()).
                    append("||\t").
                    append(bn.getCheckInfo()).
                    append("||\t").
                    append((bn.getTimeCheck() != null ? sdf.format(bn.getTimeCheck()) : null)).
                    append("||\t").
                    append(bn.getIsdnCustomer()).
                    append("||\t").
                    append(bn.getBonusStatus()).
                    append("||\t").
                    append(bn.getReasonId()).
                    append("||\t").
                    append(bn.getBlockOcsHlr());
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
}
