/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbSubProfileProcessor;
import com.viettel.paybonus.obj.AccountAgent;
import com.viettel.paybonus.obj.Agent;
import com.viettel.paybonus.obj.Bonus;
import com.viettel.paybonus.obj.Customer;
import com.viettel.paybonus.obj.ProductConnectKit;
import com.viettel.paybonus.obj.SubInfo;
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
import java.util.ResourceBundle;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class RegisterProcess extends ProcessRecordAbstract {

    Exchange pro;
    DbSubProfileProcessor db;
    //LinhNBV modified on September 04 2017: Add variables for message
    String msgFalseInfoRegistrationPoint;
    String msgTrueRegistrationPoint;
    String msgTrueCustomer;
    String msgChannelReIncorrectImage;
    String msgChannelReIncorrectName;
    String msgChannelReIncorrectIdNo;
    String msgChannelReDimImage;
    String msgChannelReMissingStudentCard;
    String msgChannelReMissingImage;
    String msgChannelReInvalidStudentCode;
    String msgChannelReDocumentCopy;
    String msgCusReIncorrectImage;
    String msgCusReIncorrectName;
    String msgCusReIncorrectIdNo;
    String msgCusReDimImage;
    String msgCusReMissingStudentCard;
    String msgCusReMissingImage;
    String msgCusReInvalidStudentCode;
    String msgCusReDocumentCopy;
    Long reasonIdUpdateInfor;
    String msgBlockInfoOneWay;
    String branch;
    String[] lstBranch;
    String bonusRateBranch;
    String bonusRateSecondBranch;
    ArrayList<HashMap> mapBonusRateBranch;
    ArrayList<HashMap> mapBonusRateSecondBranch;
    SimpleDateFormat sdf2;
    String kitConnectActiveSaleHandset;

    public RegisterProcess() {
        super();
        logger = Logger.getLogger(RegisterProcess.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbSubProfileProcessor();
        msgFalseInfoRegistrationPoint = ResourceBundle.getBundle("configPayBonus").getString("msgFalseInfoRegistrationPoint");
        msgTrueRegistrationPoint = ResourceBundle.getBundle("configPayBonus").getString("msgTrueRegistrationPoint");
        msgTrueCustomer = ResourceBundle.getBundle("configPayBonus").getString("msgTrueCustomer");
        msgChannelReIncorrectImage = ResourceBundle.getBundle("configPayBonus").getString("msgChannelReIncorrectImage");
        msgChannelReIncorrectName = ResourceBundle.getBundle("configPayBonus").getString("msgChannelReIncorrectName");
        msgChannelReIncorrectIdNo = ResourceBundle.getBundle("configPayBonus").getString("msgChannelReIncorrectIdNo");
        msgChannelReDimImage = ResourceBundle.getBundle("configPayBonus").getString("msgChannelReDimImage");
        msgChannelReMissingStudentCard = ResourceBundle.getBundle("configPayBonus").getString("msgChannelReMissingStudentCard");
        msgChannelReMissingImage = ResourceBundle.getBundle("configPayBonus").getString("msgChannelReMissingImage");
        msgChannelReInvalidStudentCode = ResourceBundle.getBundle("configPayBonus").getString("msgChannelReInvalidStudentCode");
        msgChannelReDocumentCopy = ResourceBundle.getBundle("configPayBonus").getString("msgChannelReDocumentCopy");
        msgCusReIncorrectImage = ResourceBundle.getBundle("configPayBonus").getString("msgCusReIncorrectImage");
        msgCusReIncorrectName = ResourceBundle.getBundle("configPayBonus").getString("msgCusReIncorrectName");
        msgCusReIncorrectIdNo = ResourceBundle.getBundle("configPayBonus").getString("msgCusReIncorrectIdNo");
        msgCusReDimImage = ResourceBundle.getBundle("configPayBonus").getString("msgCusReDimImage");
        msgCusReMissingStudentCard = ResourceBundle.getBundle("configPayBonus").getString("msgCusReMissingStudentCard");
        msgCusReMissingImage = ResourceBundle.getBundle("configPayBonus").getString("msgCusReMissingImage");
        msgCusReInvalidStudentCode = ResourceBundle.getBundle("configPayBonus").getString("msgCusReInvalidStudentCode");
        msgCusReDocumentCopy = ResourceBundle.getBundle("configPayBonus").getString("msgCusReDocumentCopy");
        msgBlockInfoOneWay = ResourceBundle.getBundle("configPayBonus").getString("msgBlockInfoOneWay");
        reasonIdUpdateInfor = Long.parseLong(ResourceBundle.getBundle("configPayBonus").getString("reasonIdUpdateInforSmartPhone"));
        bonusRateBranch = ResourceBundle.getBundle("configPayBonus").getString("rate_bonus_branch");
        bonusRateSecondBranch = ResourceBundle.getBundle("configPayBonus").getString("rate_bonus_second_branch");
        branch = ResourceBundle.getBundle("configPayBonus").getString("list_bonus_branch");
        lstBranch = branch.split("\\|");
        String[] tmpMapBonus = bonusRateBranch.split("\\|");
        mapBonusRateBranch = new ArrayList<HashMap>();
        String[] tmpMapBonusSecond = bonusRateSecondBranch.split("\\|");
        mapBonusRateSecondBranch = new ArrayList<HashMap>();
        for (String rate : tmpMapBonus) {
            String[] tmp = rate.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            mapBonusRateBranch.add(map);
        }
        for (String rate : tmpMapBonusSecond) {
            String[] tmp = rate.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            mapBonusRateSecondBranch.add(map);
        }
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
        AccountAgent agent;
        Agent staffInfo;
        String staffCode;
        String isdn;
        SubInfo sub;
        long timeSt;
        long actionId;
        String openFlagBAOCResult;
        String openFlagBAICResult;
        String openFlagGPRSResult;
        String openFlagPLMResult;
        boolean payEmola;
        boolean isSim4G;
        String flagRoaming;
        String flagData;
        SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
        long comission;
        for (Record record : listRecord) {
            timeSt = System.currentTimeMillis();
            agent = null;
            staffInfo = null;
            staffCode = "";
            isdn = "";
            sub = null;
            Bonus bn = (Bonus) record;
            listResult.add(bn);
            openFlagBAOCResult = "";
            openFlagBAICResult = "";
            openFlagGPRSResult = "";
            openFlagPLMResult = "";
            payEmola = false;
            isSim4G = false;
            flagRoaming = "";
            flagData = "";
            comission = 0;
            if ("0".equals(bn.getResultCode())) {
//                Check account ewallet on SM to get isdn ewallet, staff_code
                staffCode = bn.getUserName();
                if ("".equals(staffCode)) {
                    logger.warn("CreateStaff in Sub_Profile_Info is null or empty, id " + bn.getId());
                    bn.setResultCode("01");
                    bn.setDescription("CreateStaff in Sub_Profile_Info is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                } else {
                    bn.setStaffCode(staffCode);
                }
                if (bn.getIsdnCustomer() == null || bn.getIsdnCustomer().trim().length() <= 0) {
                    logger.warn("ISDN is null or empty, id " + bn.getId());
                    bn.setResultCode("02");
                    bn.setDescription("ISDN is null or empty");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//                Check invalid image
                if ("2".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Invalid image, id " + bn.getId() + " isdn " + bn.getIsdnCustomer());
                    if (!reasonIdUpdateInfor.equals(bn.getReasonId())) {
                        String tel = db.getTelByStaffCode(staffCode);
                        String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                        String tempMsgChannel = msgChannelReIncorrectImage.replace("%XYZ%", serial);
                        db.sendSms("258" + tel, tempMsgChannel, "86142");
                        String tempMsgCus = msgCusReIncorrectImage.replace("%XYZ%", bn.getIsdnCustomer());
                        db.sendSms("258" + bn.getIsdnCustomer(), tempMsgCus, "86904");
                        logger.info("Message send to registration point: " + tempMsgChannel + " isdn " + tel);
                        logger.info("Message send to customer: " + tempMsgCus + " isdn " + bn.getIsdnCustomer());
                    }
                    bn.setResultCode("04");
                    bn.setDescription("Invalid profile");//                  
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//                Check invalid info
                if ("3".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Invalid Name, id " + bn.getId() + " isdn " + bn.getIsdnCustomer());
                    //LinhNBV start modified on September 04 2017: Add message false send to Registration Point.
                    if (!reasonIdUpdateInfor.equals(bn.getReasonId())) {
                        String tel = db.getTelByStaffCode(staffCode);
                        String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                        String tempMsgChannel = msgChannelReIncorrectName.replace("%XYZ%", serial);
                        db.sendSms("258" + tel, tempMsgChannel, "86904");
                        String tempMsgCus = msgCusReIncorrectName.replace("%XYZ%", bn.getIsdnCustomer());
                        db.sendSms("258" + bn.getIsdnCustomer(), tempMsgCus, "86904");
                        logger.info("Message send to registration point: " + tempMsgChannel + " isdn " + tel);
                        logger.info("Message send to customer: " + tempMsgCus + " isdn " + bn.getIsdnCustomer());
                    }
                    //LinhNBV end.
                    bn.setResultCode("03");
                    bn.setDescription("Invalid profile Info");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//              Check invalid both image and info
                if ("4".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Invalid Id No, id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                    //LinhNBV start modified on September 04 2017: Add message false send to Registration Point.
                    if (!reasonIdUpdateInfor.equals(bn.getReasonId())) {
                        String tel = db.getTelByStaffCode(staffCode);
                        String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                        String messageFalseRegistrationPoint = msgChannelReIncorrectIdNo.replace("%XYZ%", serial);
                        db.sendSms("258" + tel, messageFalseRegistrationPoint, "86142");
                        String tempMsgCus = msgCusReIncorrectIdNo.replace("%XYZ%", bn.getIsdnCustomer());
                        db.sendSms("258" + bn.getIsdnCustomer(), tempMsgCus, "86904");
                        logger.info("Message send to registration point: " + messageFalseRegistrationPoint + " isdn " + tel);
                        logger.info("Message send to customer: " + tempMsgCus + " isdn " + bn.getIsdnCustomer());
                    }
                    //End.
                    bn.setResultCode("05");
                    bn.setDescription("Invalid both image and info");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//              Check dim image and info
                if ("5".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Dim image, id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                    //LinhNBV start modified on September 04 2017: Add message false send to Registration Point.
                    if (!reasonIdUpdateInfor.equals(bn.getReasonId())) {
                        String tel = db.getTelByStaffCode(staffCode);
                        String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                        String messageFalseRegistrationPoint = msgChannelReDimImage.replace("%XYZ%", serial);
                        db.sendSms("258" + tel, messageFalseRegistrationPoint, "86142");
                        String tempMsgCus = msgCusReDimImage.replace("%XYZ%", bn.getIsdnCustomer());
                        db.sendSms("258" + bn.getIsdnCustomer(), tempMsgCus, "86904");
                        logger.info("Message send to registration point: " + messageFalseRegistrationPoint + " isdn " + tel);
                        logger.info("Message send to customer: " + tempMsgCus + " isdn " + bn.getIsdnCustomer());
                    }
                    //End.
                    bn.setResultCode("06");
                    bn.setDescription("Dim image");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
                //              Check missing student card
                if ("6".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Missing student card, id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                    //LinhNBV start modified on September 04 2017: Add message false send to Registration Point.
                    if (!reasonIdUpdateInfor.equals(bn.getReasonId())) {
                        String tel = db.getTelByStaffCode(staffCode);
                        String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                        String messageFalseRegistrationPoint = msgChannelReMissingStudentCard.replace("%XYZ%", serial);
                        db.sendSms("258" + tel, messageFalseRegistrationPoint, "86142");
                        String tempMsgCus = msgCusReMissingStudentCard.replace("%XYZ%", bn.getIsdnCustomer());
                        db.sendSms("258" + bn.getIsdnCustomer(), tempMsgCus, "86904");
                        logger.info("Message send to registration point: " + messageFalseRegistrationPoint + " isdn " + tel);
                        logger.info("Message send to customer: " + tempMsgCus + " isdn " + bn.getIsdnCustomer());
                    }
                    bn.setResultCode("20");
                    bn.setDescription("Missing student card");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
                //              Check missing image
                if ("7".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Missing image, id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                    //LinhNBV start modified on September 04 2017: Add message false send to Registration Point.
                    if (!reasonIdUpdateInfor.equals(bn.getReasonId())) {
                        String tel = db.getTelByStaffCode(staffCode);
                        String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                        String messageFalseRegistrationPoint = msgChannelReMissingImage.replace("%XYZ%", serial);
                        db.sendSms("258" + tel, messageFalseRegistrationPoint, "86142");
                        String tempMsgCus = msgCusReMissingImage.replace("%XYZ%", bn.getIsdnCustomer());
                        db.sendSms("258" + bn.getIsdnCustomer(), tempMsgCus, "86904");
                        logger.info("Message send to registration point: " + messageFalseRegistrationPoint + " isdn " + tel);
                        logger.info("Message send to customer: " + tempMsgCus + " isdn " + bn.getIsdnCustomer());
                    }
                    bn.setResultCode("21");
                    bn.setDescription("Missing image");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//              Check invalid student card
                if ("8".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Invalid student code, id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                    //LinhNBV start modified on September 04 2017: Add message false send to Registration Point.
                    if (!reasonIdUpdateInfor.equals(bn.getReasonId())) {
                        String tel = db.getTelByStaffCode(staffCode);
                        String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                        String messageFalseRegistrationPoint = msgChannelReInvalidStudentCode.replace("%XYZ%", serial);
                        db.sendSms("258" + tel, messageFalseRegistrationPoint, "86904");
                        logger.info("Message send to registration point: " + messageFalseRegistrationPoint + " isdn " + tel);
                        String tempMsgCus = msgCusReInvalidStudentCode.replace("%XYZ%", bn.getIsdnCustomer());
                        logger.info("Message send to customer: " + tempMsgCus + " isdn " + bn.getIsdnCustomer());
                        db.sendSms("258" + bn.getIsdnCustomer(), tempMsgCus, "86904");
                    }
                    bn.setResultCode("22");
                    bn.setDescription("Invalid student card");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//              Check photo copy
                if ("11".equals(bn.getCheckInfo().trim())) {
                    logger.warn("Photo copy, id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                    if (!reasonIdUpdateInfor.equals(bn.getReasonId())) {
                        String tel = db.getTelByStaffCode(staffCode);
                        String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                        String messageFalseRegistrationPoint = msgChannelReDocumentCopy.replace("%XYZ%", serial);
                        db.sendSms("258" + tel, messageFalseRegistrationPoint, "86904");
                        logger.info("Message send to registration point: " + messageFalseRegistrationPoint + " isdn " + tel);
                        String tempMsgCus = msgCusReDocumentCopy.replace("%XYZ%", bn.getIsdnCustomer());
                        logger.info("Message send to customer: " + tempMsgCus + " isdn " + bn.getIsdnCustomer());
                        db.sendSms("258" + bn.getIsdnCustomer(), tempMsgCus, "86904");
                    }
                    bn.setResultCode("23");
                    bn.setDescription("Photo copy");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//                Check correct profile
                if ("1".equals(bn.getCheckInfo().trim())) {
                    if (!reasonIdUpdateInfor.equals(bn.getReasonId())) {
//                        Flow step...
//                        HLR_HW_MOD_BAOC
//                        HLR_HW_MOD_BAIC
//                        HLR_HW_MOD_PLMNSS
//                        4G: HLR_HW_MODI_SUB_LOCK_4G/ 3G: HLR_HW_MODI_SUB_GPRS
//                        HLR_HW_MOD_ODBROAM

                        openFlagBAOCResult = pro.activeFlagBAOC("258" + bn.getIsdnCustomer());
                        if ("0".equals(openFlagBAOCResult)) {
                            logger.info("Open flag BAOC successfully for sub when register info " + bn.getIsdnCustomer());
                        } else {
                            logger.warn("Fail to open flag BAOC for sub when register info " + bn.getIsdnCustomer());
                        }
                        openFlagBAICResult = pro.activeFlagBAIC("258" + bn.getIsdnCustomer());
                        if ("0".equals(openFlagBAICResult)) {
                            logger.info("Open flag BAIC successfully for sub when register info " + bn.getIsdnCustomer());
                        } else {
                            logger.warn("Fail to open flag BAIC for sub when register info " + bn.getIsdnCustomer());
                        }
                        openFlagPLMResult = pro.activeFlagPLMNSS("258" + bn.getIsdnCustomer());
                        if ("0".equals(openFlagPLMResult)) {
                            logger.info("Open flag PLMNSS successfully for sub when register info " + bn.getIsdnCustomer());
                        } else {
                            logger.warn("Fail to open flag PLMNSS for sub when register info " + bn.getIsdnCustomer());
                        }
//                        LinhNBV 20190117: Unlock Flag for SIM 4G.
                        if (bn.getSimSerial() != null) {
                            isSim4G = db.isSim4G(bn.getSimSerial(), 207607L);
                            if (isSim4G) {
                                logger.info("Begin open FLAG for 4G subscriber..." + bn.getIsdnCustomer());
                                flagData = pro.modDataFlag("258" + bn.getIsdnCustomer(), "FALSE", "FALSE");
                                if ("0".equals(flagData)) {
                                    logger.info("Open flag DATA 3G/4G successfully for sub when register info " + bn.getIsdnCustomer());
                                } else {
                                    logger.warn("Fail to open flag DATA 3G/4G for sub when register info " + bn.getIsdnCustomer());
                                }
                            } else {
                                openFlagGPRSResult = pro.activeFlagGPRSLCK("258" + bn.getIsdnCustomer());
                                if ("0".equals(openFlagGPRSResult)) {
                                    logger.info("Open flag GPRS successfully for sub when register info " + bn.getIsdnCustomer());
                                } else {
                                    logger.warn("Fail to open flag GPRS for sub when register info " + bn.getIsdnCustomer());
                                }
                            }
                        } else {
                            openFlagGPRSResult = pro.activeFlagGPRSLCK("258" + bn.getIsdnCustomer());
                            if ("0".equals(openFlagGPRSResult)) {
                                logger.info("Open flag GPRS successfully for sub when register info " + bn.getIsdnCustomer());
                            } else {
                                logger.warn("Fail to open flag GPRS for sub when register info " + bn.getIsdnCustomer());
                            }
                        }
                        flagRoaming = pro.modRoamingFlag("258" + bn.getIsdnCustomer(), "NOBAR");
                        if ("0".equals(flagRoaming)) {
                            logger.info("Open flag ROAMING successfully for sub when register info " + bn.getIsdnCustomer());
                        } else {
                            logger.warn("Fail to open flag ROAMING for sub when register info " + bn.getIsdnCustomer());
                        }
                    }
                    if (!reasonIdUpdateInfor.equals(bn.getReasonId())
                            && (bn.getSimSerial() == null || bn.getSimSerial().trim().length() <= 0)) {
//                        Only register for KIT
                        String tel = db.getTelByStaffCode(staffCode);
                        String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                        String tempMsgChannel = msgTrueRegistrationPoint.replace("%XYZ%", serial);
                        db.sendSms("258" + tel, tempMsgChannel, "86904");
                        String messageTrueCustomer = msgTrueCustomer.replace("%XYZ%", bn.getIsdnCustomer());
                        db.sendSms("258" + bn.getIsdnCustomer(), messageTrueCustomer, "86904");
                        logger.info("Message send to registration point: " + tempMsgChannel + " isdn " + tel);
                        logger.info("Message send to customer: " + messageTrueCustomer + " isdn " + bn.getIsdnCustomer());
                    }
                } else {
                    //                    Huynq13 20180523 add to check default value
                    logger.warn("Default invalid Info, id " + bn.getId() + " isdn " + bn.getIsdnCustomer()
                            + " check status " + bn.getCheckInfo().trim());
                    if (!reasonIdUpdateInfor.equals(bn.getReasonId())) {
                        String tel = db.getTelByStaffCode(staffCode);
                        String serial = db.getSerialByIsdnCustomer(bn.getIsdnCustomer());
                        String messageFalseRegistrationPoint = msgFalseInfoRegistrationPoint.replace("%XYZ%", serial);
                        db.sendSms("258" + tel, messageFalseRegistrationPoint, "86142");
                        logger.info("Message send to registration point: " + messageFalseRegistrationPoint + " isdn " + tel);
                    }
                    bn.setResultCode("23");
                    bn.setDescription("Default invalid profile Info");
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }
//                Huynq13 20170816 start add to open flag when correct profile

//                LinhNBV 20180809: create eMola account for customer having correct profile
                if ("1".equals(bn.getRequestEmola())) {
                    Customer customer = db.getCustomerByCustId(bn.getCustId());
                    pro.createEmolaForCustomer(customer, bn.getIsdnCustomer(), db);
                } else {
                    logger.info("Customer no need to create emola account: " + " isdn ------ staffCode: " + bn.getStaffCode());
                }
//                LinhNBV 20180809 end.
//                Huynq13 20170816 end add to open flag when correct profile    
//                LinhNBV 20190313: Check for Connect Kit By Batch...
                if (bn.getKitBatchId() == null || bn.getKitBatchId() <= 0) {
                    logger.info("KitBatchId is null, that mean connect kit single mode, not connect kit by batch...id "
                            + bn.getId() + " isdn: " + bn.getIsdnCustomer());
//                LinhNBV 20180809: Modify Id_no cannot pay Emola. modify_type = 1 >>> modify, null register or update new
                    if (bn.getModifyType() == null || (!"1".equals(bn.getModifyType()) && !"4".equals(bn.getModifyType()))) {
                        logger.info("Start check pay by Emola " + bn.getActionAuditId());
                        staffInfo = db.getAgentInfoByUser(staffCode);
                        if (staffInfo == null) {
                            logger.warn("Can not get Staff Info, user: " + staffCode + " actionId " + bn.getActionAuditId()
                                    + " id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                            bn.setResultCode("07");
                            bn.setDescription("Can not get staff Info");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            continue;
                        } else if (staffInfo.getIsdnWallet() != null && staffInfo.getIsdnWallet().length() > 0) {
//                    Huynq13 20171102 add to check channel have contract for pay via eMola
                            if (db.checkChannelHaveContract(staffCode)) {
                                payEmola = true;
                            }
//                    LinhNBV 20180702: Check staff of Movitel pay bonus
                            if (db.checkMovitelStaff(staffCode)) {
                                payEmola = true;
                            }
                        }
//                LinhNBV 20200307: pay money for user connect with handset
                        if (bn.getHandsetModel() != null && !bn.getHandsetModel().isEmpty()) {
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
                                    logger.info("start pay money for user connect with handset: " + stockModelCode
                                            + ", user: " + bn.getStaffCode() + ", isdn: " + bn.getIsdnCustomer());
                                    String priceTypeConfig = db.getBasedConfigConnectKit(staffCode, "revenue_type_handset");
                                    String[] arrPriceTypeConfig = priceTypeConfig.split("\\|");
                                    String shopPath = db.getShopPathByStaffCode(staffCode);
                                    String[] arrShopPath = shopPath.split("\\_");//_7282_shopConfig_shopChildren
                                    String shopId = "";
                                    if (arrShopPath.length > 2) {
                                        shopId = arrShopPath[2];
                                    } else {
                                        shopId = arrShopPath[1];
                                    }
                                    String pricePolicy = db.getPricePolicyHandset(mainProductConnect, stockModelCode, shopId);
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
                                                        bn.getActionCode(), staffCode, sdf.format(new Date()), 6, db);
                                                String tel = db.getTelByStaffCode(staffCode);
                                                db.sendSms("258" + tel, "Voce recebeu o total de " + comissionHandset + " MT por vender o telemovel " + stockModelCode + ". Obrigado!", "86142");
                                                logger.info("end pay commission for refill, response: " + eWalletResponse + ", isdn: " + bn.getIsdnCustomer());
                                            }
                                            break;
                                        }
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

                        }
                        if (payEmola) {
                            logger.info("payEmola is true so no need get AgentInfo more " + bn.getActionAuditId());
                            isdn = staffInfo.getIsdnWallet();
                            bn.setIsdn(isdn);
                            bn.setAgentId(staffInfo.getStaffId());
                            bn.setChannelTypeId(staffInfo.getChannelTypeId());
                        } else {
                            logger.info("Start get AgentInfo " + bn.getActionAuditId());
                            agent = db.getAccountAgentByUser(staffCode);
                            if (agent == null || agent.getIsdn() == null || agent.getIsdn().length() <= 0) {
                                logger.warn("Can not get AccountAgent Info, user: " + staffCode + " actionId " + bn.getActionAuditId()
                                        + " id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                                isdn = bn.getIsdnCustomer();
                                bn.setIsdn("");
                                bn.setChannelTypeId(staffInfo.getChannelTypeId());
                            } else {
                                isdn = agent.getIsdn();
                                bn.setIsdn(isdn);
                                bn.setAgentIsdn(isdn);
                                bn.setAccountId(agent.getAccountId());
                                bn.setAgentId(agent.getAgentId());
                                bn.setChannelTypeId(agent.getChannelTypeId());
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
                                                isActive = false;
                                                logger.warn("Open flag OCSHW_ACTIVEFIRST unsuccessfully, can not add benifit: " + arrConfig[0] + ", for sub: " + bn.getIsdnCustomer() + ", result: " + activeOnOCS);
                                            }
                                        }
                                    } else {
                                        logger.info("don't configure benefit for mainPrice : " + bn.getMainPrice() + ", isdn: " + bn.getIsdnCustomer());
                                    }
                                    if (Long.valueOf(arrConfig[1]) > 0) {
                                        logger.info("start pay comission for refill, mainPrice: " + bn.getMainPrice() + ", isdn: " + bn.getIsdnCustomer());
                                        String eWalletResponse = pro.callEwallet(bn.getActionAuditId(), bn.getChannelTypeId(), staffInfo.getIsdnWallet(), comission,
                                                bn.getActionCode(), staffCode, sdf.format(new Date()), db);
                                        logger.info("end pay commission for refill, response: " + eWalletResponse + ", isdn: " + bn.getIsdnCustomer());
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
                                if (db.checkMovitelStaff(staffCode)) {
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
                                    Date now = new Date();
                                    String strDate = sdf2.format(now);
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
                                            param = staffCode;
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
                        }
//                Get actioncode
                        logger.info("Start get actioncode for actionId " + bn.getActionAuditId()
                                + " id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                        actionId = db.getActionId(bn.getActionAuditId(), bn.getActionCode());
                        if (actionId <= 0) {
                            logger.warn("Can not get actionId from actionCode " + bn.getActionCode()
                                    + " id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                            bn.setResultCode("11");
                            bn.setDescription("Can not get actionId from actionCode " + bn.getActionCode());
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            continue;
                        } else {
                            bn.setActionId(actionId);
                        }
//                Check sub info
                        if (bn.getPkId() == null) {
                            logger.warn("Do not have Sub or Cust Id, actionId " + bn.getActionAuditId() + " isdn: " + isdn);
                        } else {
                            logger.info("Start check SubInfo actionId " + bn.getActionAuditId() + " pkid " + bn.getPkId()
                                    + " id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                            sub = db.getSubInfoBySubId(bn.getPkId(), isdn);
                            if (sub == null || sub.getProductCode() == null || sub.getProductCode().length() <= 0) {
                                logger.warn("Can not find subscriber info actionId " + bn.getActionAuditId()
                                        + " pkid " + bn.getPkId() + " id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                                bn.setResultCode("15");
                                bn.setDescription("Can not find subscriber info");
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                continue;
                            } else {
                                bn.setProductCode(sub.getProductCode());
                                bn.setActiveStatus(sub.getActStatus());
                                bn.setActiveDate(sub.getActiveDate());
                            }
                        }
                        bn.setItemFeeId(0L);
                        bn.setAmount(0L);
                        bn.setSecondAmount(0L);
                        db.insertBonusFirstSimHandset(bn);
                        if (payEmola) {
                            //              Call eWallet to add bonus
                            if (!reasonIdUpdateInfor.equals(bn.getReasonId())) {
//                        20180924 start change new rule not pay now, only pay when sub is actived call any phone or 150
                                logger.info("Not Pay Bonus now, only pay when refill scratch card for actionId " + bn.getActionAuditId() + " isdnEmola "
                                        + isdn);
                                bn.setResultCode("26");
                                bn.setDescription("Not Pay Bonus now, only pay when refill scratch card for actionId " + isdn);
                                bn.setDuration(System.currentTimeMillis() - timeSt);
                                if (comission <= 0) {
//                                    Check Elite dont pay second commission
                                    ProductConnectKit productConnectKit = db.getProductConnectKit(bn.getProductCode());
                                    if (productConnectKit != null && productConnectKit.getMoneyFee() != null && !productConnectKit.getMoneyFee().isEmpty()) {
                                        if (db.checkMovitelStaff(staffCode)) {
                                            comission += productConnectKit.getBonusCtv();
                                        } else {
                                            comission += productConnectKit.getBonusChannel();
                                        }
                                    }
                                    if (comission <= 0) {
                                        db.insertBonusSecond(bn);
                                    }
                                }
                                continue;
                            }
                        } else {
                            //                        2018/11/13 off Anypay so not pay more
                            logger.warn("Not support to pay by AnyPay more " + bn.getActionAuditId() + " id " + bn.getId());
                            bn.setResultCode("19");
                            bn.setDescription("Not support to pay by AnyPay more");
                            bn.setDuration(System.currentTimeMillis() - timeSt);
                            continue;
                        }
                    } else {
                        logger.error("CC had modifid id_no for this profile so not pay bonus " + bn.getActionAuditId()
                                + " id " + bn.getId() + " isdn: " + bn.getIsdnCustomer());
                        bn.setResultCode("25");
                        bn.setDescription("CC had modifid id_no for this profile so not pay bonus " + isdn);
                        bn.setDuration(System.currentTimeMillis() - timeSt);
                        continue;
                    }
                } else {
                    logger.info("KitBatchId is NOT NULL, that mean  connect kit by batch...id "
                            + bn.getKitBatchId() + " isdn: " + bn.getIsdnCustomer()
                            + ", only open flag when profile is correct, no need payBonus.");
                    bn.setResultCode("0");
                    bn.setDescription("Open FLAG successfully for KitBatch, id of batch: " + bn.getKitBatchId());
                    bn.setDuration(System.currentTimeMillis() - timeSt);
                    continue;
                }

            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getActionAuditId()
                        + " id " + bn.getId() + " isdn: " + bn.getIsdnCustomer()
                        + " so continue with other transaction");
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
        return listRecord;
    }

    @Override
    public boolean startProcessRecord() {
        return true;
    }
}
