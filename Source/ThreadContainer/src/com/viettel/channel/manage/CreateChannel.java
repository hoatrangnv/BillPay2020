/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.channel.manage;

import com.google.gson.Gson;
import com.itextpdf.text.PageSize;
import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbCreateChannelProcessor;
import com.viettel.paybonus.obj.RequestChannel;
import com.viettel.paybonus.obj.ResponseWallet;
import com.viettel.paybonus.obj.Staff;
import com.viettel.paybonus.obj.SubscriberInfo;
import com.viettel.paybonus.service.EWalletUtil;
import com.viettel.paybonus.service.Exchange;
import com.viettel.paybonus.service.PdfBussinessDAL;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.ResourceBundle;
import org.apache.commons.net.ftp.FTPClient;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class CreateChannel extends ProcessRecordAbstract {

    Exchange pro;
    DbCreateChannelProcessor db;
    String ERR_000, ERR_009, ERR_012, ERR_011, ERR_004;
    String ERR_013;
    String ERR_015;
    String ERR_014;
    String SUC_001, SUC_002, SUC_003;
    String[] byPassEmolaErr;
    String[] channelIdCreateEmola;
    ArrayList<String> listByPassEmolaErr;
    ArrayList<String> blackListChannelIdCreateEmola;
    String lstIsdnReceiveError;
    String[] arrIsdnReceiveError;
    String path;

    public CreateChannel() {
        super();
        logger = Logger.getLogger(CreateChannel.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        db = new DbCreateChannelProcessor("dbsm", logger);
        ERR_000 = "Your request to create the channel is failed. Please check syntax again. Channel ISDN: ";
        ERR_004 = "Your request to create the channel is failed. ISDN of the channel has not been registered the information: ";
        ERR_012 = "Your request to create the channel is failed. Please check syntax again. Channel not found with Prefix: ";
        ERR_011 = "Your request to create the channel is failed. StaffID not found: ";
        ERR_009 = "Your request to create the channel is failed. Because do not have ID_NO for isdn: ";
        ERR_013 = "Your request to create the channel E-Mola is failed. Phone of Wallet is undefined: ";
        ERR_015 = "Your request to create the channel E-Mola is failed. Phone of Wallet is duplicate: ";
        SUC_003 = "Your request to create the channel E-Mola is done. ISDN/Channel code: ";
        ERR_014 = "Your request to create the channel E-Mola is failed. Check again connect to system E-Mola Wallet ";
        SUC_001 = "Your request to create the channel is done. ISDN/Channel code:  ";
        SUC_002 = "Welcome you to become a channel of MOV. Your ISDN/Channel code: ";

        ResourceBundle configList = ResourceBundle.getBundle("configPayBonus");
        byPassEmolaErr = configList.getString("ByPassEmolaErrorCode").split(";");
        channelIdCreateEmola = configList.getString("BlackListChannelIdCreateEmola").split(";");
        listByPassEmolaErr = new ArrayList();
        blackListChannelIdCreateEmola = new ArrayList();
        listByPassEmolaErr.addAll(Arrays.asList(byPassEmolaErr));
        blackListChannelIdCreateEmola.addAll(Arrays.asList(channelIdCreateEmola));

        lstIsdnReceiveError = ResourceBundle.getBundle("configPayBonus").getString("lstIsdnReceiveError");
        arrIsdnReceiveError = lstIsdnReceiveError.split("\\|");
        path = System.getProperty("user.dir").replace("/bin", "").replace("build", "");
        logger.info("Path contains temp file is: " + path);
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        String smsForChannelIsdn;
        String smsForRequestIsdn;
        String requestUserIsdn;
        String channelIsdn;
        String channelPrefix;
        String staffCode;
        String isdnWallet;
        Long shopId;
        Long staffId;
        String dob = null;
        String idIssueDate = null;
        String parentIdWallet = null;
        ResponseWallet responseWallet = new ResponseWallet();
        String channelTypeId;
        String discountPolicyDefault;
        String pricePolicyDefault;
        Staff addStaff;

        for (Record record : listRecord) {
            RequestChannel bn = (RequestChannel) record;
            listResult.add(bn);
            if ("0".equals(bn.getResultCode())) {
                logger.info("Step 1: Create channel first... " + bn.getStaffId() + " id " + bn.getId());
                Date sysdate = new Date();
                addStaff = new Staff();
                requestUserIsdn = bn.getRequestUserIsdn();
                channelIsdn = bn.getChannelIsdn();
                channelPrefix = bn.getChannelPrefix();
                staffId = bn.getStaffId();
                requestUserIsdn = requestUserIsdn == null ? "" : requestUserIsdn.trim();
                if (requestUserIsdn.startsWith("0")) {
                    requestUserIsdn = requestUserIsdn.substring(1);
                }
                channelIsdn = channelIsdn == null ? "" : channelIsdn.trim();
                if (channelIsdn.startsWith("0")) {
                    channelIsdn = channelIsdn.substring(1);
                }
                channelPrefix = channelPrefix == null ? "" : channelPrefix.trim();
                if (requestUserIsdn.equals("")) {
                    smsForChannelIsdn = ERR_000 + "0" + channelIsdn;
                    db.sendSms(channelIsdn, smsForChannelIsdn, "86904");
                    logger.info(smsForChannelIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                    bn.setStatus(-1L);
                    bn.setDescription(smsForChannelIsdn);
                    bn.setContractStatus(-1L);
                    continue;
                }
                String channelType = db.getChannelType(channelPrefix);
                if (channelType.length() <= 0) {
                    smsForChannelIsdn = ERR_012 + "0" + channelPrefix;
                    db.sendSms(channelIsdn, smsForChannelIsdn, "86904");
                    logger.info(smsForChannelIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                    bn.setStatus(-1L);
                    bn.setContractStatus(-1L);
                    bn.setDescription(smsForChannelIsdn);
                    continue;
                }
                String[] arrChannelType = channelType.split("\\|");
                channelTypeId = arrChannelType[0];
                discountPolicyDefault = arrChannelType[1];
                pricePolicyDefault = arrChannelType[2];

                String staffInfo = db.getStaffInfo(staffId);
                if (staffInfo.length() <= 0) {
                    smsForChannelIsdn = ERR_011 + staffId;
                    db.sendSms(channelIsdn, smsForChannelIsdn, "86904");
                    logger.info(smsForChannelIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                    bn.setStatus(-1L);
                    bn.setContractStatus(-1L);
                    bn.setDescription(smsForChannelIsdn);
                    continue;
                }
                String[] arrStaffInfo = staffInfo.split("\\|");
                shopId = Long.valueOf(arrStaffInfo[0]);
                staffCode = arrStaffInfo[1];
                String province = db.getShopInfo(shopId);
                String provinceReference = db.getProvinceReference(province);
                String ownerCode = db.getStaffCodeSeqIsNotVt(provinceReference + channelPrefix);
                logger.info("ChannelCode will be create: " + ownerCode + ", staffOwnwerId: " + bn.getStaffId());

                SubscriberInfo allCustSub = db.getSubscriberInfo(channelIsdn);
                if (allCustSub == null) {
                    smsForRequestIsdn = ERR_004 + "0" + channelIsdn;
                    if (requestUserIsdn.equals("")) {
                        db.sendSms(channelIsdn, smsForRequestIsdn, "86904");
                    } else {
                        db.sendSms(requestUserIsdn, smsForRequestIsdn, "86904");
                    }
                    //Cap nhat trang thai yeu cau
                    logger.info(smsForRequestIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                    bn.setStatus(-1L);
                    bn.setContractStatus(-1L);
                    bn.setDescription("Cannot get Subscriber Info.");
                    continue;
                }

                if (allCustSub.getIdNo() == null) {
                    smsForRequestIsdn = ERR_009 + "0" + channelIsdn;
                    if (requestUserIsdn.equals("")) {
                        db.sendSms(channelIsdn, smsForRequestIsdn, "86904");
                    } else {
                        db.sendSms(requestUserIsdn, smsForRequestIsdn, "86904");
                    }
                    //Cap nhat trang thai yeu cau
                    logger.info(smsForRequestIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                    bn.setStatus(-1L);
                    bn.setContractStatus(-1L);
                    bn.setDescription("Cannot get Id no of Subscriber.");
                    continue;
                }

                addStaff.setStaffId(String.valueOf(db.getSequence("STAFF_SEQ", "dbsm")));

                if (bn.getChannelWallet() != null
                        && bn.getIsdnWallet() != null
                        && bn.getTypeAction() != null) {
                    isdnWallet = bn.getIsdnWallet().trim();

                    if (isdnWallet.equals("")) {
                        //Thong bao loi va continue
                        smsForRequestIsdn = ERR_013 + "0" + isdnWallet;
                        db.sendSms(requestUserIsdn, smsForRequestIsdn, "86904");
                        logger.info(smsForRequestIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                        bn.setStatus(-1L);
                        bn.setContractStatus(-1L);
                        bn.setDescription("Isdn Wallet is empty.");
                        continue;
                    }
                    if (db.checkIsdnWallet(isdnWallet)) {
                        smsForRequestIsdn = ERR_015 + "0" + isdnWallet;
                        db.sendSms(requestUserIsdn, smsForRequestIsdn, "86904");
                        logger.info(smsForRequestIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                        bn.setStatus(-1L);
                        bn.setContractStatus(-1L);
                        bn.setDescription("Exist isdnWallet with another staff.");
                        continue;
                    }
                    if (allCustSub.getBirthDate() != null) {
                        SimpleDateFormat date = new SimpleDateFormat("ddMMyyyy");
                        dob = date.format(allCustSub.getBirthDate());
                    }

                    if (allCustSub.getIdIssueDate() != null) {
                        SimpleDateFormat date = new SimpleDateFormat("ddMMyyyy");
                        idIssueDate = date.format(allCustSub.getIdIssueDate());
                    }
                    if (bn.getParentIdWallet() != null) {
                        parentIdWallet = bn.getParentIdWallet().toString();
                    }
                    //Goi API ben vi
                    if (blackListChannelIdCreateEmola.contains("" + channelTypeId)) {
                        logger.info("Do not create eMola Customer account because channeltypeid is in blacklist "
                                + channelIsdn + " " + channelTypeId);
                    } else {
                        String response = EWalletUtil.createCustomerEmolaAccount(db, logger, allCustSub.getCustId(), bn.getChannelName(), channelIsdn,
                                bn.getChannelName(), "1", dob, "1", allCustSub.getIdNo(), allCustSub.getCustAddress(),
                                "1", addStaff.getStaffId().toString());
                        logger.info("response createCustomerEmolaAccount isdn " + channelIsdn + ": " + response);
                        if ("ERROR".equals(response)) {
                            smsForRequestIsdn = ERR_014 + "0" + channelIsdn;
                            db.sendSms(requestUserIsdn, smsForRequestIsdn, "86904");
                            logger.info(smsForRequestIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                            bn.setStatus(-1L);
                            bn.setContractStatus(-1L);
                            bn.setDescription("Error from ewallet system.");
                            continue;
                        } else {
                            Gson gson = new Gson();
                            responseWallet = gson.fromJson(response, ResponseWallet.class);
                            if (responseWallet != null && responseWallet.getResponseCode() != null) {
                                if (listByPassEmolaErr.contains(responseWallet.getResponseCode())) {
                                    logger.info("Create EMola customer account for isdn " + channelIsdn + " is success");
                                    addStaff.setChannelWallet(bn.getChannelWallet());
                                    addStaff.setIsdnWallet(isdnWallet);
                                    addStaff.setParentIdWallet(String.valueOf(bn.getParentIdWallet()));
                                    smsForRequestIsdn = SUC_003 + "0" + isdnWallet;
                                    db.sendSms(requestUserIsdn, smsForRequestIsdn, "86904");
                                    logger.info(smsForRequestIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                                } else {
                                    smsForRequestIsdn = responseWallet.getResponseMessage() + ":0" + channelIsdn;
                                    db.sendSms(requestUserIsdn, smsForRequestIsdn, "86904");
                                    logger.info(smsForRequestIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                                    bn.setStatus(-1L);
                                    bn.setContractStatus(-1L);
                                    bn.setDescription("Error from ewallet system. Error code: " + responseWallet.getResponseCode() + ", message: " + responseWallet.getResponseMessage());
                                    continue;
                                }
                            }
                        }
                    }
                } else {
                    if (blackListChannelIdCreateEmola.contains("" + channelTypeId)) {
                        logger.info("Do not create eMola Customer account because channeltypeid is in blacklist "
                                + channelIsdn + " " + channelTypeId);
                    } else {
                        String response = EWalletUtil.createCustomerEmolaAccount(db, logger, allCustSub.getCustId(), bn.getChannelName(), channelIsdn,
                                bn.getChannelName(), "1", dob, "1", allCustSub.getIdNo(), allCustSub.getCustAddress(),
                                "1", addStaff.getStaffId().toString());
                        logger.info("response createCustomerEmolaAccount isdn " + channelIsdn + ": " + response);
                        if ("ERROR".equals(response)) {
                            smsForRequestIsdn = ERR_014 + "0" + channelIsdn;
                            db.sendSms(requestUserIsdn, smsForRequestIsdn, "86904");
                            logger.info(smsForRequestIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                            bn.setStatus(-1L);
                            bn.setContractStatus(-1L);
                            bn.setDescription("Error from ewallet system.");
                            continue;
                        } else {
                            Gson gson = new Gson();
                            responseWallet = gson.fromJson(response, ResponseWallet.class);
                            if (responseWallet != null && responseWallet.getResponseCode() != null) {
                                if (listByPassEmolaErr.contains(responseWallet.getResponseCode())) {
                                    logger.info("Create EMola customer account for isdn " + channelIsdn + " is success");
                                } else {
                                    smsForRequestIsdn = responseWallet.getResponseMessage() + ":0" + channelIsdn;
                                    db.sendSms(requestUserIsdn, smsForRequestIsdn, "86904");
                                    logger.info(smsForRequestIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                                    bn.setStatus(-1L);
                                    bn.setContractStatus(-1L);
                                    bn.setDescription("Error from ewallet system. Error code: " + responseWallet.getResponseCode() + ", message: " + responseWallet.getResponseMessage());
                                    continue;
                                }
                            }
                        }
                    }
                }
                addStaff.setShopId(String.valueOf(shopId));
                addStaff.setStaffCode(ownerCode.toUpperCase());
                addStaff.setTradeName(bn.getChannelName());
                addStaff.setContactName(bn.getChannelName());
                addStaff.setName(bn.getChannelName());
                addStaff.setStaffOwnerId(String.valueOf(staffId));

                addStaff.setIdNo(bn.getbINumber());
                addStaff.setBirthday(allCustSub.getBirthDate());
                addStaff.setIdIssueDate(allCustSub.getIdIssueDate());
                addStaff.setIdIssuePlace(allCustSub.getIdIssuePlace());

                addStaff.setProvince(allCustSub.getProvince());
                addStaff.setAddress(allCustSub.getCustAddress());

                addStaff.setChannelTypeId(channelTypeId);
                addStaff.setDiscountPolicy(discountPolicyDefault);
                addStaff.setPricePolicy(pricePolicyDefault);
                addStaff.setStatus("1");
                addStaff.setRegistryDate(sysdate);
                addStaff.setTel(channelIsdn);

                addStaff.setLastUpdateUser(staffCode);
                addStaff.setLastUpdateTime(sysdate);
                addStaff.setCreateMethod(bn.getCreateObject());
                addStaff.setBtsCode(bn.getBtsCode());
                addStaff.setImei(bn.getImei());
                addStaff.setAnotherPhone(bn.getAnotherPhone());
                addStaff.setDistrict(bn.getDistrict());
                addStaff.setPrecinct(bn.getPrecint());
                addStaff.setStreetName(bn.getStreet());
                addStaff.setSerial(bn.getSerial());
                if (addStaff.getIsdnWallet() == null || addStaff.getIsdnWallet().trim().length() <= 0) {
                    if ("6".equals(bn.getTypeAction())) {
                        addStaff.setIsdnWallet(bn.getExistIsdnWallet());
                    }
                }
                if (bn.getImei() != null && bn.getImei().trim().length() > 0) {
                    addStaff.setRegistrationPoint("RA");
                }
                addStaff.setLastUpdateKey(bn.getLastUpdateKey());
                if (bn.getAreaManageId() != null) {
                    addStaff.setAreaManageId(String.valueOf(bn.getAreaManageId()));
                }
//                Create new record in staff table...
                int rsInsertStaff = db.insertStaff(addStaff);
                if (rsInsertStaff == 1) {
                    logger.info("Insert to staff successfully, staffCode: " + addStaff.getStaffCode() + ", staffOwnerId: " + staffId);
                } else {
                    for (String isdn : arrIsdnReceiveError) {
                        db.sendSms("258" + isdn, "Cannot insert new record to staff's table, Maybe have exception. Check now...channelCode: " + addStaff.getStaffCode() + ", request_channel_id: " + bn.getId(), "86904");
                    }

                    logger.info("Cannot insert new record to staff's table, Maybe have exception, staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                    bn.setStatus(-1L);
                    bn.setContractStatus(-1L);
                    bn.setDescription("Cannot insert new record to staff's table.");
                    continue;
                }
//                Create new record in staff_location...
                if (bn.getTypeAction() != null
                        && (bn.getTypeAction().equals("1") || bn.getTypeAction().equals("6"))
                        && bn.getCreateObject() != null
                        && bn.getCreateObject().equals("SMARTPHONE")) {
                    //thuc hien Insert lai du lieu vao bang Staff_location  tren DB Smart_Phone
                    String x = "";
                    String y = "";
                    String imgUrl = "";
                    String imgUrl1 = "";
                    String imgUrl2 = "";
                    String imgPath = "";
                    String staff_Code = "";
                    String staff_id = "";
                    String staff_owner_id = "";
                    Date lastUpdateTime = sysdate;
                    String channel_type_id = "";
                    if (bn.getImgUrl() != null) {
                        imgUrl = bn.getImgUrl();
                    }
                    if (bn.getImgUrl1() != null) {
                        imgUrl1 = bn.getImgUrl1();
                    }
                    if (bn.getImgUrl2() != null) {
                        imgUrl2 = bn.getImgUrl2();
                    }
                    if (bn.getImgPath() != null) {
                        imgPath = bn.getImgPath();
                    }
                    if (addStaff.getStaffId() != null) {
                        staff_id = addStaff.getStaffId();
                    }
                    if (addStaff.getStaffCode() != null) {
                        staff_Code = addStaff.getStaffCode();
                    }
                    if (addStaff.getStaffOwnerId() != null) {
                        staff_owner_id = addStaff.getStaffOwnerId();
                    }
                    if (addStaff.getLastUpdateTime() != null) {
                        lastUpdateTime = addStaff.getLastUpdateTime();
                    }
                    if (addStaff.getChannelTypeId() != null) {
                        channel_type_id = addStaff.getChannelTypeId();
                    }
                    if (bn.getX() != null) {
                        x = bn.getX();
                    }
                    if (bn.getY() != null) {
                        y = bn.getY();
                    }
                    logger.info("Begin insert staff_location: Param0 staff_id:" + staff_id + "Param1:" + staff_owner_id + "Param2:" + channel_type_id);
                    db.insertStaffLocation(staff_id, staff_Code, staff_owner_id, lastUpdateTime, channel_type_id,
                            x, y, imgUrl, imgUrl1, imgUrl2, imgPath);
                }
//                insertStockOwnerTmp
                db.insertStockOwnerTmp(Long.valueOf(addStaff.getChannelTypeId()), addStaff.getStaffCode(), addStaff.getName(), Long.valueOf(addStaff.getStaffId()), 2L);
                //Nhan tin ket qua
                smsForRequestIsdn = SUC_001 + "0" + channelIsdn + "/" + addStaff.getStaffCode();
                smsForChannelIsdn = SUC_002 + "0" + channelIsdn + "/" + addStaff.getStaffCode();
                if (!requestUserIsdn.equals("")) {
                    db.sendSms(requestUserIsdn, smsForRequestIsdn, "86904");
                    logger.info(smsForRequestIsdn + ", staffId: " + bn.getStaffId() + ", id: " + bn.getId());
                }
                db.sendSms(channelIsdn, smsForChannelIsdn, "86904");
                //Cap nhat trang thai yeu cau
                bn.setStatus(1L);

//                Create contract...
                String HOST_FTP = "10.229.42.55";
                String USER = "scan_doc";
                String PASS = "8dw29Jk$3d";
                logger.info("HOST: " + HOST_FTP + "\nUSER: " + USER + "\nPASS: " + PASS);
                FTPClient ftpClient = new FTPClient();
                ftpClient.connect(HOST_FTP);
                ftpClient.login(USER, PASS);
                ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);// Used for
                ftpClient.setFileTransferMode(FTPClient.BINARY_FILE_TYPE);
                ftpClient.enterLocalPassiveMode();
                ftpClient.setSoTimeout(30000); // default 1
                ftpClient.setDataTimeout(30000); // default 1
                int reply = ftpClient.getReplyCode();
                logger.info("ReplyCode from FTP server: " + reply);

                SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
                String dateDir = sdf.format(sysdate);

//                String frontBI = "D:\\Restart\\ThreadContainer\\etc\\tmpImage\\" + bn.getImgUrl1();
//                String backBI = "D:\\Restart\\ThreadContainer\\etc\\tmpImage\\" + bn.getImgUrl2();
//                String signBI = "D:\\Restart\\ThreadContainer\\etc\\tmpImage\\" + bn.getImgUrl3();
                String frontBI = path + "/etc/tmpImage/" + bn.getImgUrl1();
                String backBI = path + "/etc/tmpImage/" + bn.getImgUrl2();
                String signBI = path + "/etc/tmpImage/" + bn.getImgUrl3();

                String dirImg = bn.getImgPath().split("\\/")[1];
                FileOutputStream fos = new FileOutputStream(frontBI);
                ftpClient.retrieveFile("/mBCCS_CONTRACT_CHANNEL_IMG/" + dirImg + "/" + bn.getImgUrl1(), fos);

                fos = new FileOutputStream(backBI);
                ftpClient.retrieveFile("/mBCCS_CONTRACT_CHANNEL_IMG/" + dirImg + "/" + bn.getImgUrl2(), fos);

                fos = new FileOutputStream(signBI);
                ftpClient.retrieveFile("/mBCCS_CONTRACT_CHANNEL_IMG/" + dirImg + "/" + bn.getImgUrl3(), fos);

                fos.close();

                ftpClient.changeWorkingDirectory("AUTO_GEN_CONTRACT_POS");
                if (!dateDir.isEmpty()) {
                    ftpClient.makeDirectory(dateDir);
                    ftpClient.changeWorkingDirectory(dateDir);
                }

                com.itextpdf.text.Document documentPos = new com.itextpdf.text.Document(PageSize.A4);
                SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHH24mmss");
                Date createTimeContract = sysdate;
                String posContractName = sdf1.format(createTimeContract) + "_" + addStaff.getStaffCode() + "_POS_Contract.pdf";
                logger.info("Begin generate contract for channel...contractName: " + posContractName);
                PdfBussinessDAL pdfReg = new PdfBussinessDAL(path + "/etc/tmpUpload/" + posContractName, documentPos);
//                PdfBussinessDAL pdfReg = new PdfBussinessDAL("D:\\Restart\\ThreadContainer\\etc\\tmpUpload\\" + posContractName, documentPos);
                SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
                Long contractNo = db.getSequence("CONTRACT_NO_SEQ", "dbsm");
                pdfReg.createContractRegistrationPoint(bn, sdf2.format(sysdate).substring(0, 4) + "-" + String.format("%04d", contractNo), sdf2.format(sysdate), frontBI, backBI,
                        signBI, ftpClient, path);
//                pdfReg.createContractRegistrationPoint(bn, addStaff.getStaffId(), sdf2.format(sysdate), frontBI,
//                        backBI, signBI, ftpClient, path);
                String minutesHandover = "";
                if (bn.getEquipmentInfo() != null && bn.getEquipmentInfo().length() > 0) {
                    minutesHandover = sdf1.format(createTimeContract) + "_" + addStaff.getStaffCode() + "_Minutes_Handover.pdf";
                    com.itextpdf.text.Document documentHandover = new com.itextpdf.text.Document(PageSize.A4);
//                    PdfBussinessDAL pdfHandover = new PdfBussinessDAL("D:\\Restart\\ThreadContainer\\etc\\tmpUpload\\" + minutesHandover, documentHandover);
                    PdfBussinessDAL pdfHandover = new PdfBussinessDAL(path + "/etc/tmpUpload/" + minutesHandover, documentHandover);

                    pdfHandover.createMinutesHandover(bn, addStaff.getStaffId(), sdf2.format(sysdate), signBI, ftpClient, path);
                }


                String emolaContractName = "";
                if (bn.getChannelWallet() != null
                        && bn.getIsdnWallet() != null
                        && bn.getTypeAction() != null) {
                    String address = db.getAddress(province, bn.getDistrict(), bn.getPrecint());
                    bn.setAddress(address);
                    emolaContractName = sdf1.format(createTimeContract) + "_" + addStaff.getStaffCode() + "_eMola_Contract.pdf";
                    logger.info("Begin generate emola contract for channel...contractName: " + emolaContractName);
                    com.itextpdf.text.Document documentEmola = new com.itextpdf.text.Document(PageSize.A4);
                    PdfBussinessDAL pdf = new PdfBussinessDAL(path + "/etc/tmpUpload/" + emolaContractName, documentEmola);
                    pdf.createEMolaContract(bn, sdf2.format(sysdate), signBI, ftpClient, path);
                }

//                File fileFrontBI = new File("D:\\Restart\\ThreadContainer\\etc\\tmpImage\\" + bn.getImgUrl1());
//                File fileBackBI = new File("D:\\Restart\\ThreadContainer\\etc\\tmpImage\\" + bn.getImgUrl2());
//                File fileSignBI = new File("D:\\Restart\\ThreadContainer\\etc\\tmpImage\\" + bn.getImgUrl3());

                File fileFrontBI = new File(path + "/etc/tmpUpload/" + bn.getImgUrl1());
                File fileBackBI = new File(path + "/etc/tmpUpload/" + bn.getImgUrl2());
                File fileSignBI = new File(path + "/etc/tmpUpload/" + bn.getImgUrl3());

                fileFrontBI.delete();
                fileBackBI.delete();
                fileSignBI.delete();
                //Step 3: Update path contract 
                String pathContract = "/u01/scan_doc/AUTO_GEN_CONTRACT_POS/" + dateDir + "/" + posContractName;
                if (minutesHandover.length() > 0) {
                    pathContract = pathContract + "|" + "/u01/scan_doc/AUTO_GEN_CONTRACT_POS/" + dateDir + "/" + minutesHandover;
                }
                if (emolaContractName.length() > 0) {
                    pathContract = pathContract + "|" + "/u01/scan_doc/AUTO_GEN_CONTRACT_POS/" + dateDir + "/" + emolaContractName;
                }
                db.updateContractPath(pathContract, addStaff.getStaffCode());

                if (ftpClient.isConnected()) {
                    try {
                        ftpClient.logout();
                        ftpClient.disconnect();
                    } catch (IOException f) {
                        f.printStackTrace();
                    }
                }
                bn.setContractStatus(1L);
                bn.setDescription("Create channel successfully, generate contract success. Channel Code: " + addStaff.getStaffCode());
            } else {
                logger.warn("After validate respone code is fail actionId " + bn.getId()
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
        br.setLength(0);
        br.append("\r\n").
                append("|\tID|").
                append("|\tREQUEST_USER_ISDN\t|").
                append("|\tSTAFF_ID\t|").
                append("|\tCHANNEL_NAME\t|").
                append("|\tCHANNEL_ISDN\t|").
                append("|\tCHANNEL_PREFIX\t|").
                append("|\tCHANNEL_WALLET\t|").
                append("|\tISDN_WALLET\t|").
                append("|\tDISTRICT\t|").
                append("|\tPRECINCT\t|").
                append("|\tLAST_UPDATE_KEY\t|");
        for (Record record : listRecord) {
            RequestChannel bn = (RequestChannel) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getId()).
                    append("||\t").
                    append(bn.getRequestUserIsdn()).
                    append("||\t").
                    append(bn.getStaffId()).
                    append("||\t").
                    append(bn.getChannelName()).
                    append("||\t").
                    append(bn.getChannelIsdn()).
                    append("||\t").
                    append(bn.getChannelPrefix()).
                    append("||\t").
                    append(bn.getChannelWallet()).
                    append("||\t").
                    append(bn.getIsdnWallet()).
                    append("||\t").
                    append(bn.getDistrict()).
                    append("||\t").
                    append(bn.getPrecint()).
                    append("||\t").
                    append(bn.getLastUpdateKey());
        }
        logger.info(br);
    }

    @Override
    public List<Record> processException(List<Record> listRecord, Exception ex) {
        logger.warn("TEMPLATE process exception record: " + ex.toString());
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
