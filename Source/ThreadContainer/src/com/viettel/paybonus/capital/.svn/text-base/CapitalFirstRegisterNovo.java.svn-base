/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.Capital;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.database.DbFirstRegister;
import com.viettel.paybonus.obj.BounesMonthly;
import com.viettel.paybonus.obj.FirstRegister;
import com.viettel.paybonus.obj.StaffInfo;
import com.viettel.threadfw.manager.AppManager;
import java.util.List;
import org.apache.log4j.Logger;
import com.viettel.threadfw.process.ProcessRecordAbstract;
import com.viettel.vas.util.ExchangeClientChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ResourceBundle;
import com.viettel.paybonus.service.Exchange;
import java.util.Date;
import java.util.HashMap;

/**
 *
 * @author HuyNQ13
 * @version 1.0
 * @since 24-03-2016
 */
public class CapitalFirstRegisterNovo extends ProcessRecordAbstract {

    Exchange pro;
    DbFirstRegister db;
    SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmssSSS");
    Integer overDay;
    String smsForChannelNotPosition;
    String sMapBonusForChannelCapital;
    String sMapBonusForMasterChannelCapital;
    ArrayList<HashMap> lstMapBonusForChannel;
    ArrayList<HashMap> lstMapBonusForMasterChannel;

    public CapitalFirstRegisterNovo() {
        super();
        logger = Logger.getLogger(BounesMonthly.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbFirstRegister();
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        overDay = Integer.valueOf(ResourceBundle.getBundle("configPayBonus").getString("overDay"));
        sMapBonusForChannelCapital = ResourceBundle.getBundle("configPayBonus").getString("mapBonusForChannelCapital");
        String[] tmpMapBonus = sMapBonusForChannelCapital.split("\\|");
        lstMapBonusForChannel = new ArrayList<HashMap>();
        for (String reason : tmpMapBonus) {
            String[] tmp = reason.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapBonusForChannel.add(map);
        }

        sMapBonusForMasterChannelCapital = ResourceBundle.getBundle("configPayBonus").getString("mapBonusForMasterChannelCapital");
        String[] tmpMapBonusMas = sMapBonusForMasterChannelCapital.split("\\|");
        lstMapBonusForMasterChannel = new ArrayList<HashMap>();
        for (String reason : tmpMapBonusMas) {
            String[] tmp = reason.split("\\:");
            HashMap map = new HashMap();
            map.put(tmp[0].trim().toUpperCase(), tmp[1].trim());
            lstMapBonusForMasterChannel.add(map);
        }
    }

    @Override
    public List<Record> validateContraint(List<Record> listRecord) throws Exception {
        for (Record record : listRecord) {
            FirstRegister moRecord = (FirstRegister) record;
            moRecord.setNodeName(holder.getNodeName());
            moRecord.setClusterName(holder.getClusterName());
        }
        return listRecord;
    }

    @Override
    public List<Record> processListRecord(List<Record> listRecord) throws Exception {
        List<Record> listResult = new ArrayList<Record>();
        int moneyForChannel;
        int moneyForMaster;
        StaffInfo staff;
        StaffInfo staffOwner;
        for (Record record : listRecord) {
            FirstRegister bn = (FirstRegister) record;
            listResult.add(bn);
            staff = null;
            staffOwner = null;
            moneyForChannel = 0;
            moneyForMaster = 0;
            listResult.add(bn);
            try {
//          B1: Check dublicate
                logger.info("Start check dublicate with isdn " + bn.getIsdn());
                if (db.checkAlreadyProcessRecord(bn.getIsdn())) {
                    logger.warn("Already process record in first_register_capital_his with isdn=" + bn.getIsdn());
                    bn.setResultCode("CFR01");
                    bn.setDescription("Already process record in first_register_capital_his with");
                    continue;
                }
//          B2: Check waiting over time after register
                logger.info("Check waiting over time after register with isdn " + bn.getIsdn());
                int checkingOverTime = db.checkWaitingOverTime(bn.getIsdn());
                if (checkingOverTime == -1L) {
                    logger.warn("Can not compare time with create_time at cm_pre.sub_profile_info with isdn " + bn.getIsdn());
                    bn.setResultCode("CFR02");
                    bn.setDescription("Can not compare time with create_time at cm_pre.sub_profile_info with isdn " + bn.getIsdn());
                    continue;
                } else if (checkingOverTime > overDay) {
                    logger.warn("Over time compare with register at  cm_pre.sub_profile_info with isdn=" + bn.getIsdn());
                    bn.setResultCode("CFR03");
                    bn.setDescription("Over time compare with register at  cm_pre.sub_profile_info with isdn=" + bn.getIsdn());
                    continue;
                }

//          B3: Check PC Assign Area
                staff = db.getStaffInfo(bn.getIsdn().toString());
                if (staff == null) {
                    logger.warn("Can not get StaffInfo");
                    bn.setResultCode("CFR04");
                    bn.setDescription("Can not get StaffInfo");
                    continue;
                } else if (staff.getChannelTypeId() == 1000487 && staff.getAreaManageId() <= 0) {
                    logger.warn("Channel is PC but not assign to any Area isdn=" + bn.getIsdn());
                    bn.setResultCode("CFR05");
                    bn.setDescription("Channel is PC but not assign to any Area");
                    db.sendSms(staff.getIsdnWallet(), smsForChannelNotPosition, "155");
                    continue;
                }

//          B4: calculate bonus
                for (int i = 0; i < lstMapBonusForChannel.size(); i++) {
                    if (lstMapBonusForChannel.get(i).containsKey(bn.getPackageCode().trim().toUpperCase())) {
                        moneyForChannel = Integer.valueOf(lstMapBonusForChannel.get(i).get(bn.getPackageCode().trim().toUpperCase()).toString());
                        break;
                    }
                }
                for (int i = 0; i < lstMapBonusForMasterChannel.size(); i++) {
                    if (lstMapBonusForMasterChannel.get(i).containsKey(bn.getPackageCode().trim().toUpperCase())) {
                        moneyForMaster = Integer.valueOf(lstMapBonusForMasterChannel.get(i).get(bn.getPackageCode().trim().toUpperCase()).toString());
                        break;
                    }
                }

//          B5: Pay bonus for channel
                String description = "";
                if (moneyForChannel > 0) {
                    String eWalletResponse = pro.callEwallet(Long.valueOf(bn.getIsdn()), staff.getChannelTypeId(), staff.getIsdnWallet(),
                            moneyForChannel, "615", staff.getStaffCode(), sdf.format(new Date()), db);
                    bn.setEwalletErrCode(eWalletResponse);
                    if ("01".equals(eWalletResponse)) {
                        logger.info("Pay Bonus success for isdnEmola " + staff.getIsdnWallet() + " amount " + moneyForChannel + " isdn " + bn.getIsdn());
                        bn.setResultCode("0");
                        description = "Pay Bonus success for isdnEmola " + staff.getIsdnWallet() + " amount " + moneyForChannel;
                        bn.setDescription(description);
                        String msgChannelByPck = ResourceBundle.getBundle("configPayBonus").getString("smsForChannel_" + bn.getPackageCode().trim().toUpperCase());
                        if (msgChannelByPck != null && msgChannelByPck.trim().length() > 0) {
                            msgChannelByPck = msgChannelByPck.replace("%MT%", String.valueOf(moneyForChannel)).replace("%CHANNEL%", staff.getStaffCode());
                            db.sendSms(staff.getIsdnWallet(), msgChannelByPck, "155");
                        } else {
                            logger.error("Missing config message, so not send sms to channel " + bn.getIsdn());
                        }
                        bn.setEwalletErrCode(eWalletResponse);
                    } else {
                        logger.error("Fail to pay bonus for isdnEmola " + staff.getIsdnWallet() + " amount " + moneyForChannel);
                        bn.setResultCode("CFR06");
                        bn.setDescription("Fail to pay bonus to channel with error from emola is: "
                                + eWalletResponse + " isdnEmola=" + staff.getIsdnWallet() + " amount " + moneyForChannel);
                        continue;
                    }
                } else {
                    logger.warn("Can not map bonus money for channel pls check config isdn=" + bn.getIsdn());
                    bn.setResultCode("CFR07");
                    bn.setDescription("Can not map bonus money for channel pls check config isdn=" + bn.getIsdn());
                    continue;
                }

                String staffCodeChannel = staff.getStaffCode();

//          B4: Check staff owner 
                staffOwner = db.getStaffMaster(staff.getStaffOnerId());
                if (staffOwner == null) {
                    logger.warn("Can not get staffMaster");
                    bn.setResultCode("CFR08");
                    bn.setDescription("Can not get staffMaster");
                    continue;
                } else if (staffOwner.getIsdnWallet() == null || "".equals(staffOwner.getIsdnWallet())) {
                    logger.warn("Can not get isdn_wallet");
                    bn.setResultCode("CFR09");
                    bn.setDescription("Can not get isdn_wallet of staff");
                    continue;
                }
//          B6:  Pay bonus for Master channel
                if (moneyForMaster > 0) {
                    String eWalletResponse = pro.callEwallet(Long.valueOf(staffOwner.getIsdnWallet()), staffOwner.getChannelTypeId(), staffOwner.getIsdnWallet(),
                            moneyForMaster, "615", staffOwner.getStaffCode(), sdf.format(new Date()), db);
                    bn.setEwalletErrCode(eWalletResponse);
                    if ("01".equals(eWalletResponse)) {
                        logger.info("Pay Bonus success for isdnEmola "
                                + staffOwner.getIsdnWallet() + " amount " + moneyForMaster + " isdn " + staffOwner.getIsdnWallet());
                        bn.setResultCode("0");
                        bn.setDescription(description + "||Pay Bonus success for isdnEmola " + staffOwner.getIsdnWallet() + " amount " + moneyForMaster);
                        String msgMasterByPck = ResourceBundle.getBundle("configPayBonus").getString("smsForMaster_" + bn.getPackageCode().trim().toUpperCase());
                        if (msgMasterByPck != null && msgMasterByPck.trim().length() > 0) {
                            msgMasterByPck = msgMasterByPck.replace("%MT%", String.valueOf(moneyForMaster)).replace("%CHANNEL%", staffCodeChannel);
                            db.sendSms(staffOwner.getIsdnWallet(), msgMasterByPck, "155");
                        }
                        bn.setEwalletErrCode(eWalletResponse);
                        continue;
                    } else {
                        logger.error("Fail to pay bonus for isdnEmola " + staffOwner.getIsdnWallet() + " amount " + moneyForMaster);
                        bn.setResultCode("CFR10");
                        bn.setDescription("Fail to pay bonus to Master channel with error from emola is: "
                                + eWalletResponse + " isdnEmola=" + staffOwner.getIsdnWallet() + " amount " + moneyForMaster);
                        continue;
                    }
                } else {
                    logger.warn("Can not map bonus money for master-channel pls check config isdn=" + bn.getIsdn());
                    bn.setResultCode("CFR11");
                    bn.setDescription("Can not map bonus money for master-channel pls check config isdn=" + bn.getIsdn());
                    continue;
                }
            } catch (Exception e) {
                logger.error("Someting Error CapitalFirstRegister " + e.toString() + " so system delete Info " + bn.getIsdn());
                db.deleteFirstResister(bn.getIsdn().toString());
                bn.setResultCode("CFR99");
                bn.setDescription("Had exception " + e.toString());
                db.insertFirstResisterHis(bn);
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
                append("|\tisdn|").
                append("|\tcreate_date\t|").
                append("|\tpackage_code\t|");
        for (Record record : listRecord) {
            FirstRegister bn = (FirstRegister) record;
            br.append("\r\n").
                    append("|\t").
                    append(bn.getIsdn()).
                    append("||\t").
                    append((bn.getCreateDate() != null ? sdf.format(bn.getCreateDate()) : null)).
                    append("||\t").
                    append(bn.getPackageCode());
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
