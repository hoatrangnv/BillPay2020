/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.branch.process;

import com.viettel.cluster.agent.integration.Record;
import com.viettel.paybonus.obj.BranchPromotionBts;
import com.viettel.paybonus.obj.BranchPromotionConfig;
import com.viettel.paybonus.obj.BranchPromotionMonitor;
import com.viettel.paybonus.obj.BranchPromotionSub;
import com.viettel.paybonus.service.Exchange;
import com.viettel.paybonus.database.DbPromotionMonitor;
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

/**
 *
 * @author itbl-jony
 * @version 1.0
 * @since 15-01-2019
 */
public class PromotionMonitor extends ProcessRecordAbstract {

    Exchange pro;
    DbPromotionMonitor db;
    Calendar calNow = Calendar.getInstance();
    Calendar calCompare = Calendar.getInstance();
    List<BranchPromotionConfig> config = new ArrayList<BranchPromotionConfig>();
    int hour;
    int minute;
    String branchSmsOutZone;
    String branchSmsInZone;

    public PromotionMonitor() {
        super();
        logger = Logger.getLogger(PromotionMonitor.class);
    }

    @Override
    public void initBeforeStart() throws Exception {
        db = new DbPromotionMonitor();
        config = db.getConfig();
        pro = new Exchange(ExchangeClientChannel.getInstance(AppManager.pathExch).getInstanceChannel(), logger);
        hour = Integer.parseInt(ResourceBundle.getBundle("configPayBonus").getString("branchHourDelay"));
        minute = Integer.parseInt(ResourceBundle.getBundle("configPayBonus").getString("branchMinuteDelay"));
        branchSmsOutZone = ResourceBundle.getBundle("configPayBonus").getString("branchSmsOutZone");
        branchSmsInZone = ResourceBundle.getBundle("configPayBonus").getString("branchSmsInZone");
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
        BranchPromotionMonitor bpm;
        String description;
        BranchPromotionBts checkPolicyMapBts;
        for (Record record : listRecord) {
            mscNumCus = "";
            cellIdCus = "";
            cellCodeCus = "";
            bpc = null;
            description = "";
            bpm = null;
            checkPolicyMapBts = null;
            BranchPromotionSub bps = (BranchPromotionSub) record;
            listResult.add(bps);
            for (BranchPromotionConfig bpcTmp : config) {
                if (bpcTmp.getPromotionCode().equals(bps.getProductCode()) && bpcTmp.getExtendMode() != null) {
                    bpc = bpcTmp;
                    break;
                }
            }
            description = "Monitor Isdn: " + bps.getIsdn() + "|";
            if (bpc != null) {
                bpm = db.getLastRecord(bps.getIsdn());
                if (bpm == null || bpm.getActionType() == null) {
//                Neu chua quet lan nao
                    //B1 lay ra GetMscId va IMEI
                    mscNumCus = pro.getMSCInfor(bps.getIsdn(), "");
                    if (mscNumCus.trim().length() <= 0) {
                        logger.warn("Can not get mscNumCus with ISDN= " + bps.getIsdn());
                        bps.setResultCode("PM01");
                        bps.setActionType("2");
                        description = removePrice(bps, bpc, description);
                        bps.setDescription(description + "Can not get mscNumCus");
                        continue;
                    }

                    //B3:   Kiem tra thue bao dang thuoc BTS nao 
                    cellIdCus = pro.getCellIdRsString(bps.getIsdn(), mscNumCus, "");
                    if (cellIdCus.trim().length() <= 0) {
                        logger.warn("Can not get Cell with mscNumCus " + mscNumCus + " ISDN= " + bps.getIsdn());
                        bps.setResultCode("PM03");
                        bps.setActionType("2");
                        description = removePrice(bps, bpc, description);
                        bps.setDescription(description + "Can not get Cell with mscNumCus " + mscNumCus);
                        continue;
                    }

                    String[] arrCellId = cellIdCus.split("\\|");
                    if ((arrCellId != null) && (arrCellId.length == 2)) {
                        cellCodeCus = db.getCell("", arrCellId[0], arrCellId[1]);
                        if (cellCodeCus.trim().length() <= 0) {
                            logger.warn("Can not get CellCode " + bps.getIsdn() + " ID  " + bps.getID());
                            bps.setResultCode("PM04");
                            bps.setActionType("2");
                            description = removePrice(bps, bpc, description);
                            bps.setDescription(description + "Can not get BTS online ");
                            continue;
                        } else {
                            bps.setLastBtsCode(cellCodeCus);
                        }
                    } else {
                        logger.warn("Invalid cellIdCus " + bps.getIsdn() + " ID  " + bps.getID());
                        bps.setResultCode("PM05");
                        bps.setActionType("2");
                        description = removePrice(bps, bpc, description);
                        bps.setDescription(description + "Invalid cellIdCus ");
                        continue;
                    }
                    checkPolicyMapBts = db.getPolicyMapBts(cellCodeCus, bps.getProductCode());
                    if (checkPolicyMapBts == null) {
                        logger.warn("only apply with BTS " + cellCodeCus.trim().toUpperCase() + " , isdn :  " + bps.getIsdn() + " ActionAuditId " + bps.getActionAuditId());
                        bps.setResultCode("PM06");
                        bps.setActionType("2");
                        description = removePrice(bps, bpc, description);
                        bps.setDescription(description + "only apply with BTS " + cellCodeCus.trim().toUpperCase());
                        continue;
                    }
                    bps.setResultCode("PM00");
                    bps.setActionType("1");
                    bps.setDescription(description + " Already online on " + cellCodeCus.trim().toUpperCase());
                }
                if ("2".equals(bpm.getActionType())) {
//                Neu da quet va lan cuoi khong an song tai tram tuc la da duoc remove ma gia roi=> retry lan nua
                    //B1 lay ra GetMscId va IMEI
                    mscNumCus = pro.getMSCInfor(bps.getIsdn(), "");
                    if (mscNumCus.trim().length() <= 0) {
                        logger.warn("Can not get mscNumCus with ISDN= " + bps.getIsdn());
                        bps.setResultCode("PM07");
                        bps.setActionType("2");
                        bps.setDescription(description + "Can not get mscNumCus");
                        continue;
                    }

                    //B3:   Kiem tra thue bao dang thuoc BTS nao 
                    cellIdCus = pro.getCellIdRsString(bps.getIsdn(), mscNumCus, "");
                    if (cellIdCus.trim().length() <= 0) {
                        logger.warn("Can not get Cell with mscNumCus " + mscNumCus + " ISDN= " + bps.getIsdn());
                        bps.setResultCode("PM09");
                        bps.setActionType("2");
                        bps.setDescription(description + "Can not get Cell with mscNumCus " + mscNumCus);
                        continue;
                    }

                    String[] arrCellId = cellIdCus.split("\\|");
                    if ((arrCellId != null) && (arrCellId.length == 2)) {
                        cellCodeCus = db.getCell("", arrCellId[0], arrCellId[1]);
                        if (cellCodeCus.trim().length() <= 0) {
                            logger.warn("Can not get CellCode " + bps.getIsdn() + " ID  " + bps.getID());
                            bps.setResultCode("PM10");
                            bps.setActionType("2");
                            bps.setDescription(description + "Can not get BTS online ");
                            continue;
                        } else {
                            bps.setLastBtsCode(cellCodeCus);
                        }
                    } else {
                        logger.warn("Invalid cellIdCus " + bps.getIsdn() + " ID  " + bps.getID());
                        bps.setResultCode("PM11");
                        bps.setActionType("2");
                        bps.setDescription(description + "Invalid cellIdCus ");
                        continue;
                    }

                    checkPolicyMapBts = db.getPolicyMapBts(cellCodeCus, bps.getProductCode());
                    if (checkPolicyMapBts == null) {
                        logger.warn("only apply with BTS " + cellCodeCus.trim().toUpperCase() + " , isdn :  " + bps.getIsdn() + " ActionAuditId " + bps.getActionAuditId());
                        bps.setResultCode("PM12");
                        bps.setActionType("2");
                        bps.setDescription(description + "only apply with BTS " + cellCodeCus.trim().toUpperCase());
                        continue;
                    }
                    bps.setResultCode("PM13");
                    bps.setActionType("1");
                    description = addPrice(bps, bpc, description);
                    bps.setDescription(description + " Already online on " + cellCodeCus.trim().toUpperCase());
                    if (!db.checkAlreadyWarningInDay(bps.getIsdn(), branchSmsInZone)
                            && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), branchSmsInZone)) {
                        db.sendSms(bps.getIsdn(), branchSmsInZone, "86904");
                    }
                }
                if ("1".equals(bpm.getActionType())) {
//             Neu lan cuoi quet mà van con an song tai tram => kiem tra gio hien tai với lan cuoi quet
                    calNow.setTime(new Date());
                    calNow.add(Calendar.HOUR, -hour);
                    calNow.add(Calendar.MINUTE, -minute);

                    calCompare.setTime(bpm.getProcessTime());
                    if (calCompare.getTime().before(calNow.getTime())) {
                        //B1 lay ra GetMscId va IMEI
                        mscNumCus = pro.getMSCInfor(bps.getIsdn(), "");
                        if (mscNumCus.trim().length() <= 0) {
                            logger.warn("Can not get mscNumCus with ISDN= " + bps.getIsdn());
                            bps.setResultCode("PM14");
                            bps.setActionType("2");
                            description = removePrice(bps, bpc, description);
                            bps.setDescription(description + "Can not get mscNumCus");
                            continue;
                        }
                        //B3:   Kiem tra thue bao dang thuoc BTS nao 
                        cellIdCus = pro.getCellIdRsString(bps.getIsdn(), mscNumCus, "");
                        if (cellIdCus.trim().length() <= 0) {
                            logger.warn("Can not get Cell with mscNumCus " + mscNumCus + " ISDN= " + bps.getIsdn());
                            bps.setResultCode("PM16");
                            bps.setActionType("2");
                            description = removePrice(bps, bpc, description);
                            bps.setDescription(description + "Can not get Cell with mscNumCus " + mscNumCus);
                            continue;
                        }

                        String[] arrCellId = cellIdCus.split("\\|");
                        if ((arrCellId != null) && (arrCellId.length == 2)) {
                            cellCodeCus = db.getCell("", arrCellId[0], arrCellId[1]);
                            if (cellCodeCus.trim().length() <= 0) {
                                logger.warn("Can not get CellCode " + bps.getIsdn() + " ID  " + bps.getID());
                                bps.setResultCode("PM17");
                                bps.setActionType("2");
                                description = removePrice(bps, bpc, description);
                                bps.setDescription(description + "Can not get BTS online ");
                                continue;
                            } else {
                                bps.setLastBtsCode(cellCodeCus);
                            }
                        } else {
                            logger.warn("Invalid cellIdCus " + bps.getIsdn() + " ID  " + bps.getID());
                            bps.setResultCode("PM18");
                            bps.setActionType("2");
                            bps.setDescription(description + "Invalid cellIdCus ");
                            continue;
                        }

                        checkPolicyMapBts = db.getPolicyMapBts(cellCodeCus, bps.getProductCode());
                        if (checkPolicyMapBts == null) {
                            logger.warn("only apply with BTS " + cellCodeCus.trim().toUpperCase() + " , isdn :  " + bps.getIsdn() + " ActionAuditId " + bps.getActionAuditId());
                            bps.setResultCode("PM19");
                            bps.setActionType("2");
                            description = removePrice(bps, bpc, description);
                            bps.setDescription(description + "only apply with BTS " + cellCodeCus.trim().toUpperCase());
                            if (!db.checkAlreadyWarningInDay(bps.getIsdn(), branchSmsOutZone)
                                    && !db.checkAlreadyWarningInDayQueue(bps.getIsdn(), branchSmsOutZone)) {
                                db.sendSms(bps.getIsdn(), branchSmsOutZone, "86904");
                            }
                            continue;
                        }
                        bps.setResultCode("PM20");
                        bps.setActionType("1");
                        bps.setDescription(description + " Already online on " + cellCodeCus.trim().toUpperCase());
                    }
                    bps.setActionType("1");
                }
            } else {
                logger.warn("Can not get config " + bps.getProductCode() + " from branch_promotion_config ");
                bps.setResultCode("PM21");
                bps.setDescription(description + "Can not get config " + bps.getProductCode() + " from branch_promotion_config ");
            }

        }
        listRecord.clear();
        Thread.sleep(5 * 60 * 1000);
        return listResult;
    }

    private String removePrice(BranchPromotionSub bps, BranchPromotionConfig bpc, String description) {
        String removePrice = "";
//        String pcrfInfo = "";
//        String rsRemoveSub = "";
        //B1: Huy chinh sach goi nhan tin mien phi noi mang               
        if (bpc.getCallSmsOnNetPlan() != null) {
            removePrice = pro.removePrice(bps.getIsdn(), bpc.getCallSmsOnNetPlan());
            if ("0".equals(removePrice) || "102010227".equals(removePrice)) {
                logger.info("Successfully remove Price " + bpc.getCallSmsOnNetPlan() + " isdn " + bps.getIsdn());
                description = description + " remove free call sms price: " + bpc.getCallSmsOnNetPlan() + " success|";
            } else {
                logger.error("fail to remove Price " + bpc.getCallSmsOnNetPlan() + " isdn " + bps.getIsdn());
                description = description + " remove free call sms price: " + bpc.getCallSmsOnNetPlan() + " fail|";
            }
        }
        //B2: Huy chinh sach goi quoc te gia re
//        if (bpc.getInterCallPlan() != null) {
//            removePrice = pro.removePrice(bps.getIsdn(), bpc.getInterCallPlan());
//            if ("0".equals(removePrice) || "102010227".equals(removePrice)) {
//                logger.info("Successfully remove Price " + bpc.getInterCallPlan() + " isdn " + bps.getIsdn());
//                description = description + " remove free call sms price: " + bpc.getInterCallPlan() + " success|";
//            } else {
//                logger.error("fail to remove Price " + bpc.getInterCallPlan() + " isdn " + bps.getIsdn());
//                description = description + " remove free call sms price: " + bpc.getInterCallPlan() + " fail|";
//            }
//        }
//        pcrfInfo = pro.querySubPCRF(bps.getIsdn());
//        if ("0".equals(pcrfInfo)) {
////      neu co thong tin thue bao tren PCRF roi           
//            rsRemoveSub = pro.removeSubServicePCRF(bps.getIsdn(), "Diamond_Unlimited");
//            if ("0".equals(rsRemoveSub)) {
//                logger.error("Remove sub on PCRF successful " + rsRemoveSub + " isdn " + bps.getIsdn());
//                description = description + " Remove sub on PCRF successful " + rsRemoveSub + " successful|";
//            } else {
//                logger.error("Remove sub on PCRF fail " + rsRemoveSub + " isdn " + bps.getIsdn());
//                description = description + " Remove sub on PCRF successful " + rsRemoveSub + " fail|";
//            }
//        }
        return description;
    }

    private String addPrice(BranchPromotionSub bps, BranchPromotionConfig bpc, String description) {
        String rsAddPrice = "";
        //Công chinh sach goi va nhan tin mien phi noi mang
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
        //Chinh sach goi quoc te gia re
        if (bpc.getInterCallPlan() != null) {
            rsAddPrice = pro.addPrice(bps.getIsdn(), bpc.getInterCallPlan(), "", "30");
            if ("0".equals(rsAddPrice) || "102010228".equals(rsAddPrice)) {
                logger.info("Successfully add Price " + bpc.getInterCallPlan() + " isdn " + bps.getIsdn());
                description = description + " add freeCallOnNet price: " + bpc.getInterCallPlan() + " success|";
                bps.setDescription(description);
            } else {
                logger.error("fail to add Price " + bpc.getInterCallPlan() + " isdn " + bps.getIsdn());
                description = description + " add freeCallOnNet price: " + bpc.getInterCallPlan() + " fail|";
                bps.setDescription(description);
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
