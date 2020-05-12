/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.database;

import com.viettel.smsfw.manager.AppManager;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.obj.DataResources;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import com.viettel.vas.wsfw.object.Subscriber;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.wsfw.common.Common;
import com.viettel.vas.wsfw.object.Config;
import com.viettel.vas.wsfw.object.ProductConnectKit;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

/**
 *
 * @author kdvt_tungtt8
 * @version x.x
 * @since Dec 18, 2012
 */
public class DbPre {

    private Logger logger;
    private String loggerLabel = DbPre.class.getSimpleName() + ": ";
    private PoolStore dataStore;
    public SimpleDateFormat formatDate = new SimpleDateFormat("yyyyMMddHHmmss");
    private static String countryCode;

    public DbPre(String sessionName, Logger logger) {
        this.logger = logger;
        try {
            dataStore = new PoolStore(sessionName, logger);
            if (AppManager.enableQueryDbTimeout && AppManager.queryDbTimeout > 0) {
                dataStore.setQueryTimeOut(AppManager.queryDbTimeout);
            }
            countryCode = ResourceBundle.getBundle("vas").getString("country_code"); // lay o thu vien AllVas.jar
        } catch (Exception ex) {
            logger.error(loggerLabel + "ERROR DbPre", ex);
        }
    }

    /**
     * Lấy toàn bộ danh sách REL_PRODUCT_CODE của thuê bao
     *
     * @param subId
     * @return
     */
    public ArrayList<String> getAllVasList(String subId) {
        /**
         * select rel_product_code as rel_product_code from
         * cm_pos2.sub_rel_product where sub_id = ?
         */
        ArrayList<String> resultObj = null;
        ParamList paramList = new ParamList();
        long timeStart = System.currentTimeMillis();;
        try {
            paramList.add(new Param("REL_PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("SUB_ID", subId, Param.DataType.STRING, Param.IN));
            DataResources data = this.dataStore.selectTable(paramList, "SUB_REL_PRODUCT");
            resultObj = new ArrayList<String>();
            while (data.next()) {
                resultObj.add(data.getString("REL_PRODUCT_CODE"));
            }
            logTimeDb("getAllVasList success ", timeStart);
        } catch (Exception ex) {
            logger.error("ERROR getAllVasList", ex);
            try {
                timeStart = System.currentTimeMillis();
                DataResources data = this.dataStore.selectTable(paramList, "SUB_REL_PRODUCT");
                resultObj = new ArrayList<String>();
                while (data.next()) {
                    resultObj.add(data.getString("REL_PRODUCT_CODE"));
                }
                logTimeDb("Time to getAllVasList again", timeStart);
            } catch (Exception ex1) {
                logger.error("ERROR retry getAllVasList", ex1);
                AppManager.logException(timeStart, ex);
            }
        }
        return resultObj;
    }

    /**
     * Lấy thông tin thuê bao: SUB_ID, ACT_STATUS, PRODUCT_CODE, STA_DATETIME
     * Tham số input: msisdn (số điện thoại gồm mã nước), getVas (có lấy
     * sub_rel_product không)
     *
     * @param msisdn
     * @param getVas
     * @return
     */
    public Subscriber getSubInfoMobile(String msisdn, boolean getVas) {
        Subscriber sub = null;
        List<String> listVas = null;
        ParamList paramList = new ParamList();
        long timeStart = System.currentTimeMillis();
        try {
            if (msisdn.startsWith(Common.config.countryCode)) {
                msisdn = msisdn.substring(Common.config.countryCode.length());
            }
            paramList.add(new Param("ISDN", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("SUB_ID", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("ACT_STATUS", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("REG_TYPE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("STA_DATETIME", null, Param.DataType.TIMESTAMP, Param.OUT));
            paramList.add(new Param("STATUS", 2, Param.DataType.INT, Param.IN));
            paramList.add(new Param("ISDN", msisdn, Param.DataType.STRING, Param.IN));
            DataResources rs = dataStore.selectTable(paramList, "SUB_MB");
            if (rs.next()) {
//                if (rs.getString("ACT_STATUS") == null || (rs.getString("ACT_STATUS") != null && "03".equals(rs.getString("ACT_STATUS").trim()))) {
//                    sub = new Subscriber("NO_INFO_SUB");
//                    logger.info("ActStatus = 03, time to get sub info for sub " + msisdn
//                            + " " + (System.currentTimeMillis() - timeStart));
//                    return sub;
//                }
                String isdn = rs.getString("ISDN");
                sub = new Subscriber(countryCode + isdn);
                long subId = rs.getLong("SUB_ID");
                sub.setSubId("" + subId);
                sub.setActStatus(rs.getString("ACT_STATUS"));
                sub.setProductCode(rs.getString("PRODUCT_CODE"));
                sub.setActiveTime((rs.getTimestamp("STA_DATETIME") != null) ? formatDate.format(rs.getTimestamp("STA_DATETIME")) : "");
                sub.setServiceType(0);
                sub.setStatus("2");
                sub.setIsdn(isdn);
                sub.setRegType(rs.getString("REG_TYPE"));

                if (getVas) {
                    listVas = getSubRelProductMobile(subId);
                    if (listVas != null && !listVas.isEmpty()) {
                        sub.setVasList((String[]) listVas.toArray(new String[listVas.size()]));
                    }
                }
            } else {
                sub = new Subscriber("NO_INFO_SUB");
            }
            logger.info("Time to get sub info for sub " + msisdn + " " + (System.currentTimeMillis() - timeStart));
            return sub;
        } catch (Exception ex) {
            logger.error("ERROR getSubInfoMobile", ex);
            try {
                DataResources rs = dataStore.selectTable(paramList, "SUB_MB");
                if (rs.next()) {
                    String isdn = rs.getString("ISDN");
                    sub = new Subscriber(countryCode + isdn);
                    long subId = rs.getLong("SUB_ID");
                    sub.setSubId("" + subId);
                    sub.setActStatus(rs.getString("ACT_STATUS"));
                    sub.setProductCode(rs.getString("PRODUCT_CODE"));
                    sub.setActiveTime((rs.getTimestamp("STA_DATETIME") != null) ? formatDate.format(rs.getTimestamp("STA_DATETIME")) : "");
                    sub.setServiceType(0);
                    sub.setStatus("2");
                    sub.setIsdn(isdn);
                    sub.setRegType(rs.getString("REG_TYPE"));
                    if (getVas) {
                        listVas = getSubRelProductMobile(subId);
                        if (listVas != null && !listVas.isEmpty()) {
                            sub.setVasList((String[]) listVas.toArray(new String[listVas.size()]));
                        }
                    }
                } else {
                    sub = new Subscriber("NO_INFO_SUB");
                }
                logTimeDb("Time to getSubInfoMobile again", timeStart);
                return sub;
            } catch (Exception ex1) {
                logger.error("ERROR retry getSubInfoMobile", ex1);
                AppManager.logException(timeStart, ex);
                return null;
            }
        }
    }

    /**
     * Lấy thông tin thuê bao: SUB_ID, ACT_STATUS, PRODUCT_CODE, STA_DATETIME
     * Tham số input: msisdn (số điện thoại gồm mã nước), getVas (có lấy
     * sub_rel_product không)
     *
     * @param msisdn
     * @param getVas
     * @return
     */
    public Subscriber getSubInfoHomephone(String msisdn, boolean getVas) {
        Subscriber sub = null;
        List<String> listVas = null;
        long timeStart = System.currentTimeMillis();
        ParamList paramList = new ParamList();
        try {
            if (msisdn.startsWith(Common.config.countryCode)) {
                msisdn = msisdn.substring(Common.config.countryCode.length());
            }
            paramList.add(new Param("ISDN", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("SUB_ID", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("ACT_STATUS", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("SUB_TYPE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("STA_DATETIME", null, Param.DataType.TIMESTAMP, Param.OUT));
            paramList.add(new Param("REG_TYPE", null, Param.DataType.STRING, Param.OUT));
            //
            paramList.add(new Param("STATUS", 2, Param.DataType.INT, Param.IN));
            paramList.add(new Param("ISDN", msisdn, Param.DataType.STRING, Param.IN));
            DataResources rs = dataStore.selectTable(paramList, "SUB_HP");
            if (rs.next()) {
                String isdn = rs.getString("ISDN");
                sub = new Subscriber(countryCode + isdn);
                long subId = rs.getLong("SUB_ID");
                sub.setSubId("" + subId);
                sub.setActStatus(rs.getString("ACT_STATUS"));
                sub.setProductCode(rs.getString("PRODUCT_CODE"));
                sub.setActiveTime((rs.getTimestamp("STA_DATETIME") != null) ? formatDate.format(rs.getTimestamp("STA_DATETIME")) : "");
                sub.setServiceType(0);
                sub.setStatus("2");
                sub.setIsdn(isdn);
//                sub.setSubType(rs.getString("SUB_TYPE"));
                sub.setRegType(rs.getString("REG_TYPE"));
                if (getVas) {
                    listVas = getSubRelProductHomephone(subId);
                    if (listVas != null && !listVas.isEmpty()) {
                        sub.setVasList((String[]) listVas.toArray(new String[listVas.size()]));
                    }
                }
            } else {
                sub = new Subscriber("NO_INFO_SUB");
            }
            logTimeDb("Time to getSubInfoHomephone", timeStart);
            return sub;
        } catch (Exception ex) {
            logger.error("ERROR getSubInfoHomephone", ex);
            try {
                timeStart = System.currentTimeMillis();
                DataResources rs = dataStore.selectTable(paramList, "SUB_HP");
                if (rs.next()) {
                    String isdn = rs.getString("ISDN");
                    sub = new Subscriber(countryCode + isdn);
                    long subId = rs.getLong("SUB_ID");
                    sub.setSubId("" + subId);
                    sub.setActStatus(rs.getString("ACT_STATUS"));
                    sub.setProductCode(rs.getString("PRODUCT_CODE"));
                    sub.setActiveTime((rs.getTimestamp("STA_DATETIME") != null) ? formatDate.format(rs.getTimestamp("STA_DATETIME")) : "");
                    sub.setServiceType(0);
                    sub.setStatus("2");
                    sub.setIsdn(isdn);
//                    sub.setSubType(rs.getString("SUB_TYPE"));
                    sub.setRegType(rs.getString("REG_TYPE"));
                } else {
                    sub = new Subscriber("NO_INFO_SUB");
                }
                logTimeDb("Time to getSubInfoHomephone again", timeStart);
                return sub;
            } catch (Exception ex1) {
                logger.error("ERROR retry getSubInfoHomephone", ex1);
                AppManager.logException(timeStart, ex);
                return null;
            }
        }
    }

    /**
     * Lấy thông tin 1 list các thuê bao: SUB_ID, ACT_STATUS, PRODUCT_CODE,
     * STA_DATETIME Tham số input: listMsisdn (các số điện thoại gồm mã nước,
     * ngăn cách bởi ','), getVas (có lấy sub_rel_product không)
     *
     * @param msisdn
     * @param getVas
     * @return
     */
    public HashMap<String, Subscriber> getListSubInfoMobile(String listMsisdn, boolean getVas) {
        HashMap<String, Subscriber> mSubs = null;
        List<Long> listSubId = null;
        long timeStart = System.currentTimeMillis();
        List<String> lstMsisdn = new ArrayList<String>();
        for (String msisdn : Arrays.asList(listMsisdn.split(","))) {
            if (msisdn.startsWith(Common.config.countryCode)) {
                msisdn = msisdn.substring(Common.config.countryCode.length());
            }
            lstMsisdn.add(msisdn);
        }
        ParamList paramList = new ParamList();
        try {
            paramList.add(new Param("ISDN", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("SUB_ID", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("ACT_STATUS", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("STA_DATETIME", null, Param.DataType.TIMESTAMP, Param.OUT));
            paramList.add(new Param("STATUS", 2, Param.DataType.INT, Param.IN));
            paramList.add(new Param("ISDN", lstMsisdn, Param.OperatorType.IN, Param.DataType.OBJ, Param.IN));
            DataResources rs = dataStore.selectTable(paramList, "SUB_MB");
            mSubs = new HashMap<String, Subscriber>();
            if (getVas) {
                listSubId = new ArrayList<Long>();
            }
            while (rs.next()) {
                String isdn = rs.getString("ISDN");
                Subscriber sub = new Subscriber(countryCode + isdn);
                long subId = rs.getLong("SUB_ID");
                sub.setSubId("" + subId);
                sub.setActStatus(rs.getString("ACT_STATUS"));
                sub.setProductCode(rs.getString("PRODUCT_CODE"));
                sub.setActiveTime((rs.getTimestamp("STA_DATETIME") != null) ? formatDate.format(rs.getTimestamp("STA_DATETIME")) : "");
                sub.setServiceType(0);
                mSubs.put(countryCode + isdn, sub);

                if (getVas) {
                    listSubId.add(subId);
                }
            }

            if (getVas) {
                // Lay thong tin VAS_CODE
                if (!listSubId.isEmpty()) {
                    HashMap<String, List<String>> listVasSub = getListSubRelProductMobile(listSubId);
                    if (listVasSub == null) {
                        return null;
                    }

                    for (Subscriber subscriber : mSubs.values()) {
                        List<String> listVas = listVasSub.get(subscriber.getSubId());
                        if (listVas != null && !listVas.isEmpty()) {
                            subscriber.setVasList((String[]) listVas.toArray(new String[listVas.size()]));
                        }
                    }
                }
            }
            logTimeDb("Time to getListSubInfoMobile", timeStart);
            return mSubs;
        } catch (Exception ex) {
            logger.error("ERROR getListSubInfoMobile", ex);
            mSubs = null;
            try {
                timeStart = System.currentTimeMillis();
                DataResources rs = dataStore.selectTable(paramList, "SUB_MB");
                mSubs = new HashMap<String, Subscriber>();
                if (getVas) {
                    listSubId = new ArrayList<Long>();
                }
                while (rs.next()) {
                    String isdn = rs.getString("ISDN");
                    Subscriber sub = new Subscriber(countryCode + isdn);
                    long subId = rs.getLong("SUB_ID");
                    sub.setSubId("" + subId);
                    sub.setActStatus(rs.getString("ACT_STATUS"));
                    sub.setProductCode(rs.getString("PRODUCT_CODE"));
                    sub.setActiveTime((rs.getTimestamp("STA_DATETIME") != null) ? formatDate.format(rs.getTimestamp("STA_DATETIME")) : "");
                    sub.setServiceType(0);
                    mSubs.put(countryCode + isdn, sub);
                    if (getVas) {
                        listSubId.add(subId);
                    }
                }
                if (getVas) {
                    // Lay thong tin VAS_CODE
                    if (!listSubId.isEmpty()) {
                        HashMap<String, List<String>> listVasSub = getListSubRelProductMobile(listSubId);
                        if (listVasSub == null) {
                            return null;
                        }
                        for (Subscriber subscriber : mSubs.values()) {
                            List<String> listVas = listVasSub.get(subscriber.getSubId());
                            if (listVas != null && !listVas.isEmpty()) {
                                subscriber.setVasList((String[]) listVas.toArray(new String[listVas.size()]));
                            }
                        }
                    }
                }
                logTimeDb("Time to getListSubInfoMobile again", timeStart);
                return mSubs;
            } catch (Exception ex1) {
                logger.error("ERROR retry getListSubInfoMobile", ex1);
                AppManager.logException(timeStart, ex);
                return null;
            }
        }
    }

    /**
     * Lấy danh sách các REL_PRODUCT_CODE đang hoạt động
     *
     * @param subId
     * @return
     */
    public List<String> getSubRelProductMobile(long subId) {
        List<String> listRel = null;
        ParamList paramList = new ParamList();
        long timeStart = System.currentTimeMillis();
        try {
            paramList.add(new Param("REL_PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("SUB_ID", subId, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("STATUS", 1, Param.DataType.INT, Param.IN));
            DataResources rs = dataStore.selectTable(paramList, "SUB_REL_PRODUCT");
            listRel = new ArrayList<String>();
            while (rs.next()) {
                listRel.add(rs.getString("REL_PRODUCT_CODE"));
            }
            logTimeDb("Time to getSubRelProductMobile", timeStart);
        } catch (Exception ex) {
            logger.error("ERROR getSubRelProductMobile", ex);
            try {
                timeStart = System.currentTimeMillis();
                DataResources rs = dataStore.selectTable(paramList, "SUB_REL_PRODUCT");
                while (rs.next()) {
                    listRel.add(rs.getString("REL_PRODUCT_CODE"));
                }
                logTimeDb("Time to getSubRelProductMobile again ", timeStart);
            } catch (Exception ex1) {
                logger.error("ERROR retry getSubRelProductMobile", ex1);
                AppManager.logException(timeStart, ex);
            }
        }
        return listRel;
    }

    /**
     * Lấy danh sách các REL_PRODUCT_CODE đang hoạt động
     *
     * @param subId
     * @return
     */
    public List<String> getSubRelProductHomephone(long subId) {
        List<String> listRel = null;
        ParamList paramList = new ParamList();
        long timeStart = System.currentTimeMillis();
        try {
            paramList.add(new Param("REL_PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("SUB_ID", subId, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("STATUS", 1, Param.DataType.INT, Param.IN));
            DataResources rs = dataStore.selectTable(paramList, "SUB_REL_PRODUCT_HP");
            listRel = new ArrayList<String>();
            while (rs.next()) {
                listRel.add(rs.getString("REL_PRODUCT_CODE"));
            }
            logTimeDb("Time to getSubRelProductHomephone", timeStart);
        } catch (Exception ex) {
            logger.error("ERROR getSubRelProductHomephone", ex);
            try {
                timeStart = System.currentTimeMillis();
                DataResources rs = dataStore.selectTable(paramList, "SUB_REL_PRODUCT_HP");
                logTimeDb("Time to getSubRelProductHomephone", timeStart);
                while (rs.next()) {
                    listRel.add(rs.getString("REL_PRODUCT_CODE"));
                }
            } catch (Exception ex1) {
                logger.error("ERROR retry getSubRelProductHomephone", ex1);
                AppManager.logException(timeStart, ex);
            }
        }
        return listRel;
    }

    /**
     * Lấy danh sách các REL_PRODUCT_CODE đang hoạt động của 1 danh sách các
     * SUB_ID
     *
     * @param subId
     * @return
     */
    public HashMap<String, List<String>> getListSubRelProductMobile(List<Long> listSubId) {
        HashMap<String, List<String>> listSubRel = new HashMap<String, List<String>>();
        ParamList paramList = new ParamList();
        long timeStart = System.currentTimeMillis();
        try {
            paramList.add(new Param("SUB_ID", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("REL_PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("SUB_ID", listSubId, Param.OperatorType.IN, Param.DataType.OBJ, Param.IN));
            paramList.add(new Param("STATUS", 1, Param.DataType.INT, Param.IN));
            paramList.add(new Param("SUB_ID", null, Param.DataType.LONG, Param.ORDER_ASC));
            DataResources rs = dataStore.selectTable(paramList, "SUB_REL_PRODUCT");
            long subId = -1;
            long tmp = -1;
            List<String> listRel = null;
            while (rs.next()) {
                tmp = rs.getLong("SUB_ID");
                if (subId != tmp) {
                    if (subId > 0) {
                        listSubRel.put("" + subId, listRel);
                        listRel = new ArrayList<String>();
                        subId = tmp;
                    } else {
                        subId = tmp;
                        listRel = new ArrayList<String>();
                    }
                }

                listRel.add(rs.getString("REL_PRODUCT_CODE"));
            }
            if (listRel != null && !listRel.isEmpty()) {
                listSubRel.put("" + subId, listRel);
            }
            logTimeDb("Time to getListSubRelProductMobile", timeStart);
        } catch (Exception ex) {
            logger.error("ERROR getListSubRelProductMobile", ex);
            try {
                timeStart = System.currentTimeMillis();
                DataResources rs = dataStore.selectTable(paramList, "SUB_REL_PRODUCT");
                logTimeDb("Time to getListSubRelProductMobile", timeStart);
                long subId = -1;
                long tmp = -1;
                List<String> listRel = null;
                while (rs.next()) {
                    tmp = rs.getLong("SUB_ID");
                    if (subId != tmp) {
                        if (subId > 0) {
                            listSubRel.put("" + subId, listRel);
                            listRel = new ArrayList<String>();
                            subId = tmp;
                        } else {
                            subId = tmp;
                            listRel = new ArrayList<String>();
                        }
                    }
                    listRel.add(rs.getString("REL_PRODUCT_CODE"));
                }
                if (listRel != null && !listRel.isEmpty()) {
                    listSubRel.put("" + subId, listRel);
                }
            } catch (Exception ex1) {
                logger.error("ERROR retry getListSubRelProductMobile", ex1);
                AppManager.logException(timeStart, ex);
            }
        }
        return listSubRel;
    }

    public void logTimeDb(String strLog, long timeSt) {
        long timeEx = System.currentTimeMillis() - timeSt;
        StringBuilder br = new StringBuilder();
        if (timeEx >= AppManager.minTimeDb && AppManager.loggerDbMap != null) {
            br.setLength(0);
            br.append(loggerLabel).
                    append(AppManager.getTimeLevelDb(timeEx)).append(": ").
                    append(strLog).
                    append(": ").
                    append(timeEx).
                    append(" ms");

            logger.warn(br);
        } else {
            br.setLength(0);
            br.append(loggerLabel).
                    append(strLog).
                    append(": ").
                    append(timeEx).
                    append(" ms");

            logger.info(br);
        }
    }

    public int insertSubRelProduct(String msisdn, String subId, String productCode, String vasCode) {

        ParamList paramList = new ParamList();
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("SUB_REL_PRODUCT_ID", "SEQ_SUB_REL_PRODUCT.nextval", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("SUB_ID", Long.valueOf(subId), Param.DataType.LONG, Param.IN));
            paramList.add(new Param("STA_DATETIME", "sysdate", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("REG_DATE", "sysdate", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("MAIN_PRODUCT_CODE", productCode, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("REL_PRODUCT_CODE", vasCode, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("IS_CONNECTED", "1", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("STATUS", "1", Param.DataType.CONST, Param.IN));
            PoolStore.PoolResult prs = dataStore.insertTable(paramList, "sub_rel_product");
            logTimeDb("Time to insertSubRelProduct isdn " + msisdn + " subId " + subId
                    + " productCode " + productCode
                    + " vasCode " + vasCode, timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertSubRelProduct default return -1: isdn " + msisdn + " subId " + subId
                    + " productCode " + productCode
                    + " vasCode " + vasCode);
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public int insertIsdnAddMoneyDataForKeeto(String msisdn, String dataMBMonthlyValue, String moneyMonthlyValue, String voiceOnnetId, String dataMonthlyId) {

        ParamList paramList = new ParamList();
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("sub_rel_keeto_id", "seq_sub_rel_keeto.nextval", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("msisdn", Long.valueOf(msisdn), Param.DataType.LONG, Param.IN));
            paramList.add(new Param("REG_DATE", "sysdate", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("dataMB_Monthly_Value", dataMBMonthlyValue, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("money_Monthly_Value", moneyMonthlyValue, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("voice_Onnet_Id", voiceOnnetId, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("data_Monthly_Id", dataMonthlyId, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("STATUS", "1", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("description", "addMoneyData success", Param.DataType.STRING, Param.IN));
            PoolStore.PoolResult prs = dataStore.insertTable(paramList, "sub_rel_keeto");
            logTimeDb("Time to insertIsdnAddMoneyDataForKeeto isdn " + msisdn + " dataMBMonthlyValue " + dataMBMonthlyValue
                    + " moneyMonthlyValue " + moneyMonthlyValue
                    + " voiceOnnetId " + voiceOnnetId, timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logTimeDb("ERROR insertSubRelProduct default return -1: isdn " + msisdn + " dataMBMonthlyValue " + dataMBMonthlyValue
                    + " moneyMonthlyValue " + moneyMonthlyValue
                    + " voiceOnnetId " + voiceOnnetId, timeSt);
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public boolean checkAlreadyBonues(String isdn) {
        /**
         * SELECT product_code FROM ba_product_config WHERE product_code =
         * p_product_code AND status = 0;
         */
        ParamList paramList = new ParamList();
        boolean result = false;
        long timeSt = System.currentTimeMillis();
        try {
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            paramList.add(new Param("msisdn", Integer.parseInt(isdn), Param.DataType.INT, Param.IN));
            paramList.add(new Param("msisdn", null, Param.DataType.STRING, Param.OUT));
            DataResources rs = this.dataStore.selectTable(paramList, "sub_rel_keeto");
            while (rs.next()) {
                String msisdn = rs.getString("msisdn");
                if (msisdn != null && msisdn.trim().length() > 0) {
                    result = true;
                    break;
                }
            }
            logTimeDb("Time to checkAlreadyBonues msisdn " + isdn + " result: " + result, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR checkAlreadyBonues default return true " + " isdn " + isdn);
            logger.error(AppManager.logException(timeSt, ex));
            result = true;
        }
        return result;
    }

    public int removeSubFromGroup(String isdn, int kitBatchId, String processUser) {
        ParamList paramList = new ParamList();
        long timeSt = System.currentTimeMillis();
        try {
            if (isdn.startsWith("258")) {
                isdn = isdn.substring(3);
            }
            paramList.add(new Param("isdn", isdn, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("kit_batch_id", kitBatchId, Param.DataType.INT, Param.IN));
            paramList.add(new Param("result_code", "0", Param.DataType.STRING, Param.IN));
            PoolStore.PoolResult prs = dataStore.deleteTable(paramList, "kit_batch_detail");
            logTimeDb("Time to removeSubFromGroup in kit_batch_detail isdn " + isdn + " kitBatchId " + kitBatchId + " processUser " + processUser, timeSt);
            
            paramList = new ParamList();
            paramList.add(new Param("isdn", isdn, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("kit_batch_id", kitBatchId, Param.DataType.INT, Param.IN));
            paramList.add(new Param("result_code", "0", Param.DataType.STRING, Param.IN));
            prs = dataStore.deleteTable(paramList, "kit_batch_extend");
            logTimeDb("Time to removeSubFromGroup in kit_batch_extend isdn " + isdn + " kitBatchId " + kitBatchId + " processUser " + processUser, timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR removeSubFromGroup default return -1: isdn " + isdn + " kitBatchId " + kitBatchId + " processUser " + processUser);
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public int getActionAuditSeq() {
        String sql = "select SEQ_ACTION_AUDIT.NEXTVAL as ACTION_AUDIT_ID from dual";
        ParamList paramList = new ParamList();
        long timeSt = System.currentTimeMillis();
        int actionAuditId = -1;
        try {
            paramList.add(new Param("ACTION_AUDIT_ID", null, Param.DataType.INT, Param.OUT));

            DataResources rs = dataStore.select(paramList, sql);
            while (rs.next()) {
                actionAuditId = rs.getInt("ACTION_AUDIT_ID");
                break;
            }
            logTimeDb("Time to getActionAuditSeq", timeSt);
            return actionAuditId;
        } catch (Exception ex) {
            logger.error("ERROR getActionAuditSeq default return -1");
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public int insertActionAudit(long actionAuditId, String isdn, String description, String processUser, String ip) {
        ParamList paramList = new ParamList();
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("ACTION_AUDIT_ID", actionAuditId, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("ISSUE_DATETIME", "sysdate", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("ACTION_CODE", "00", Param.DataType.STRING, Param.IN));
            paramList.add(new Param("REASON_ID", null, Param.DataType.INT, Param.IN));
            paramList.add(new Param("SHOP_CODE", "", Param.DataType.STRING, Param.IN));
            paramList.add(new Param("USER_NAME", processUser, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("PK_TYPE", "3", Param.DataType.STRING, Param.IN));
            paramList.add(new Param("PK_ID", 0, Param.DataType.INT, Param.IN));
            paramList.add(new Param("IP", ip, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("DESCRIPTION", description, Param.DataType.STRING, Param.IN));

            PoolStore.PoolResult prs = dataStore.insertTable(paramList, "action_audit");
            logTimeDb("Time to insertActionAudit isdn " + isdn + " description " + description + " processUser " + processUser + " ip " + ip, timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertActionAudit default return -1: isdn " + isdn + " description " + description + " processUser " + processUser + " ip " + ip);
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public int insertKitBatchRebuildHis(String isdn, int kitBatchId, int actionAuditId, String processUser) {
        ParamList paramList = new ParamList();
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("kit_batch_id", kitBatchId, Param.DataType.INT, Param.IN));
            paramList.add(new Param("isdn", isdn, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("result_code", "0", Param.DataType.STRING, Param.IN));
            paramList.add(new Param("description", "Remove member from group", Param.DataType.STRING, Param.IN));
            paramList.add(new Param("process_time", "sysdate", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("action_audit_id", actionAuditId, Param.DataType.INT, Param.IN));
            PoolStore.PoolResult prs = dataStore.insertTable(paramList, "kit_batch_rebuild_his");
            logTimeDb("Time to insertKitBatchRebuildLog isdn " + isdn + " kitBatchId " + kitBatchId, timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertKitBatchRebuildLog default return -1: isdn " + isdn + " kitBatchId " + kitBatchId);
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }

    public HashMap<String, Config> getConfigs() {
        ParamList paramList = new ParamList();
        HashMap<String, Config> map = new HashMap<String, Config>();
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("PARAM_TYPE", "KIT_BATCH", Param.DataType.STRING, Param.IN));
            paramList.add(new Param("STATUS", 1, Param.DataType.INT, Param.IN));
            paramList.add(new Param("PARAM_ID", null, Param.DataType.INT, Param.OUT));
            paramList.add(new Param("PARAM_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("PARAM_NAME", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("PARAM_VALUE", null, Param.DataType.STRING, Param.OUT));
            DataResources rs = dataStore.selectTable(paramList, "AP_PARAM");
            logTimeDb("Time to getConfigs", timeSt);
            while (rs.next()) {
                Config conf = new Config();
                conf.setConfigId(rs.getInt("PARAM_ID"));
                conf.setValue(rs.getString("PARAM_VALUE"));
                conf.setCode(rs.getString("PARAM_CODE"));
                map.put(rs.getString("PARAM_CODE").toLowerCase(), conf);
                continue;
            }
        } catch (Exception ex) {
            logger.error("ERROR getConfigs default return -1");
            logger.error(AppManager.logException(timeSt, ex));
        }
        return map;
    }

    public ProductConnectKit getProductInfo(String isdn, String productCode) {
        ParamList paramList = new ParamList();
        String sql = "select * from product.product_connect_kit where upper(product_code) = upper(?) and status = 1 and vip_product = '1'";
        long timeSt = System.currentTimeMillis();
        ProductConnectKit result = null;
        try {
            paramList.add(new Param("product_code", productCode, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("product_code", productCode, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("money_fee", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("price_plan_id", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("pp_buy_more", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("offer_id", null, Param.DataType.LONG, Param.OUT));

            DataResources rs = dataStore.select(paramList, sql);
            
            if (rs.next()) {
                result = new ProductConnectKit();
                result.setProductCode(rs.getString("product_code"));
                result.setMoneyFee(rs.getString("money_fee"));
                result.setPricePlan(rs.getString("price_plan_id"));
                result.setOfferId(rs.getLong("offer_id"));
                result.setPpBuyMore(rs.getString("pp_buy_more"));
            }
            logTimeDb("Time to getProductInfo isdn " + isdn + " productCode " + productCode, timeSt);
        } catch (Exception ex) {
            logger.error("ERROR getPrepaidMonth default return -1 isdn " + isdn + " productCode " + productCode);
            logger.error(AppManager.logException(timeSt, ex));
        } finally {
            return result;
        }
    }
    
    public float calculatePercentBonus(String[] arrChangeEliteBonusPaidMonth, int paidMonth, String isdn) {
        String strPercentBonus = "";
        for (String tmp : arrChangeEliteBonusPaidMonth) {
            String[] arrTmp = tmp.split("\\:");
            int configMonth;
            try {
                configMonth = Integer.parseInt(arrTmp[0]);
            } catch (NumberFormatException ex) {
                ex.printStackTrace();
                configMonth = 0;
            }
            if (paidMonth >= configMonth) {
                strPercentBonus = arrTmp[1];
                logger.info("Paid value: " + paidMonth + ", percentBonus: "
                        + strPercentBonus + ", isdn:  " + isdn);
                break;
            }
        }
        float percentBonus;
        try {
            percentBonus = Float.valueOf(strPercentBonus);
        } catch (NumberFormatException ex) {
            ex.printStackTrace();
            logger.info("Have exeption when parse percentBonus so auto set default percentBonus is zero, ex:"
                    + ex.getMessage() + ", isdn:  " + isdn);
            percentBonus = 0.0f;
        }
        logger.info("Paid value: " + paidMonth + ", percentBonus after calculate: "
                + strPercentBonus + ", isdn:  " + isdn);
        return percentBonus;
    }
    
    
    
    public int insertKitBatchExtend (int kitBatchId, String isdn, int cugId, String cugName, String resultCode, String description, String staffCode) {
        ParamList paramList = new ParamList();
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("kit_batch_id", kitBatchId, Param.DataType.INT, Param.IN));
            paramList.add(new Param("isdn", isdn, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("cug_id", cugId, Param.DataType.INT, Param.IN));
            paramList.add(new Param("cug_name", cugName, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("result_code", resultCode, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("description", description, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("staff_code", staffCode, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("extend_time", "sysdate", Param.DataType.CONST, Param.IN));
            
            PoolStore.PoolResult prs = dataStore.insertTable(paramList, "kit_batch_extend");
            logTimeDb("End insertKitBatchExtend isdn " + isdn + " kitBatchId " + kitBatchId
                    + " cugId " + cugId + " cugName " + cugName + " staffCode " + staffCode + " description " + description + " result " + prs, timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertGroupManagementLog default return -1: isdn " + isdn + " kitBatchId " + kitBatchId
                    + " cugId " + cugId + " cugName " + cugName + " staffCode " + staffCode + " description " + description);
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }
    
    public int insertGroupManagementLog (Long moId, Integer kitBatchId, String isdn, String productCode, String enterpriseWallet, Integer status, 
            Integer actionType, String createUser, Integer prepaidMonth, String transId, Double price, Double discount, String description, String resultCode, String requestId) {
        ParamList paramList = new ParamList();
        long timeSt = System.currentTimeMillis();
        try {
            paramList.add(new Param("id", "group_management_log_seq.nextval", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("kit_batch_id", kitBatchId, Param.DataType.INT, Param.IN));
            paramList.add(new Param("mo_id", moId, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("isdn", isdn, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("product_code", productCode, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("enterprise_wallet", enterpriseWallet, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("status", status, Param.DataType.INT, Param.IN));
            paramList.add(new Param("action_type", actionType, Param.DataType.INT, Param.IN));
            paramList.add(new Param("create_user", createUser, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("prepaid_month", prepaidMonth, Param.DataType.INT, Param.IN));
            paramList.add(new Param("trans_Id", transId, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("price", price, Param.DataType.DOUBLE, Param.IN));
            paramList.add(new Param("discount", discount, Param.DataType.DOUBLE, Param.IN));
            paramList.add(new Param("description", description, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("result_code", resultCode, Param.DataType.STRING, Param.IN));
            paramList.add(new Param("create_time", "sysdate", Param.DataType.CONST, Param.IN));
            paramList.add(new Param("request_id", requestId, Param.DataType.STRING, Param.IN));

            PoolStore.PoolResult prs = dataStore.insertTable(paramList, "group_management_log");
            logTimeDb("End insertGroupManagementLog isdn " + isdn + " transId " + transId
                    + " kitBatchId " + kitBatchId + " result " + prs, timeSt);
            return prs == PoolStore.PoolResult.SUCCESS ? 0 : -1;
        } catch (Exception ex) {
            logger.error("ERROR insertGroupManagementLog default return -1: isdn " + isdn + " transId " + transId
                    + " kitBatchId " + kitBatchId);
            logger.error(AppManager.logException(timeSt, ex));
            return -1;
        }
    }
}
