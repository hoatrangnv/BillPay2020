/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.database;

import com.viettel.smsfw.manager.AppManager;
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

/**
 *
 * @author kdvt_tungtt8
 * @version x.x
 * @since Dec 17, 2012
 */
public class DbPost {

    private Logger logger;
    private String loggerLabel = DbPost.class.getSimpleName() + ": ";
    private PoolStore dataStore;
    private SimpleDateFormat formatDate = new SimpleDateFormat("yyyyMMddHHmmss");
    //
    private static String countryCode;

    public DbPost(String sessionName, Logger logger) {
        this.logger = logger;
        try {
            dataStore = new PoolStore(sessionName, logger);
            if (AppManager.enableQueryDbTimeout && AppManager.queryDbTimeout > 0) {
                dataStore.setQueryTimeOut(AppManager.queryDbTimeout);
            }
            countryCode = ResourceBundle.getBundle("vas").getString("country_code"); // lay o thu vien AllVas.jar
        } catch (Exception ex) {
            logger.error(loggerLabel + "ERROR DbPos", ex);
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
        long timeStart = System.currentTimeMillis();
        try {
            paramList.add(new Param("REL_PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("SUB_ID", subId, Param.DataType.STRING, Param.IN));
            DataResources data = this.dataStore.selectTable(paramList, "SUB_REL_PRODUCT");
            resultObj = new ArrayList<String>();
            while (data.next()) {
                resultObj.add(data.getString("REL_PRODUCT_CODE"));
            }
            logTimeDb("Time getAllVasList", timeStart);
        } catch (Exception ex) {
            logger.error("ERROR getAllVasList", ex);
            try {
                timeStart = System.currentTimeMillis();
                DataResources data = this.dataStore.selectTable(paramList, "SUB_REL_PRODUCT");
                resultObj = new ArrayList<String>();
                while (data.next()) {
                    resultObj.add(data.getString("REL_PRODUCT_CODE"));
                }
                logTimeDb("Time getAllVasList", timeStart);
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
            paramList.add(new Param("REG_TYPE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("STA_DATETIME", null, Param.DataType.TIMESTAMP, Param.OUT));
            paramList.add(new Param("STATUS", 2, Param.DataType.INT, Param.IN));
            paramList.add(new Param("ISDN", msisdn, Param.DataType.STRING, Param.IN));
            DataResources rs = dataStore.selectTable(paramList, "SUB_MB");
            if (rs.next()) {
                String isdn = rs.getString("ISDN");
                sub = new Subscriber(countryCode + isdn);
                long subId = rs.getLong("SUB_ID");
                sub.setSubId("" + subId);
                sub.setActStatus(rs.getString("ACT_STATUS"));
                sub.setProductCode(rs.getString("PRODUCT_CODE"));
                sub.setActiveTime((rs.getTimestamp("STA_DATETIME") != null) ? formatDate.format(rs.getTimestamp("STA_DATETIME")) : "");
                sub.setServiceType(1);
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
            logTimeDb("Time to getSubInfoMobile", timeStart);
            return sub;
        } catch (Exception ex) {
            logger.error("ERROR getSubInfoMobile", ex);
            try {
                timeStart = System.currentTimeMillis();
                DataResources rs = dataStore.selectTable(paramList, "SUB_MB");
                if (rs.next()) {
                    String isdn = rs.getString("ISDN");
                    sub = new Subscriber(countryCode + isdn);
                    long subId = rs.getLong("SUB_ID");
                    sub.setSubId("" + subId);
                    sub.setActStatus(rs.getString("ACT_STATUS"));
                    sub.setProductCode(rs.getString("PRODUCT_CODE"));
                    sub.setActiveTime((rs.getTimestamp("STA_DATETIME") != null) ? formatDate.format(rs.getTimestamp("STA_DATETIME")) : "");
                    sub.setServiceType(1);
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
            paramList.add(new Param("SUB_TYPE", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("CUST_REQ_ID", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("STA_DATETIME", null, Param.DataType.TIMESTAMP, Param.OUT));
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
                sub.setServiceType(1);
                sub.setStatus("2");
                sub.setIsdn(isdn);
                sub.setSubType(rs.getString("SUB_TYPE"));
//                sub.setCustReqId(rs.getLong("CUST_REQ_ID"));
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
                logTimeDb("Time to getSubInfoHomephone", timeStart);

                if (rs.next()) {
                    String isdn = rs.getString("ISDN");
                    sub = new Subscriber(countryCode + isdn);
                    long subId = rs.getLong("SUB_ID");
                    sub.setSubId("" + subId);
                    sub.setActStatus(rs.getString("ACT_STATUS"));
                    sub.setProductCode(rs.getString("PRODUCT_CODE"));
                    sub.setActiveTime((rs.getTimestamp("STA_DATETIME") != null) ? formatDate.format(rs.getTimestamp("STA_DATETIME")) : "");
                    sub.setServiceType(1);
                    sub.setStatus("2");
                    sub.setIsdn(isdn);
                    sub.setSubType(rs.getString("SUB_TYPE"));
//                    sub.setCustReqId(rs.getLong("CUST_REQ_ID"));
                } else {
                    sub = new Subscriber("NO_INFO_SUB");
                }

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
                sub.setServiceType(1);
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
                logTimeDb("Time to getSubInfoMobile", timeStart);
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
                    sub.setServiceType(1);
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
                logTimeDb("Time to getSubRelProductMobile", timeStart);

                while (rs.next()) {
                    listRel.add(rs.getString("REL_PRODUCT_CODE"));
                }
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

    /**
     * Lấy OFFER_ID của 1 thuê bao
     *
     * @param isdn
     * @return
     */
    public long getOfferId(String isdn) {
        String sql = "SELECT CO.OFFER_ID AS OFFER_ID , OS.STA_DATETIME AS STA_DATETIME "
                + "FROM " + dataStore.getSchema() + ".CONTRACT_OFFER CO, "
                + "" + dataStore.getSchema() + ".OFFER_SUB OS, "
                + "" + dataStore.getSchema() + ".SUB_MB MB "
                + "WHERE CO.CONTRACT_OFFER_ID = OS.CONTRACT_OFFER_ID AND OS.SUB_ID = MB.SUB_ID "
                + "AND CO.STATUS = 1 AND OS.STATUS = 1 AND OS.STA_DATETIME IS NOT NULL "
                + "AND MB.STATUS = 2 AND MB.ISDN =?";
        long offerId = -1;
        ParamList paramList = new ParamList();
        long timeStart = System.currentTimeMillis();
        try {
            if (isdn.startsWith(Common.config.countryCode)) {
                isdn = isdn.substring(Common.config.countryCode.length());
            }
            paramList.add(new Param("OFFER_ID", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("STA_DATETIME", null, Param.DataType.DATE, Param.OUT));
            paramList.add(new Param("ISDN", isdn, Param.DataType.STRING, Param.IN));
            DataResources rs = dataStore.select(paramList, sql);
            if (rs.next()) {
                offerId = rs.getLong("OFFER_ID");
            }
            logTimeDb("Time to getOfferId", timeStart);
        } catch (Exception ex) {
            logger.error("ERROR getOfferId", ex);
            try {
                timeStart = System.currentTimeMillis();
                DataResources rs = dataStore.select(paramList, sql);
                logTimeDb("Time to getOfferId", timeStart);
                if (rs.next()) {
                    offerId = rs.getLong("OFFER_ID");
                }
            } catch (Exception ex1) {
                logger.error("ERROR retry getOfferId", ex1);
                AppManager.logException(timeStart, ex);
            }
        }
        return offerId;
    }

    /**
     * Lấy tổng hạn mức sử dụng của thuê bao
     *
     * @param msisdn
     * @param subId
     * @return
     */
    public float getHmsd(String msisdn, long subId) {
        long timeSt = System.currentTimeMillis();
        Float hmsd = -1F;
        Float deposit = -1F;
        try {
            // Lay hmsd
            /**
             * select AMOUNT from cm_pos.sub_limit_usage_mb where status=1 and
             * sub_id=? order by create_date desc
             */
            ParamList paramList = new ParamList();
            paramList.add(new Param("AMOUNT", null, Param.DataType.FLOAT, Param.OUT));
            paramList.add(new Param("STATUS", 1, Param.DataType.INT, Param.IN));
            paramList.add(new Param("SUB_ID", subId, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("CREATE_DATE", null, Param.DataType.DATE, Param.ORDER_DESC));
            DataResources rs = dataStore.selectTable(paramList, "SUB_LIMIT_USAGE_MB");
            hmsd = 0F;
            if (rs.next()) {
                hmsd = rs.getFloat("AMOUNT");
            }
            if (hmsd == null) {
                hmsd = 0F;
            }
            logger.info(loggerLabel
                    + "UseageLimit: MSISDN=" + msisdn + " - UseageLimit=" + hmsd);

            // Lay tien dat coc
            /**
             * select sum(DEPOSIT) num from cm_pos.sub_deposit where status=1
             * and sub_id=?
             */
            String sqlGetDeposit = "select sum(DEPOSIT) NUM from " + dataStore.getSchema()
                    + ".SUB_DEPOSIT where status=1  and sub_id=?";
            paramList = new ParamList();
            paramList.add(new Param("NUM", null, Param.DataType.FLOAT, Param.OUT));
            paramList.add(new Param("SUB_ID", subId, Param.DataType.LONG, Param.IN));
            rs = dataStore.select(paramList, sqlGetDeposit);
            deposit = 0F;
            if (rs.next()) {
                deposit = rs.getFloat("NUM");
            }
            if (deposit == null) {
                deposit = 0F;
            }
            logger.info(loggerLabel
                    + "Deposit of subs: MSISDN=" + msisdn + " - DEPOSIT=" + hmsd);

            hmsd += deposit;
        } catch (Exception ex) {
            logger.error(loggerLabel
                    + "ERROR getHmsd: MSISDN=" + msisdn, ex);
            hmsd = -1F;
        } finally {
            logTimeDb("Time to getHmsd: MSISDN=" + msisdn, timeSt);
        }
        return hmsd;
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
    public Subscriber getSubInfoADSL(String msisdn) {
        Subscriber sub = null;
        String sql = "SELECT ISDN , SUB_ID , ACT_STATUS, PRODUCT_CODE, SUB_TYPE, STA_DATETIME "
                + "FROM sub_adsl_ll "
                + "WHERE STATUS = ? AND  (account = ? OR sub_id = ?)"; //Huynq 20180810 add to support pay by subId
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
            paramList.add(new Param("SUB_TYPE", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("CUST_REQ_ID", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("STA_DATETIME", null, Param.DataType.TIMESTAMP, Param.OUT));
            //
            paramList.add(new Param("STATUS", 2, Param.DataType.INT, Param.IN));
            paramList.add(new Param("account", msisdn, Param.DataType.STRING, Param.IN));
            boolean isNumber = true;
            ArrayList<String> listNumber = new ArrayList<String>();
            listNumber.add("0");
            listNumber.add("1");
            listNumber.add("2");
            listNumber.add("3");
            listNumber.add("4");
            listNumber.add("5");
            listNumber.add("6");
            listNumber.add("7");
            listNumber.add("8");
            listNumber.add("9");
            for (int i = 0; i < msisdn.length(); i++) {
                if (!listNumber.contains(String.valueOf(msisdn.charAt(i)))) {
                    isNumber = false;
                    break;
                }
            }
            if (isNumber) {
                paramList.add(new Param("sub_id", msisdn, Param.DataType.STRING, Param.IN)); //Huynq 20180810 add to support pay by subId
            } else {
                paramList.add(new Param("sub_id", "", Param.DataType.STRING, Param.IN)); //Huynq 20180810 add to support pay by subId
            }
//            DataResources rs = dataStore.selectTable(paramList, "sub_adsl_ll");
            DataResources rs = dataStore.select(paramList, sql);
            if (rs.next()) {
                String isdn = rs.getString("ISDN");
                sub = new Subscriber(countryCode + isdn);
                long subId = rs.getLong("SUB_ID");
                sub.setSubId("" + subId);
                sub.setActStatus(rs.getString("ACT_STATUS"));
                sub.setProductCode(rs.getString("PRODUCT_CODE"));
                sub.setActiveTime((rs.getTimestamp("STA_DATETIME") != null) ? formatDate.format(rs.getTimestamp("STA_DATETIME")) : "");
                sub.setServiceType(1);
                sub.setStatus("2");
                sub.setIsdn(isdn);
                sub.setSubType(rs.getString("SUB_TYPE"));

            } else {
                sub = new Subscriber("NO_INFO_SUB");
            }
            logTimeDb("Time to getSubInfoADSL", timeStart);
            return sub;
        } catch (Exception ex) {
            logger.error("ERROR retry getSubInfoADSL", ex);
            AppManager.logException(timeStart, ex);
            return null;
        }
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

    public ArrayList<String> getAllContract() {
        ArrayList<String> listContract = new ArrayList<String>();
        String sql = "SELECT contract_id "
                + "FROM contract ";
        long timeStart = System.currentTimeMillis();
        ParamList paramList = new ParamList();
        try {
            paramList.add(new Param("contract_id", null, Param.DataType.LONG, Param.OUT));
            DataResources rs = dataStore.select(paramList, sql);
            while (rs.next()) {
                long contractId = rs.getLong("contract_id");
                listContract.add(contractId + "");
            }
            logTimeDb("Time to getAllContract", timeStart);
            return listContract;
        } catch (Exception ex) {
            logger.error("ERROR getAllContract ", ex);
            AppManager.logException(timeStart, ex);
            return null;
        }
    }
}
