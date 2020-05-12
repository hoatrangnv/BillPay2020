/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.paybonus.database;

import com.viettel.threadfw.manager.AppManager;
import com.viettel.paybonus.obj.CDR;
import com.viettel.paybonus.obj.CmSubRelProduct;
import com.viettel.paybonus.obj.SubRemoveService;
import com.viettel.vas.util.PoolStore;
import com.viettel.vas.util.obj.DataResources;
import com.viettel.vas.util.obj.Param;
import com.viettel.vas.util.obj.ParamList;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;

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
    private long timeStart;
    private StringBuilder br = new StringBuilder();
    private SimpleDateFormat formatDate = new SimpleDateFormat("yyyyMMddHHmmss");
    private String countryCode;

    public DbPre(String sessionName, Logger logger) {
        this.logger = logger;
        try {
            dataStore = new PoolStore(sessionName, logger);
            if (AppManager.enableQueryDbTimeout && AppManager.queryDbTimeout > 0) {
                dataStore.setQueryTimeOut(AppManager.queryDbTimeout);
            }
            countryCode = ResourceBundle.getBundle("configPayBonus").getString("country_code");
        } catch (Exception ex) {
            logger.error(loggerLabel + "ERROR DbPost", ex);
        }
    }

    public ArrayList<String> getAllVasList(String subId) {
        /**
         * select rel_product_code as rel_product_code from
         * cm_pos2.sub_rel_product where sub_id = ?
         */
        ArrayList<String> resultObj = null;
        ParamList paramList = new ParamList();
        try {
            paramList.add(new Param("REL_PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("SUB_ID", subId, Param.DataType.STRING, Param.IN));
            timeStart = System.currentTimeMillis();
            DataResources data = this.dataStore.selectTable(paramList, "SUB_REL_PRODUCT");
            logTimeDb("Time to select CM_PRE2.SUB_REL_PRODUCT", timeStart);
            resultObj = new ArrayList<String>();
            while (data.next()) {
                resultObj.add(data.getString("REL_PRODUCT_CODE"));
            }
        } catch (SQLException ex) {
            logger.error("ERROR select CM_POS2.SUB_REL_PRODUCT", ex);
            try {
                timeStart = System.currentTimeMillis();
                DataResources data = this.dataStore.selectTable(paramList, "SUB_REL_PRODUCT");
                logTimeDb("Time to select CM_PRE2.SUB_REL_PRODUCT", timeStart);
                resultObj = new ArrayList<String>();
                while (data.next()) {
                    resultObj.add(data.getString("REL_PRODUCT_CODE"));
                }
            } catch (Exception ex1) {
                logger.error("ERROR retry select CM_PRE2.SUB_REL_PRODUCT", ex1);
            }
        } catch (Exception ex) {
            logger.error("ERROR select CM_PRE2.SUB_REL_PRODUCT", ex);
        }
        return resultObj;
    }

    public String getSubInfoMobile(String isdn) {
        br.setLength(0);
        List<CmSubRelProduct> listVas = null;
        long subId = 0;
        long contractId = 0;
        long offerId = 0;
        String serial = "";
        String imsi = "";
        String serviceType = "PRE_PAID";
        String actStatus = "";
        String productCode = "";
        String activeTime = "";
        int numResetZone = 0;
        Long telServiceId = 1L;
        String language = "";
        //
        ParamList paramList = new ParamList();
        try {
            paramList.add(new Param("SUB_ID", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("SERIAL", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("IMSI", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("ACT_STATUS", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("STA_DATETIME", null, Param.DataType.DATE, Param.OUT));
            paramList.add(new Param("NUM_RESET_ZONE", null, Param.DataType.INT, Param.OUT));
            paramList.add(new Param("OFFER_ID", null, Param.DataType.LONG, Param.OUT));
//            paramList.add(new Param("LANGUAGE", null, Param.DataType.STRING, Param.OUT));
            //
            paramList.add(new Param("STATUS", 2, Param.DataType.INT, Param.IN));
            paramList.add(new Param("ISDN", isdn, Param.DataType.STRING, Param.IN));

            timeStart = System.currentTimeMillis();
            DataResources rs = dataStore.selectTable(paramList, "SUB_MB");
            logTimeDb("Time to getSubInfoMobile", timeStart);
            if (rs.next()) {
                subId = rs.getLong("SUB_ID");
                serial = rs.getString("SERIAL");
                imsi = rs.getString("IMSI");
                actStatus = rs.getString("ACT_STATUS");
                productCode = rs.getString("PRODUCT_CODE");
                activeTime = (rs.getDate("STA_DATETIME") != null) ? formatDate.format(rs.getDate("STA_DATETIME")) : "";
                numResetZone = rs.getInt("NUM_RESET_ZONE");
                offerId = rs.getLong("OFFER_ID");
//                language = (rs.getString("LANGUAGE") == null) ? Commons.defaultLang : rs.getString("LANGUAGE").toLowerCase();
            }
        } catch (SQLException ex) {
            logger.error("ERROR getSubInfoMobile", ex);
            try {
                timeStart = System.currentTimeMillis();
                DataResources rs = dataStore.selectTable(paramList, "SUB_MB");
                logTimeDb("Time to getSubInfoMobile", timeStart);
                if (rs.next()) {
                    subId = rs.getLong("SUB_ID");
                    serial = rs.getString("SERIAL");
                    imsi = rs.getString("IMSI");
                    actStatus = rs.getString("ACT_STATUS");
                    productCode = rs.getString("PRODUCT_CODE");
                    activeTime = (rs.getDate("STA_DATETIME") != null) ? formatDate.format(rs.getDate("STA_DATETIME")) : "";
                    numResetZone = rs.getInt("NUM_RESET_ZONE");
                    offerId = rs.getLong("OFFER_ID");
//                    language = (rs.getString("LANGUAGE") == null) ? Commons.defaultLang : rs.getString("LANGUAGE").toLowerCase();
                }
            } catch (Exception ex1) {
                logger.error("ERROR retry getSubInfoMobile", ex1);
                return "SYS_ERROR";
            }
        } catch (Exception ex) {
            logger.error("ERROR getSubInfoMobile", ex);
            return "SYS_ERROR";
        }

        if (subId > 0) {
            listVas = getSubRelProductMobile(subId);
            br.setLength(0);
            br.append("<SUB_INFO>\n");
            br.append("<SUB_ID>");
            br.append(subId);
            br.append("</SUB_ID>\n");
            br.append("<LANGUAGE>");
            br.append(language);
            br.append("</LANGUAGE>\n");
            br.append("<CONTRACT_ID>");
            br.append(contractId);
            br.append("</CONTRACT_ID>\n");
            br.append("<BILL_CYCLE>");
            br.append("");
            br.append("</BILL_CYCLE>\n");
            br.append("<SERIAL>");
            br.append(serial);
            br.append("</SERIAL>\n");
            br.append("<IMSI>");
            br.append(imsi);
            br.append("</IMSI>\n");
            br.append("<SERVICE_TYPE>");
            br.append(serviceType);
            br.append("</SERVICE_TYPE>\n");
            br.append("<ACT_STATUS>");
            br.append(actStatus);
            br.append("</ACT_STATUS>\n");
            br.append("<PRODUCT_CODE>");
            br.append(productCode);
            br.append("</PRODUCT_CODE>\n");
            br.append("<OFFER_ID>");
            br.append(offerId);
            br.append("</OFFER_ID>\n");
            br.append("<ACTIVE_TIME>");
            br.append(activeTime);
            br.append("</ACTIVE_TIME>\n");
            br.append("<ID_NO>");
            br.append("");
            br.append("</ID_NO>\n");
            br.append("<NUM_RESET_ZONE>");
            br.append(numResetZone);
            br.append("</NUM_RESET_ZONE>\n");
            br.append("<TEL_SERVICE_ID>");
            br.append(telServiceId.toString());
            br.append("</TEL_SERVICE_ID>\n");
            br.append("<VAS>\n");
            if ((listVas != null) && (listVas.size() > 0)) {
                for (CmSubRelProduct cmSubRelProduct : listVas) {
                    br.append("<ITEM>");
                    br.append(cmSubRelProduct.getRelProductCode());
                    br.append("</ITEM>\n");
                }
            }
            br.append("</VAS>\n");
            br.append("</SUB_INFO>");
        } else {
            br.setLength(0);
            br.append("NO_INFO_SUB");
        }

        return br.toString();
    }

    public List<CmSubRelProduct> getSubRelProductMobile(long subId) {
        List<CmSubRelProduct> listRel = null;
        ParamList paramList = new ParamList();
        try {
            paramList.add(new Param("REL_PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("SUB_ID", subId, Param.DataType.LONG, Param.IN));
            paramList.add(new Param("STATUS", 1, Param.DataType.INT, Param.IN));

            timeStart = System.currentTimeMillis();
            DataResources rs = dataStore.selectTable(paramList, "SUB_REL_PRODUCT");
            logTimeDb("Time to getSubRelProductMobile", timeStart);
            List lst = dataStore.parseListObject(CmSubRelProduct.class, rs);
            listRel = new ArrayList<CmSubRelProduct>();
            for (Object object : lst) {
                listRel.add((CmSubRelProduct) object);
            }
        } catch (SQLException ex) {
            logger.error("ERROR getSubRelProductMobile", ex);
            try {
                timeStart = System.currentTimeMillis();
                DataResources rs = dataStore.selectTable(paramList, "SUB_REL_PRODUCT");
                logTimeDb("Time to getSubRelProductMobile", timeStart);
                List lst = dataStore.parseListObject(CmSubRelProduct.class, rs);
                listRel = new ArrayList<CmSubRelProduct>();
                for (Object object : lst) {
                    listRel.add((CmSubRelProduct) object);
                }
            } catch (Exception ex1) {
                logger.error("ERROR retry getSubRelProductMobile", ex1);
            }
        } catch (Exception ex) {
            logger.error("ERROR getSubRelProductMobile", ex);
        }

        return listRel;
    }

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
        try {
            paramList.add(new Param("OFFER_ID", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("STA_DATETIME", null, Param.DataType.DATE, Param.OUT));
            paramList.add(new Param("ISDN", isdn, Param.DataType.STRING, Param.IN));

            timeStart = System.currentTimeMillis();
            DataResources rs = dataStore.selectTable(paramList, sql);
            logTimeDb("Time to getOfferId", timeStart);
            if (rs.next()) {
                offerId = rs.getLong("OFFER_ID");
            }
        } catch (SQLException ex) {
            logger.error("ERROR getOfferId", ex);
            try {
                timeStart = System.currentTimeMillis();
                DataResources rs = dataStore.selectTable(paramList, sql);
                logTimeDb("Time to getOfferId", timeStart);
                if (rs.next()) {
                    offerId = rs.getLong("OFFER_ID");
                }
            } catch (Exception ex1) {
                logger.error("ERROR retry getOfferId", ex1);
            }
        } catch (Exception ex) {
            logger.error("ERROR getOfferId", ex);
        }

        return offerId;
    }

    public List<SubRemoveService> getSubRemoveService(int id, int members, int maxRow, String relProducts) {
        String sIn = "'" + relProducts.replaceAll(",", "','") + "'";
        String sql = "select * from "
                + "(select ID, SUB_ID, ISDN, REL_PRODUCT_CODE from " + dataStore.getSchema() + ".SUB_REMOVE_SERVICE "
                + "where VALID is null and REL_PRODUCT_CODE in (" + sIn + ") and END_DATETIME>=sysdate-3) "
                + "where rownum<" + maxRow + " and mod(SUB_ID, " + members + ") = " + id;
        List<SubRemoveService> list = null;
        ParamList paramList = new ParamList();
        try {
            paramList.add(new Param("ID", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("SUB_ID", null, Param.DataType.LONG, Param.OUT));
            paramList.add(new Param("ISDN", null, Param.DataType.STRING, Param.OUT));
            paramList.add(new Param("REL_PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));

            long timeSt = System.currentTimeMillis();
            DataResources rs = dataStore.select(paramList, sql);
            logTimeDb("Time to getSubRemoveService", timeSt);
            list = new ArrayList<SubRemoveService>();
            while (rs.next()) {
                SubRemoveService remove = new SubRemoveService();
                remove.setId(rs.getLong("ID"));
                remove.setSubId(rs.getLong("SUB_ID"));
                remove.setMsisdn(countryCode + rs.getString("ISDN"));
                remove.setRelProductCode(rs.getString("REL_PRODUCT_CODE"));
                list.add(remove);
            }
        } catch (SQLException ex) {
            logger.error("ERROR getSubRemoveService", ex);
            try {
                long timeSt = System.currentTimeMillis();
                DataResources rs = dataStore.select(paramList, sql);
                logTimeDb("Time to getSubRemoveService", timeSt);
                list = new ArrayList<SubRemoveService>();
                while (rs.next()) {
                    SubRemoveService remove = new SubRemoveService();
                    remove.setId(rs.getLong("ID"));
                    remove.setSubId(rs.getLong("SUB_ID"));
                    remove.setMsisdn(countryCode + rs.getString("ISDN"));
                    remove.setRelProductCode(rs.getString("REL_PRODUCT_CODE"));
                    list.add(remove);
                }
            } catch (Exception ex1) {
                logger.error("ERROR retry getSubRemoveService", ex1);
            }
        } catch (Exception ex) {
            logger.error("ERROR getSubRemoveService", ex);
        }
        return list;
    }

    public int[] updateSubRemove(List<SubRemoveService> listRemove) {
        ArrayList<ParamList> listParam = new ArrayList<ParamList>();
        try {
            for (SubRemoveService remove : listRemove) {
                ParamList paramList = new ParamList();
                paramList.add(new Param("VALID", 1, Param.DataType.INT, Param.IN));
                paramList.add(new Param("PROCESS_DATE", "sysdate", Param.DataType.CONST, Param.IN));
                paramList.add(new Param("ID", remove.getId(), Param.DataType.LONG, Param.OUT));

                listParam.add(paramList);
            }

            long timeSt = System.currentTimeMillis();
            int[] res = dataStore.updateTable(listParam.toArray(new ParamList[listParam.size()]), "SUB_REMOVE_SERVICE");
            logTimeDb("Time to updateSubRemove", timeSt);

            return res;
        } catch (SQLException ex) {
            logger.error("ERROR updateSubRemove", ex);
            try {
                long timeSt = System.currentTimeMillis();
                int[] res = dataStore.updateTable(listParam.toArray(new ParamList[listParam.size()]), "SUB_REMOVE_SERVICE");
                logTimeDb("Time to updateSubRemove", timeSt);
                return res;
            } catch (Exception ex1) {
                logger.error("ERROR retry updateSubRemove", ex1);
            }
        } catch (Exception ex) {
            logger.error("ERROR updateSubRemove", ex);
        }

        return null;
    }

    public void logTimeDb(String strLog, long timeSt) {
        long timeEx = System.currentTimeMillis() - timeSt;

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

    public int insertVasRegister(CDR cdr) {
        ParamList paramList = new ParamList();
        try {
            
            paramList.add(new Param("VAS_CODE", cdr.getVasCode(), Param.DataType.STRING, 0));
            paramList.add(new Param("USER_ACT", cdr.getUser(), Param.DataType.STRING, 0));
            paramList.add(new Param("IP", cdr.getIp(), Param.DataType.STRING, 0));
            paramList.add(new Param("CONTRACT_ID", cdr.getContractId(), Param.DataType.STRING, 0));
            paramList.add(new Param("SUB_ID", cdr.getSubId(), Param.DataType.STRING, 0));
            paramList.add(new Param("ISDN", cdr.getMsisdn(), Param.DataType.STRING, 0));
            paramList.add(new Param("ACTION_ID", cdr.getActionId(), Param.DataType.STRING, 0));
//            if (cdr.getRegDate() == null) {
            paramList.add(new Param("DATE_TIME", "sysdate", Param.DataType.CONST, 0));
//            } else {
//                paramList.add(new Param("DATE_TIME", cdr.getRegDate(), Param.DataType.TIMESTAMP, 0));
//            }
            paramList.add(new Param("MONEY", cdr.getMoney(), Param.DataType.STRING, 0));
            paramList.add(new Param("DESCRIPTION", cdr.getDescription(), Param.DataType.STRING, 0));
            paramList.add(new Param("CHANNEL", cdr.getChannel(), Param.DataType.STRING, 0));
            paramList.add(new Param("NEW_OFFER_ID", cdr.getNewofferid(), Param.DataType.STRING, 0));
            paramList.add(new Param("NEW_PRODUCT_CODE", cdr.getNewproductcode(), Param.DataType.STRING, 0));
            paramList.add(new Param("HOME_NUMER", "", Param.DataType.STRING, 0));

            this.timeStart = System.currentTimeMillis();
            PoolStore.PoolResult pls = this.dataStore.insertTable(paramList, "VAS_REGISTER");
//            AppManager.logTimeDb("Time to insert VAS_REGISTER", this.timeStart, this.logger, this.loggerLabel);
            logTimeDb("Time to InsertVasRegister", timeStart);
            return pls == PoolStore.PoolResult.SUCCESS ? 0 : 1;
        } catch (SQLException ex) {
            this.logger.error("ERROR insert CONFIRM", ex);
            try {
                this.timeStart = System.currentTimeMillis();
                PoolStore.PoolResult pls = this.dataStore.insertTable(paramList, "VAS_REGISTER");
//                AppManager.logTimeDb("Time to insert VAS_REGISTER", this.timeStart, this.logger, this.loggerLabel);
                logTimeDb("Time to InsertVasRegister", timeStart);
                return pls == PoolStore.PoolResult.SUCCESS ? 0 : 1;
            } catch (Exception ex1) {
                this.logger.error("ERROR retry insert VAS_REGISTER", ex1);
            }
        } catch (Exception ex) {
            this.logger.error("ERROR insert VAS_REGISTER", ex);
        }
        return -1;
    }
}
