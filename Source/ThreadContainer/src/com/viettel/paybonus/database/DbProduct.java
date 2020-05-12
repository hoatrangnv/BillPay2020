package com.viettel.paybonus.database;

///*
// * Copyright 2012 Viettel Telecom. All rights reserved.
// * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
// */
//package com.viettel.template.database;
//
//import com.viettel.smsfw.manager.AppManager;
//import com.viettel.vas.data.obj.CmPricePlan;
//import com.viettel.vas.data.obj.CmProduct;
//import com.viettel.vas.data.utils.Data;
//import com.viettel.vas.util.PoolStore;
//import com.viettel.vas.util.obj.db.DataResources;
//import com.viettel.vas.util.obj.db.Param;
//import com.viettel.vas.util.obj.db.ParamList;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.ResourceBundle;
//import org.apache.log4j.Logger;
//
///**
// *
// * @author kdvt_tungtt8
// * @version x.x
// * @since Dec 18, 2012
// */
//public class DbProduct {
//
//    private Logger logger;
//    private String loggerLabel = DbProduct.class.getSimpleName() + ": ";
//    private StringBuilder br = new StringBuilder();
//    private PoolStore dataStore;
//    private long timeStart;
//
//    public DbProduct(String dbConfig, Logger logger) {
//        this.logger = logger;
//        try {
//            String dbType = ResourceBundle.getBundle("data").getString("dbproduct_type");
//            dataStore = new PoolStore(dbConfig, dbType, logger);
//            if (AppManager.enableQueryDbTimeout && AppManager.queryDbTimeout > 0) {
//                dataStore.setQueryTimeOut(AppManager.queryDbTimeout);
//            }
//        } catch (Exception ex) {
//            logger.error(loggerLabel + "ERROR DbPost", ex);
//        }
//    }
//
//    public CmProduct findByProductId(String productCode, int subType) {
//        CmProduct product = null;
//        ParamList paramList = new ParamList();
//        try {
//            paramList.add(new Param("PRODUCT_ID", null, Param.DataType.LONG, Param.OUT));
//            paramList.add(new Param("TELECOM_SERVICE_ID", null, Param.DataType.LONG, Param.OUT));
//            paramList.add(new Param("SERVICE_TYPE_ID", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("PRODUCT_NAME", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("PRODUCT_CODE", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("STATUS", null, Param.DataType.LONG, Param.OUT));
//            paramList.add(new Param("PRODUCT_TYPE", null, Param.DataType.STRING, Param.OUT));
//            //
//            paramList.add(new Param("PRODUCT_CODE", productCode, Param.DataType.STRING, Param.IN));
//            paramList.add(new Param("STATUS", 1, Param.DataType.INT, Param.IN));
//            paramList.add(new Param("SERVICE_TYPE_ID", subType == Data.Constanst.POSTPAID ? 1 : 2, Param.DataType.INT, Param.IN));
//
//            timeStart = System.currentTimeMillis();
//            DataResources rs = dataStore.selectTable(paramList, "PRODUCT");
//            logTimeDb("Time to select PRODUCT.PRODUCT", timeStart);
//            product = (CmProduct) dataStore.parseObject(CmProduct.class, rs);
//        } catch (SQLException ex) {
//            logger.error("ERROR select PRODUCT.PRODUCT", ex);
//            try {
//                long time = System.currentTimeMillis();
//                DataResources rs = dataStore.selectTable(paramList, "PRODUCT");
//                logTimeDb("Time to select PRODUCT.PRODUCT", time);
//                product = (CmProduct) dataStore.parseObject(CmProduct.class, rs);
//            } catch (Exception ex1) {
//                logger.error("ERROR retry select PRODUCT.PRODUCT", ex1);
//            }
//        } catch (Exception ex) {
//            logger.error("ERROR select PRODUCT.PRODUCT", ex);
//        }
//        return product;
//    }
//
//    public List<CmPricePlan> getListPricePlanByVasId(String productCode, String vasCode, int subType) {
//        String SQL_GET_PRICE_PLAN_VAS = "SELECT * FROM " + dataStore.getSchema() + ".PRICE_PLAN  WHERE STATUS = ? AND PRICE_PLAN_ID IN "
//                + "( (SELECT " + dataStore.getSchema() + ".RELATION_PRODUCT_PP.PRICE_PLAN_ID FROM " + dataStore.getSchema() + ".RELATION_PRODUCT_PP "
//                + "WHERE RELATION_PRODUCT_ID in (SELECT REL_PRODUCT_ID FROM " + dataStore.getSchema() + ".RELATION_PRODUCT "
//                + "WHERE MAIN_PRODUCT_ID = ? AND " + dataStore.getSchema() + ".RELATION_PRODUCT.RELATION_PRODUCT_ID = ?) )) "
//                + "AND EFFECT_DATE<sysdate";
//        logger.info("product_code=" + productCode + " - vas_code=" + vasCode + " => sub_type=" + subType);
//        List<CmPricePlan> listPrice = null;
//        if (productCode == null || vasCode == null) {
//            return listPrice;
//        }
//        CmProduct product = findByProductId(productCode, subType);
//        if (product == null) {
//            return listPrice;
//        }
//        CmProduct vas = findByProductId(vasCode, subType);
//        if (vas == null) {
//            return listPrice;
//        }
//
//        logger.info("main=" + product.getProductId() + " - relation_product=" + vas.getProductId());
//        ParamList paramList = new ParamList();
//        try {
//            paramList.add(new Param("PRICE_PLAN_ID", null, Param.DataType.LONG, Param.OUT));
//            paramList.add(new Param("PRICE_PLAN_NAME", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("PRICE_PLAN_CODE", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("PRICE_PLAN_TYPE", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("EXCHANGE_ID", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("TELECOM_SERVICE_ID", null, Param.DataType.LONG, Param.OUT));
//            paramList.add(new Param("DESCRIPTION", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("CREATE_DATE", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("EFFECT_DATE", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("EXPIRE_DATE", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("STATUS", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("USER_CREATE_ID", null, Param.DataType.STRING, Param.OUT));
//
//            paramList.add(new Param("STATUS", 1, Param.DataType.INT, Param.IN));
//            paramList.add(new Param("MAIN_PRODUCT_ID", product.getProductId(), Param.DataType.LONG, Param.IN));
//            paramList.add(new Param("RELATION_PRODUCT_ID", vas.getProductId(), Param.DataType.LONG, Param.IN));
//
//            timeStart = System.currentTimeMillis();
//            DataResources rs = dataStore.select(paramList, SQL_GET_PRICE_PLAN_VAS);
//            logTimeDb("Time to getListPricePlanByVasId", timeStart);
//
////            List listObj = dataStore.parseListObject(CmPricePlan.class, rs);
//            listPrice = new ArrayList<CmPricePlan>();
//            while (rs.next()) {
//                CmPricePlan cm = new CmPricePlan();
//                cm.setPricePlanId(rs.getLong("PRICE_PLAN_ID"));
//                cm.setPricePlanName(rs.getString("PRICE_PLAN_NAME"));
//                cm.setPricePlanCode(rs.getString("PRICE_PLAN_CODE"));
//                cm.setPricePlanType(rs.getString("PRICE_PLAN_TYPE"));
//                cm.setExchangeId(rs.getString("EXCHANGE_ID"));
//                cm.setTelecomServiceId(rs.getLong("TELECOM_SERVICE_ID"));
//                listPrice.add(cm);
//            }
//        } catch (SQLException ex) {
//            logger.error("ERROR getListPricePlanByVasId", ex);
//            try {
//                long time = System.currentTimeMillis();
//                DataResources rs = dataStore.select(paramList, SQL_GET_PRICE_PLAN_VAS);
//                logTimeDb("Time to getListPricePlanByVasId", time);
//
//                List listObj = dataStore.parseListObject(CmPricePlan.class, rs);
//                listPrice = new ArrayList<CmPricePlan>();
//                for (Object object : listObj) {
//                    listPrice.add((CmPricePlan) object);
//                }
//            } catch (Exception ex1) {
//                logger.error("ERROR retry getListPricePlanByVasId", ex1);
//            }
//        } catch (Exception ex) {
//            logger.error("ERROR getListPricePlanByVasId", ex);
//        }
//        return listPrice;
//    }
//
//    public HashMap<String, String> getPricePlan(String productCode, String vasCode, int subType) {
//
//        HashMap<String, String> mapPricePlan = new HashMap<String, String>();
//        List<CmPricePlan> pricePlanList = getListPricePlanByVasId(productCode, vasCode, subType);
//        if (pricePlanList != null && !pricePlanList.isEmpty()) {
//            for (CmPricePlan pp : pricePlanList) {
//                mapPricePlan.put(pp.getExchangeId(), pp.getPricePlanCode());
//            }
//        }
//        return mapPricePlan;
//    }
//
//    public boolean checkVasInProduct(String productCode, String vasCode, int subType) {
//        boolean result = false;
//        if (productCode == null || vasCode == null) {
//            return result;
//        }
//        CmProduct product = findByProductId(productCode, subType);
//        if (product == null) {
//            return result;
//        }
//        CmProduct vas = findByProductId(vasCode, subType);
//        if (vas == null) {
//            return result;
//        }
//        //
//        ParamList paramList = new ParamList();
//        try {
//            paramList.add(new Param("MAIN_PRODUCT_ID", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("MAIN_PRODUCT_ID", product.getProductId(), Param.DataType.LONG, Param.IN));
//            paramList.add(new Param("RELATION_PRODUCT_ID", vas.getProductId(), Param.DataType.LONG, Param.IN));
//
//            long time = System.currentTimeMillis();
//            DataResources rs = dataStore.selectTable(paramList, "RELATION_PRODUCT");
//            logTimeDb("Time to checkVasInProduct", time);
//            if (rs.next()) {
//                result = true;
//            }
//        } catch (SQLException ex) {
//            logger.error("ERROR checkVasInProduct", ex);
//            try {
//                paramList.add(new Param("MAIN_PRODUCT_ID", null, Param.DataType.STRING, Param.OUT));
//                paramList.add(new Param("MAIN_PRODUCT_ID", product.getProductId(), Param.DataType.LONG, Param.IN));
//                paramList.add(new Param("RELATION_PRODUCT_ID", vas.getProductId(), Param.DataType.LONG, Param.IN));
//
//                long time = System.currentTimeMillis();
//                DataResources rs = dataStore.selectTable(paramList, "RELATION_PRODUCT");
//                logTimeDb("Time to checkVasInProduct", time);
//                if (rs.next()) {
//                    result = true;
//                }
//            } catch (Exception ex1) {
//                logger.error("ERROR retry checkVasInProduct", ex1);
//            }
//        } catch (Exception ex) {
//            logger.error("ERROR checkVasInProduct", ex);
//        }
//        return result;
//    }
//
//    public List<CmPricePlan> getListPricePlanByOfferId(long offerId) {
//        String sql = "SELECT * FROM " + dataStore.getSchema() + ".PRICE_PLAN WHERE STATUS = ? AND PRICE_PLAN_ID IN "
//                + "(SELECT PRICE_PLAN_ID FROM " + dataStore.getSchema() + ".PRODUCT_OFFER_PP WHERE PRODUCT_OFFER_ID = ?) "
//                + "AND EFFECT_DATE<sysdate";
//        List<CmPricePlan> listPrice = null;
//        ParamList paramList = new ParamList();
//        try {
//            paramList.add(new Param("PRICE_PLAN_ID", null, Param.DataType.LONG, Param.OUT));
//            paramList.add(new Param("PRICE_PLAN_NAME", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("PRICE_PLAN_CODE", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("PRICE_PLAN_TYPE", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("EXCHANGE_ID", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("TELECOM_SERVICE_ID", null, Param.DataType.LONG, Param.OUT));
//            paramList.add(new Param("DESCRIPTION", null, Param.DataType.STRING, Param.OUT));
//            paramList.add(new Param("CREATE_DATE", null, Param.DataType.DATE, Param.OUT));
//            paramList.add(new Param("EFFECT_DATE", null, Param.DataType.DATE, Param.OUT));
//            paramList.add(new Param("EXPIRE_DATE", null, Param.DataType.DATE, Param.OUT));
//            paramList.add(new Param("STATUS", null, Param.DataType.LONG, Param.OUT));
//            paramList.add(new Param("USER_CREATE_ID", null, Param.DataType.LONG, Param.OUT));
//            //
//            paramList.add(new Param("STATUS", 1, Param.DataType.INT, Param.IN));
//            paramList.add(new Param("PRODUCT_OFFER_ID", offerId, Param.DataType.LONG, Param.IN));
//
//            timeStart = System.currentTimeMillis();
//            DataResources rs = dataStore.select(paramList, sql);
//            logTimeDb("Time to getListPricePlanByOfferId", timeStart);
//            List lst = dataStore.parseListObject(CmPricePlan.class, rs);
//            listPrice = new ArrayList<CmPricePlan>();
//            for (Object object : lst) {
//                listPrice.add((CmPricePlan) object);
//            }
//        } catch (SQLException ex) {
//            logger.error("ERROR getListPricePlanByOfferId", ex);
//            try {
//                timeStart = System.currentTimeMillis();
//                DataResources rs = dataStore.select(paramList, sql);
//                logTimeDb("Time to getListPricePlanByOfferId", timeStart);
//                List lst = dataStore.parseListObject(CmPricePlan.class, rs);
//                listPrice = new ArrayList<CmPricePlan>();
//                for (Object object : lst) {
//                    listPrice.add((CmPricePlan) object);
//                }
//            } catch (Exception ex1) {
//                logger.error("ERROR retry getListPricePlanByOfferId", ex1);
//            }
//        } catch (Exception ex) {
//            logger.error("ERROR getListPricePlanByOfferId", ex);
//        }
//        return listPrice;
//    }
//
//    /**
//     *
//     * @param productCode
//     * @return
//     */
//    public long getOfferIdByProductId(String productCode) {
//        String sql = "select offer_id from " + dataStore.getSchema() + ".product_offer "
//                + "where product_id = (select product_id from " + dataStore.getSchema() + ".product"
//                + " where product_code = ? and status =1 and product_type = 'P')";
//        ParamList paramList = new ParamList();
//        long offerId = -1;
//        try {
//            paramList.add(new Param("OFFER_ID", null, Param.DataType.LONG, Param.OUT));
//            paramList.add(new Param("PRODUCT_CODE", productCode, Param.DataType.STRING, Param.IN));
//
//            timeStart = System.currentTimeMillis();
//            DataResources rs = dataStore.select(paramList, sql);
//            logTimeDb("Time to getOfferIdByProductId", timeStart);
//            offerId = 0;
//            if (rs.next()) {
//                offerId = rs.getLong("OFFER_ID");
//            }
//        } catch (SQLException ex) {
//            logger.error("ERROR getOfferIdByProductId", ex);
//            offerId = -1;
//            try {
//                timeStart = System.currentTimeMillis();
//                DataResources rs = dataStore.select(paramList, sql);
//                logTimeDb("Time to getOfferIdByProductId", timeStart);
//                offerId = 0;
//                if (rs.next()) {
//                    offerId = rs.getLong("OFFER_ID");
//                }
//            } catch (Exception ex1) {
//                logger.error("ERROR retry select PRODUCT.PRODUCT", ex1);
//            }
//        } catch (Exception ex) {
//            logger.error("ERROR select PRODUCT.PRODUCT", ex);
//            offerId = -1;
//        }
//        return offerId;
//    }
//
//    /**
//     * Check thuê bao có phải loại Unlimit hay không theo CONTRACT_OFFER_ID
//     *
//     * @param offerId
//     * @return
//     */
//    public int checkUnlimitByOfferId(long offerId) {
//        /**
//         * SELECT * FROM PRODUCT.PRODUCT_OFFER_FEATURE_VIEW WHERE STATUS = 1 AND
//         * ATTRIBUTE_CODE = 'USAGE_LIMIT' AND DEFAULE_VALUE= '99999.9999' AND
//         * OFFER_ID = 2890;
//         */
//        int check = -1;
//        ParamList paramList = new ParamList();
//        try {
//            paramList.add(new Param("OFFER_ID", null, Param.DataType.LONG, Param.OUT));
//            paramList.add(new Param("STATUS", 1, Param.DataType.INT, Param.IN));
//            paramList.add(new Param("ATTRIBUTE_CODE", "USAGE_LIMIT", Param.DataType.STRING, Param.IN));
//            paramList.add(new Param("DEFAULE_VALUE", "99999.9999", Param.DataType.STRING, Param.IN));
//            paramList.add(new Param("OFFER_ID", offerId, Param.DataType.LONG, Param.IN));
//
//            timeStart = System.currentTimeMillis();
//            DataResources rs = dataStore.selectTable(paramList, "PRODUCT_OFFER_FEATURE_VIEW");
//            logTimeDb("Time to checkUnlimitByOfferId", timeStart);
//            check = 0;
//            if (rs.next()) {
//                check = 1;
//            }
//        } catch (SQLException ex) {
//            logger.error("ERROR checkUnlimitByOfferId", ex);
//            try {
//                timeStart = System.currentTimeMillis();
//                DataResources rs = dataStore.selectTable(paramList, "PRODUCT_OFFER_FEATURE_VIEW");
//                logTimeDb("Time to checkUnlimitByOfferId", timeStart);
//                check = 0;
//                if (rs.next()) {
//                    check = 1;
//                }
//            } catch (Exception ex1) {
//                logger.error("ERROR retry checkUnlimitByOfferId", ex1);
//            }
//        } catch (Exception ex) {
//            logger.error("ERROR checkUnlimitByOfferId", ex);
//        }
//
//        return check;
//    }
//
//    public void logTimeDb(String strLog, long timeSt) {
//        long timeEx = System.currentTimeMillis() - timeSt;
//
//        if (timeEx >= AppManager.minTimeDb && AppManager.loggerDbMap != null) {
//            br.setLength(0);
//            br.append(loggerLabel).
//                    append(AppManager.getTimeLevelDb(timeEx)).append(": ").
//                    append(strLog).
//                    append(": ").
//                    append(timeEx).
//                    append(" ms");
//
//            logger.warn(br);
//        } else {
//            br.setLength(0);
//            br.append(loggerLabel).
//                    append(strLog).
//                    append(": ").
//                    append(timeEx).
//                    append(" ms");
//
//            logger.info(br);
//        }
//    }
//}
