/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.wsfw.object;

import java.util.HashMap;
import org.apache.log4j.Logger;

/**
 *
 * @author dev_manhnv
 */
public class Config {

    public static HashMap listMessage;
    public static String smsChannel = "SMS_CHANNEL";
    public static String enterpriseProductCode = "ENTERPRISE_PRODUCT_CODE";
    public static String limitProductFee = "LIMIT_PRODUCT_FEE";
    public static String limitSubEnterprise = "LIMIT_SUB_ENTERPRISE";
    public static String enterprisePricePlanCode = "ENTERPRISE_PRICE_PLAN_CODE";
    public static String limitPrepaidMonth = "LIMIT_PREPAID_MONTH";
    public static String actionTypeChangePackage = "ACTION_TYPE_CHANGE_PACKAGE";
    public static String actionTypeAddPackage = "ACTION_ADD_PACKAGE";
    public static String channelChangePackage = "CHANNEL_CHANGE_PACKAGE";
    public static String channelTypeChangePackage = "CHANNELTYPE_CHANGE_PACKAGE";
    public static String changeEliteConfigBonusChannel = "CHANGE_ELITE_BONUS_CHANNEL";
    public static String changeEliteBonusPaidMonth = "CHANGEPACKAGE_BONUS_PAIDMONTH";
    public static String receiveMessageList = "RECEIVE_MESSAGE_LIST";
    public static String discountRenewForPaidMonth = "RENEW_BONUS_PAIDMONTH";
    public static String renewGroupElitePricePlan = "RENEW_GROUP_ELITE_PRICE_PLAN";
    public static String minSubInBatch = "MIN_SUB_IN_BATCH";
    public static String minMoneyInBatch = "MIN_MONEY_IN_BATCH";
    public static String staffCodeMakeSaleTrans = "STAFF_CODE_MAKE_INVOICE";
    public static String productElites = "PRODUCT_ELITE";
    public static String msgRenewBatchSuccess = "MSG_RENEW_BATCH_SUCCESS";
    private long configId;
    private String valuesId;
    private String valuesName;
    private String code;
    private String value;

    public Config() {
    }

    public long getConfigId() {
        return configId;
    }

    public void setConfigId(long configId) {
        this.configId = configId;
    }

    public String getValuesId() {
        return valuesId;
    }

    public void setValuesId(String valuesId) {
        this.valuesId = valuesId;
    }

    public String getValuesName() {
        return valuesName;
    }

    public void setValuesName(String valuesName) {
        this.valuesName = valuesName;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public static void setConfig(HashMap listMessage) throws Exception {
        if (listMessage == null) {
            throw new Exception(
                    "Error set Config");
        }
        Config.listMessage = listMessage;
    }

    public static String getConfig(String key, Logger logger) {
        if (key == null) {
            logger.warn("Key is null can not get Value");
            return "";
        }
        logger.info("getMessage for key: " + key);
        Config cfg = (Config) Config.listMessage.get(key.toLowerCase());
        String message;
        if (cfg == null) {
            message = "";
            logger.warn("Don't have config for key " + key + " please check CONFIG table");
        } else {
            message = cfg.getValue();
            if (message == null || message.trim().length() <= 0) {
                message = "";
            }
        }
        return message;
    }

    public static Long getLongValue(String key, Logger logger) {
        if (key == null) {
            logger.warn("Key is null can not get Value");
            return null;
        }
        try {
            logger.info("getMessage for key: " + key);
            Config cfg = (Config) Config.listMessage.get(key.toLowerCase());
            String message;
            if (cfg == null) {
                message = "";
                logger.warn("Don't have config for key " + key + " please check CONFIG table");
            } else {
                message = cfg.getValue();
                if (message == null || message.trim().length() <= 0) {
                    message = "";
                }
            }

            Long value = Long.parseLong(message);
            return value;
        } catch (Exception ex) {
            logger.error("ERROR Get value of key " + key + " error." + ex.toString() + "\n" + ex.getMessage());
            return null;
        }
    }

    public static Integer getIntValue(String key, Logger logger) {
        if (key == null) {
            logger.warn("Key is null can not get Value");
            return null;
        }
        try {
            logger.info("getMessage for key: " + key);
            Config cfg = (Config) Config.listMessage.get(key.toLowerCase());
            String message;
            if (cfg == null) {
                message = "";
                logger.warn("Don't have config for key " + key + " please check CONFIG table");
            } else {
                message = cfg.getValue();
                if (message == null || message.trim().length() <= 0) {
                    message = "";
                }
            }

            Integer value = Integer.parseInt(message);
            return value;
        } catch (Exception ex) {
            logger.error("ERROR Get value of key " + key + " error." + ex.toString() + "\n" + ex.getMessage());
            return null;
        }
    }
}
