/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import java.util.HashMap;
import org.apache.log4j.Logger;

/**
 *
 * @author itbl_jony
 */
public class Config {

    public static HashMap listMessage;
    public static String expireTime = "EMOLA_PROM_EXPIRE_TIME";
    public static String expireTimeProm = "EMOLA_PROM_EXPIRE_TIME_";
    public static String listBtsProm = "EMOLA_LIST_BTS_";
    public static String listBts = "EMOLA_PROM_LIST_BTS";
    public static String msgWinner = "MSG_WINNER";
    public static String msgNotWinner = "MSG_NOT_WINNER";
    public static String msgSystemErr = "MSG_SYSTEM_ERROR";
    public static String msgInvalidSyntax = "MSG_INVALID_SYNTAX";
    public static String msgIntroFirst = "MSG_INTRO_FIRST";
    public static String msgHelp = "MSG_HELP";
    public static String syntaxService = "SYNTAX_SERVICE";
    public static String msgOverPlayTime = "MSG_OVER_PLAY_TIME";
    public static String promPolicySpec = "EMOLA_PROM_POLICY_SPEC";
    public static String promPolicyMultiple = "EMOLA_PROM_POLICY_MULTIPLE";
    public static String promPolicy = "EMOLA_PROM_POLICY";
    public static String promSms = "EMOLA_PROM_MSG_";
    public static String prefixLuckyNumber = "EMOLA_PREFIX_LUCKY_NUMBER";
    public static String remainTV = "EMOLA_PROM_REMAIN_TV_";
    public static String remainPhone = "EMOLA_PROM_REMAIN_PHONE_";
    public static String remainFloat = "EMOLA_PROM_REMAIN_FLOAT_";
    public static String remainPhone3G = "EMOLA_PROM_REMAIN_PHONE3G_";
    public static String remainPhone4G = "EMOLA_PROM_REMAIN_PHONE4G_";
    public static String currentRankingProm = "EMOLA_CURRENT_RANKING_PROM_";
    public static String prom2PolicySpec = "EMOLA_PROM_2_POLICY_SPEC";
    public static String prom2PolicyMultiple = "EMOLA_PROM_2_POLICY_MULTIPLE";
    public static String prom2Policy = "EMOLA_PROM_2_POLICY";
    public static String promSms2 = "EMOLA_PROM_MSG_2_";
    public static String maxRankingProm = "EMOLA_MAX_RANKING_PROM_";
    public static String targetChannelCode = "EMOLA_TARGET_CHANNEL_ID";
    public static String enablePromChannel = "EMOLA_ENABLE_PROM_CHANNEL";
    public static String prom3PolicySpec = "EMOLA_PROM_3_POLICY_SPEC";
    public static String prom3PolicyMultiple = "EMOLA_PROM_3_POLICY_MULTIPLE";
    public static String prom3Policy = "EMOLA_PROM_3_POLICY";
    public static String prom3Marked = "EMOLA_PROM_3_MARKED";
    public static String promSms3 = "EMOLA_PROM_MSG_3_";
    public static String smsChannelCode = "EMOLA_SMS_CHANNEL_CODE";
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
                    "Error set Emola_Config");
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
