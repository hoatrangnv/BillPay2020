/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.wsfw.common;

import com.viettel.vas.wsfw.object.UserInfo;
import java.util.HashMap;
import java.util.Random;
import org.apache.log4j.Logger;

/**
 *
 * @author tungnt64
 */
public class Common {

    public static HashMap<String, UserInfo> mapUser = null;
    public static HashMap listMessage;
    public static String productCodeWhiteList;
    public static String productCodeBlackList;
    public static String productCodeCheckFellow;
    public static String curDir = "..";

    public static String configDir = "../etc";
    public static String logDir = "../log";
    private static final int NUM = 10;
    public static final String LOG_WRITER_CONF = "../etc/log_writer.cfg";
    public static final String subPathRequest = "/cdr";


    public class config {

        public static final String countryCode = "258";
        public static final String accountBalance = "2000&4300&5001&5100&4500&5301&5101";
        
    }

    public class ErrCode {
        public static final int FAIL = 1;
        public static final int SUCCESS = 0;
        public static final int INVALID_CARD = 2;

    }
    
    public class Provisioning{
        public static final String SET_PASS = "000047";
    }

    public class SchemaDb {

        public static final String dbPre = "CM_PRE_BRD";
        public static final String dbPost = "CM_POST_BRD";
    }

    public static void setMessage(HashMap listMessage) throws Exception {
        if (listMessage == null) {
            throw new Exception(
                    "Loi khi lay thong tin cau hinh trong bang CONFIG");
        }
        Common.listMessage = listMessage;
        Common.productCodeBlackList = (String) listMessage.get("PRODUCT_CODE_BLACK_LIST");
    }

    public static String getMessage(String key, Logger logger) {
        logger.info("Lay ma tin nhan tra ve, key: " + key);
        String message = (String) Common.listMessage.get(key);
        if (message == null || message.trim().length() == 0) {
            message = "";
            logger.warn("Chua cau hinh " + key + " trong bang CONFIG");
        }
        return message;
    }

    /**
     * @param length :length of random string
     * @return random string
     */
    public static String randomString(int length) {
        Random random = new Random();
        String strRandom = "";
        for (int i = 0; i < length; i++) {
            strRandom += "" + random.nextInt(NUM);
        }
        return strRandom;
    }
    
        /**
     * @param str xau de kiem tra
     * @return true neu xau do gom toan so
     */
    public static boolean isValidNumber(String str) {
        if (str.length() == 0 || str == null) {
            return false;
        }
        for (int i = 0; i < str.length(); i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
