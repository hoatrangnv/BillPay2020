/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.data.ws.utils;

import com.viettel.vas.wsfw.common.Common;

import com.viettel.vas.wsfw.object.UserInfo;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.Logger;

/**
 *
 * @author tuanns3
 */
public class Utils {
    //

//    public static UserInfo authenticate(String userName, String password, String ipAddress, DbProcessorFW dbProcess, Logger logger) throws Exception {
//        if (Common.mapUser == null) {
////            logger.info("Lay thong tin user tren db");
//            Common.mapUser = dbProcess.iGetUser();
//        }
//        UserInfo userInfo = new UserInfo();
//        UserInfo userInfoInMap = Common.mapUser.get(userName != null ? userName.trim() : userName);
//        if (userInfoInMap == null) {
//            logger.info("Login information is not correct, not found user : " + userName);
//            userInfo = new UserInfo();
//            userInfo.setId(UserInfo.USER_WRONG);
//            userInfo.setUser("Login information is not correct");
//            return userInfo;
//        }
//        //
//        userInfo.setUser(userName);
//        userInfo.setChannel(userInfoInMap.getChannel());
//        userInfo.setId(userInfoInMap.getId());
//        //
//        if (userInfoInMap.getIp() != null && userInfoInMap.getIp().length() > 0) {
//            // Check IP
//            String ip[] = userInfoInMap.getIp().split(",");
//            boolean pass = false;
//            for (String ipConfig : ip) {
//                if (pair(ipAddress, ipConfig.trim())) {
//                    pass = true;
//                    userInfo.setIp(ipAddress);
//                    break;
//                }
//            }
//            if (!pass) {
//                logger.info("IP not allow: " + userName + ", ip=" + ipAddress);
//                userInfo.setId(UserInfo.NOT_ALLOW);
//                userInfo.setUser("IP not allow access: user:" + userName + ", ip=" + ipAddress);
//                return userInfo;
//            }
//        }
//        userInfo.setIp(ipAddress);
//        // Check password
//        if (userInfoInMap.getPassword() != null || userInfoInMap.getPassword().trim().length() > 0) {
//            if (password.equals(userInfoInMap.getPassword())) {
//                return userInfo;
//            }
//            String passEncript = Encrypt.MD5(password);
//            System.out.println("PASS: " + Encrypt.MD5("123456a@"));
//            if (passEncript.equals(userInfoInMap.getPassword())) {
//                return userInfo;
//            }
//
//            logger.info("Login information is not correct: " + userName);
//            userInfo.setId(UserInfo.USER_WRONG);
//            userInfo.setUser("Login information is not correct");
//        }
//
//        return userInfo;
//    }
    public static boolean pair(String ipClient, String ipConfig) {
        if (ipClient == null || ipClient.equals("") || ipConfig == null || ipConfig.equals("")) {
            return false;
        }
        ipConfig = ipConfig.replaceAll("x", "\\\\d+");
        return ipClient.matches(ipConfig);
    }

    public static boolean validateMsisdn(String msisdn, String regex) {
        return (msisdn != null && msisdn.length() >= 10
                && msisdn.length() <= 15 && msisdn.matches(regex));
    }

    public static String convertDateToString(Date date, String pattern) {
        SimpleDateFormat formatter = new SimpleDateFormat(pattern);
        return formatter.format(date);
    }

    public static String dbUpdateDateTime2String(Date value) {
        if (value != null) {
            SimpleDateFormat dbUpdateDateTime = new SimpleDateFormat(
                    "yyyy-MM-dd HH:mm:ss");
            return dbUpdateDateTime.format(value);
        }
        return "";
    }

//    Is 11 digis characters
    public static synchronized boolean checkIsReference(String input) {
        if (input == null || input.equals("") || input.length() != 11) {
            return false;
        }
        try {
            for (int i = 0; i < input.length(); i++) {
                Integer a = Integer.parseInt(String.valueOf(input.charAt(i)));
            }
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    public static boolean checkSendSms(String input) {
        if (input == null || input.equals("")) {
            return false;
        }
        if (!input.equals("0") && !input.equals("1")) {
            return false;
        }
        return true;
    }

    public static void main(String[] args) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        System.out.println("PASS: " + Encrypt.MD5("biennoinhovaem2017"));
    }
}
