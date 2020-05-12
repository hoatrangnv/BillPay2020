/*
 * @Encrypt.java	version 1.0	29/03/2010
 *
 * Copyright 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.data.ws.utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import sun.misc.BASE64Encoder;

/**
 *
 * @author TungTT
 */
public class Encrypt {

    private static String convertToHex(byte[] data) {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < data.length; i++) {
            int halfbyte = (data[i] >>> 4) & 0x0F;
            int two_halfs = 0;
            do {
                if ((0 <= halfbyte) && (halfbyte <= 9)) {
                    buf.append((char) ('0' + halfbyte));
                } else {
                    buf.append((char) ('a' + (halfbyte - 10)));
                }
                halfbyte = data[i] & 0x0F;
            } while (two_halfs++ < 1);
        }
        return buf.toString();
    }

    public static String MD5(String text)
            throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md;
        md = MessageDigest.getInstance("MD5");
        byte[] md5hash = new byte[32];
        md.update(text.getBytes("iso-8859-1"), 0, text.length());
        md5hash = md.digest();
        return convertToHex(md5hash);
    }

    public static String byteToHex(byte[] data) {

        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            int halfbyte = (data[i] >>> 4) & 0x0F;
            int two_halfs = 0;

            do {
                if ((0 <= halfbyte) && (halfbyte <= 9)) {
                    buf.append((char) ('0' + halfbyte));
                } else {
                    buf.append((char) ('a' + (halfbyte - 10)));
                }
                halfbyte = data[i] & 0x0F;
            } while (two_halfs++ < 1);
        }
        return buf.toString();
    }

    /**
     * convert a HEX array to bytes
     *
     * @param hex
     * @return
     */
    public static byte[] hexToBytes(char[] hex) {

        int length = hex.length / 2;
        byte[] raw = new byte[length];
        for (int i = 0; i < length; i++) {
            int high = Character.digit(hex[i * 2], 16);
            int low = Character.digit(hex[i * 2 + 1], 16);
            int value = (high << 4) | low;
            if (value > 127) {
                value -= 256;
            }
            raw[i] = (byte) value;
        }
        return raw;
    }

    /**
     * get HEX string MD5
     *
     * @param data
     * @return
     */
    public static String hashMD5(byte[] data) throws Exception {


        MessageDigest md5 = MessageDigest.getInstance("MD5");
        return byteToHex(md5.digest(data));
    }

    public static synchronized String hashSHA1(String plaintext) throws Exception {
        MessageDigest md = null;
        md = MessageDigest.getInstance("SHA-1"); //step 2
        md.update(plaintext.getBytes("UTF-8")); //step 3
        byte raw[] = md.digest(); //step 4
        String hash = (new BASE64Encoder()).encode(raw); //step 5
        return hash; //step 6
    }

    public static void main(String[] args) throws Exception {
//        System.out.println(hashMD5("123456a@".getBytes()));
        System.out.println(MD5("UTTM@092018"));
        System.out.println(hashSHA1("87003838654"));
    }
}
