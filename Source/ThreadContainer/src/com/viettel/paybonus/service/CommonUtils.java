/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.service;

import java.text.StringCharacterIterator;

/**
 *
 * @author tuantm11
 */
public class CommonUtils {

	/**
	 * function to check null or empty string
	 *
	 * @param str
	 * @return
	 */
	public static boolean isNullOrEmpty(String str) {
		if (str != null && !"".equals(str)) {
			return false;
		}
		return true;
	}

	/**
	 * standard isdn: cut prefix: 0, 00, 0084, +84
	 *
	 * @param isdn
	 * @return
	 */
	public static String standardIsdn(String isdn) {
		if (!CommonUtils.isNullOrEmpty(isdn)) {
			if (isdn.startsWith("00")) {
				isdn = isdn.substring(2);
			}
			if (isdn.startsWith("0")) {
				isdn = isdn.substring(1);
			}
			if (isdn.startsWith("+")) {
				isdn = isdn.substring(1);
			}
			if (isdn.startsWith("84")) {
				isdn = isdn.substring(2);
			}
		}
		return isdn;
	}

	//LinhNBV 20180904: JSON Escape Utility
	public static String crunchifyJSONEscapeUtil(String crunchifyJSON) {
		final StringBuilder crunchifyNewJSON = new StringBuilder();

		// StringCharacterIterator class iterates over the entire String
		StringCharacterIterator iterator = new StringCharacterIterator(crunchifyJSON);
		char myChar = iterator.current();

		// DONE = \\uffff (not a character)
		while (myChar != StringCharacterIterator.DONE) {
			if (myChar == '\"') {
				crunchifyNewJSON.append("\\\"");
			} else if (myChar == '\t') {
				crunchifyNewJSON.append("\\t");
			} else if (myChar == '\f') {
				crunchifyNewJSON.append("\\f");
			} else if (myChar == '\n') {
				crunchifyNewJSON.append("\\n");
			} else if (myChar == '\r') {
				crunchifyNewJSON.append("\\r");
			} else if (myChar == '\\') {
				crunchifyNewJSON.append("\\\\");
			} else if (myChar == '/') {
				crunchifyNewJSON.append("\\/");
			} else if (myChar == '\b') {
				crunchifyNewJSON.append("\\b");
			} else {

				// nothing matched - just as text as it is.
				crunchifyNewJSON.append(myChar);
			}
			myChar = iterator.next();
		}
		return crunchifyNewJSON.toString();
	}
}
