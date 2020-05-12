/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.data.ws.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.viettel.vas.wsfw.object.CommonResponse;
import com.viettel.vas.wsfw.object.PMSagaCreateUser;
import com.viettel.vas.wsfw.object.PMSagaRequest;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ResourceBundle;
import org.apache.commons.net.util.Base64;
import org.apache.log4j.Logger;

/**
 *
 * @author dev_bacnx
 */
public class PMSagaConnecting {

	public Logger logger;

	public PMSagaConnecting() {
		logger = Logger.getLogger(PMSagaConnecting.class);
	}

	public String getAccessToken(String AuthorizationUrl, String authenInfo) {
		logger.info("Begin getAccessToken...");
		String accessToken = "";
		try {
			URL url = new URL(AuthorizationUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			byte[] encodedAuth = Base64.encodeBase64(authenInfo.getBytes());
			String authHeader = "Basic " + new String(encodedAuth);
			conn.setRequestProperty("Authorization", authHeader);
			conn.setConnectTimeout(60000);
			conn.setReadTimeout(60000);
			conn.setDoOutput(true);
			conn.getOutputStream();
			Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
			StringBuilder sbJSON = new StringBuilder();
			for (int c; (c = in.read()) >= 0;) {
				sbJSON.append((char) c);
			}
			JsonObject object = (JsonObject) new JsonParser().parse(sbJSON.toString());
			if (object != null && object.get("access_token") != null) {
				String token = object.get("access_token").toString().trim();
				if (token.startsWith("\"")) {
					accessToken = token.substring(1, token.length() - 1);
				} else {
					accessToken = token;
				}
			} else {
				logger.error("Error while get access token result:" + sbJSON.toString());
			}
		} catch (Exception ex) {
			logger.error("Exception while get access token " + ex.toString());
			accessToken = null;
		}

		return accessToken;
	}

	public CommonResponse connectPaymentSagaWS(PMSagaRequest transRequest, long staffId, long shopId, String staffCode) {
		logger.info("Begin connectPaymentSagaWS...");
		try {
			String authenUrl = ResourceBundle.getBundle("vas").getString("PMSAGA_AuthUrl");
			String pmWs = ResourceBundle.getBundle("vas").getString("PMSAGA_WS");
			String authenInfo = ResourceBundle.getBundle("vas").getString("PMSAGA_AuthInfo");
			if (authenUrl == null || authenUrl.trim().length() == 0
					|| pmWs == null || pmWs.trim().length() == 0
					|| authenInfo == null || authenInfo.trim().length() == 0) {
				return new CommonResponse("99", "Invalid configuration webservice server");
			}
			URL url = new URL(pmWs);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			String accessToken = getAccessToken(authenUrl, authenInfo);

			if (accessToken == null || accessToken.trim().length() == 0) {
				return new CommonResponse("01", "Get access token has failed.");
			}
			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept-Language", "en-MZ");
			conn.setRequestMethod("POST");
			Gson gson = new Gson();
			String request = gson.toJson(transRequest, PMSagaRequest.class);
			conn.setDoOutput(true);
			conn.getOutputStream().write(request.getBytes());
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String output;
			StringBuffer response = new StringBuffer();
			while ((output = in.readLine()) != null) {
				response.append(output);
			}
			logger.info(response.toString());
			JsonObject object = (JsonObject) new JsonParser().parse(response.toString());
			if (object != null && object.get("success") != null) {
				if ("true".equals(object.get("success").toString())) {
					return new CommonResponse("0", "The transaction was done successfully!");
				}
			}
			return new CommonResponse("1", "The transaction has failed!");
		} catch (Exception ex) {
			logger.error("Error while connectPaymentSagaWS " + ex.toString());
			return new CommonResponse("98", "An error occurred while call webservice payment-saga");
		}
	}

	public PMSagaRequest initPMSagaRequest(long staffId, long shopId, String staffCode, long contractId, long cusId,
			double paymentAmount, String paymentTypeCode, String ip, int isOpen) {
		PMSagaCreateUser createUser = new PMSagaCreateUser(staffId, shopId, staffCode, staffCode);
		PMSagaRequest req = new PMSagaRequest();
		req.setAgreementId(contractId);
		req.setCustomerId(cusId);
		req.setPaymentAmount(paymentAmount);
		req.setPaymentTypeCode(paymentTypeCode);
		req.setCreateUser(createUser);
		req.setBillCycleFrom(1);
		req.setReceiptDate(new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.000").format(new Date()));
		req.setOpenType(1);
		req.setIp(ip);
		req.setIsOpen(isOpen);
		req.setPayMethod(1);
		req.setOtpCode("");
		req.setIsdnEpay("");
		return req;
	}
}
