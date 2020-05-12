/*
 * Copyright 2012 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.object;

import com.viettel.vas.wsfw.common.CMUtils;
import com.viettel.vas.wsfw.common.Vas;
/**
 *
 * @author kdvt_tungtt8
 * @version x.x
 * @since Dec 17, 2012
 */
public class Subscriber {

    private String isdn = "";
    private String msisdn = "";
    private String subId = "";
    private String productCode = "";
    private int serviceType;
    private String contractId = "";
    private String[] vasList;
    private String billCycle = "";
    private String offerId = "";
    private String actStatus = "";
    private String status = "";
    private String activeTime = "";
    private String imsi = "";
    private String serial = "";
    private String is2g = "";
    private String currentService = "";
    private String serNameNew = "";
    private String serNameOld = "";
    private String subType = "";
    private long custReqId = 0; // tra sau
    private String regType; // tra truoc
    //

    @Override
    public String toString() {
        StringBuilder br = new StringBuilder();
        br.append("<SUB_INFO>\n");
        br.append("\r\t<MSISDN>");
        br.append(msisdn);
        br.append("</MSISDN>\n");
        br.append("\r\t<SUB_ID>");
        br.append(subId);
        br.append("</SUB_ID>\n");
        br.append("\r\t<SERVICE_TYPE>");
        br.append(serviceType == Vas.Constanst.POSTPAID ? "POSTPAID" : "PREPAID");
        br.append("</SERVICE_TYPE>\n");
        br.append("\r\t<STATUS>");
        br.append(status);
        br.append("</STATUS>\n");
        br.append("\r\t<ACT_STATUS>");
        br.append(actStatus);
        br.append("</ACT_STATUS>\n");
        br.append("\r\t<PRODUCT_CODE>");
        br.append(productCode);
        br.append("</PRODUCT_CODE>\n");
        br.append("\r\t<STA_DATETIME>");
        br.append(activeTime);
        br.append("</STA_DATETIME>\n");
        br.append("\r\t<VAS>\n");
        if (vasList != null) {
            for (String relProduct : vasList) {
                br.append("\r\t\r\t<ITEM>");
                br.append(relProduct);
                br.append("</ITEM>\n");
            }
        } else {
            br.append("\r\t\r\t<ITEM>");
            br.append("NULL");
            br.append("</ITEM>\n");
        }
        br.append("</VAS>\n");

        br.append("</SUB_INFO>");

        return br.toString();
    }

    public Subscriber(String msisdn) {
        this.msisdn = msisdn;
    }

    public Subscriber(
            String msisdn, String subId, String contractId, int serviceType,
            String serNameNew, String serNameOld) {
        this.msisdn = msisdn;
        this.serNameNew = serNameNew;
        this.serNameOld = serNameOld;
        this.serviceType = serviceType;
        this.subId = subId;
        this.contractId = contractId;
    }

    public Subscriber(String msisdn, String subinfor) {
        this.msisdn = msisdn;
        this.actStatus = CMUtils.getPropertyXML(subinfor, "ACT_STATUS");
        this.subId = CMUtils.getPropertyXML(subinfor, "SUB_ID");
        this.activeTime = CMUtils.getPropertyXML(subinfor, "ACTIVE_TIME");
        this.contractId = CMUtils.getPropertyXML(subinfor, "CONTRACT_ID");
        this.billCycle = CMUtils.getPropertyXML(subinfor, "BILL_CYCLE");
        this.imsi = CMUtils.getPropertyXML(subinfor, "IMSI");
        this.productCode = CMUtils.getPropertyXML(subinfor, "PRODUCT_CODE");
        this.offerId = CMUtils.getPropertyXML(subinfor, "OFFER_ID");
        this.serial = CMUtils.getPropertyXML(subinfor, "SERIAL");
        if ("PRE_PAID".equals(CMUtils.getPropertyXML(subinfor, "SERVICE_TYPE"))) {
            this.serviceType = Vas.Constanst.PREPAID;
        } else {
            this.serviceType = Vas.Constanst.POSTPAID;
        }

        this.vasList = CMUtils.getVasList(subinfor);
    }

    public Subscriber() {
    }

    public String getSerial() {
        return this.serial;
    }

    public void setSerial(String serial) {
        this.serial = serial;
    }

    public String getActStatus() {
        return actStatus;
    }

    public void setActStatus(String actStatus) {
        this.actStatus = actStatus;
    }

    public String getActiveTime() {
        return activeTime;
    }

    public void setActiveTime(String activeTime) {
        this.activeTime = activeTime;
    }

    public String getBillCycle() {
        return billCycle;
    }

    public void setBillCycle(String billCycle) {
        this.billCycle = billCycle;
    }

    public String getContractId() {
        return contractId;
    }

    public void setContractId(String contractId) {
        this.contractId = contractId;
    }

    public String getCurrentService() {
        return currentService;
    }

    public void setCurrentService(String currentService) {
        this.currentService = currentService;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    public String getIs2g() {
        return is2g;
    }

    public void setIs2g(String is2g) {
        this.is2g = is2g;
    }

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public String getOfferId() {
        return offerId;
    }

    public void setOfferId(String offerId) {
        this.offerId = offerId;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getSerNameNew() {
        return serNameNew;
    }

    public void setSerNameNew(String serNameNew) {
        this.serNameNew = serNameNew;
    }

    public String getSerNameOld() {
        return serNameOld;
    }

    public void setSerNameOld(String serNameOld) {
        this.serNameOld = serNameOld;
    }

    public int getServiceType() {
        return serviceType;
    }

    public void setServiceType(int serviceType) {
        this.serviceType = serviceType;
    }

    public String getSubId() {
        return subId;
    }

    public void setSubId(String subId) {
        this.subId = subId;
    }

    public String[] getVasList() {
        return vasList;
    }

    public void setVasList(String[] vasList) {
        this.vasList = vasList;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getIsdn() {
        return isdn;
    }

    public void setIsdn(String isdn) {
        this.isdn = isdn;
    }

    public String getSubType() {
        return subType;
    }

    public void setSubType(String subType) {
        this.subType = subType;
    }

    public long getCustReqId() {
        return custReqId;
    }

    public void setCustReqId(long custReqId) {
        this.custReqId = custReqId;
    }

    public String getRegType() {
        return regType;
    }

    public void setRegType(String regType) {
        this.regType = regType;
    }
}
