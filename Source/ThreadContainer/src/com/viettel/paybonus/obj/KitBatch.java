/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import java.util.Date;

/**
 *
 * @author dev_linh
 */
public class KitBatch {

    private Long kitBatchId;
    private String serial;
    private String isdn;
    private String productCode;
    private String stateOfRecord;
    private String resultCode;
    private String description;
    private String nodeName;
    private String moneyProduct;
    private String moneyIsdn;
    private Date processTime;
    private SaleServices saleServices;
    private SaleServicesPrice saleServicesPrice;
    private Long reasonId;
    private String saleServiceCode;
    private Double amountTax;
    private Price priceOfIsdn;
    private Double discountAmount;
    private long custId;
    private String vasCode;
    private String handsetSerial;
    private double discountForPrepaidMonth;
    private double amountVas;
    private Long subProfileId;
    private Long actionAuditId;
    private double bonusForPrepaid;
    private boolean isConnectNew;
    private String productAddOn;
    private double mountAddOn;
    private int inputType;

    public boolean isIsConnectNew() {
        return isConnectNew;
    }

    public void setIsConnectNew(boolean isConnectNew) {
        this.isConnectNew = isConnectNew;
    }

    public String getProductAddOn() {
        return productAddOn;
    }

    public void setProductAddOn(String productAddOn) {
        this.productAddOn = productAddOn;
    }

    public double getMountAddOn() {
        return mountAddOn;
    }

    public void setMountAddOn(double mountAddOn) {
        this.mountAddOn = mountAddOn;
    }

    public int getInputType() {
        return inputType;
    }

    public void setInputType(int inputType) {
        this.inputType = inputType;
    }

    public double getBonusForPrepaid() {
        return bonusForPrepaid;
    }

    public void setBonusForPrepaid(double bonusForPrepaid) {
        this.bonusForPrepaid = bonusForPrepaid;
    }

    public Long getSubProfileId() {
        return subProfileId;
    }

    public void setSubProfileId(Long subProfileId) {
        this.subProfileId = subProfileId;
    }

    public Long getActionAuditId() {
        return actionAuditId;
    }

    public void setActionAuditId(Long actionAuditId) {
        this.actionAuditId = actionAuditId;
    }

    public double getAmountVas() {
        return amountVas;
    }

    public void setAmountVas(double amountVas) {
        this.amountVas = amountVas;
    }

    public double getDiscountForPrepaidMonth() {
        return discountForPrepaidMonth;
    }

    public void setDiscountForPrepaidMonth(double discountForPrepaidMonth) {
        this.discountForPrepaidMonth = discountForPrepaidMonth;
    }

    public String getHandsetSerial() {
        return handsetSerial;
    }

    public void setHandsetSerial(String handsetSerial) {
        this.handsetSerial = handsetSerial;
    }

    public String getVasCode() {
        return vasCode;
    }

    public void setVasCode(String vasCode) {
        this.vasCode = vasCode;
    }

    public Double getDiscountAmount() {
        return discountAmount;
    }

    public void setDiscountAmount(Double discountAmount) {
        this.discountAmount = discountAmount;
    }

    public Price getPriceOfIsdn() {
        return priceOfIsdn;
    }

    public void setPriceOfIsdn(Price priceOfIsdn) {
        this.priceOfIsdn = priceOfIsdn;
    }

    public Double getAmountTax() {
        return amountTax;
    }

    public void setAmountTax(Double amountTax) {
        this.amountTax = amountTax;
    }

    public String getSaleServiceCode() {
        return saleServiceCode;
    }

    public void setSaleServiceCode(String saleServiceCode) {
        this.saleServiceCode = saleServiceCode;
    }

    public Long getReasonId() {
        return reasonId;
    }

    public void setReasonId(Long reasonId) {
        this.reasonId = reasonId;
    }

    public SaleServices getSaleServices() {
        return saleServices;
    }

    public void setSaleServices(SaleServices saleServices) {
        this.saleServices = saleServices;
    }

    public SaleServicesPrice getSaleServicesPrice() {
        return saleServicesPrice;
    }

    public void setSaleServicesPrice(SaleServicesPrice saleServicesPrice) {
        this.saleServicesPrice = saleServicesPrice;
    }

    public Long getKitBatchId() {
        return kitBatchId;
    }

    public void setKitBatchId(Long kitBatchId) {
        this.kitBatchId = kitBatchId;
    }

    public String getSerial() {
        return serial;
    }

    public void setSerial(String serial) {
        this.serial = serial;
    }

    public String getIsdn() {
        return isdn;
    }

    public void setIsdn(String isdn) {
        this.isdn = isdn;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getStateOfRecord() {
        return stateOfRecord;
    }

    public void setStateOfRecord(String stateOfRecord) {
        this.stateOfRecord = stateOfRecord;
    }

    public String getResultCode() {
        return resultCode;
    }

    public void setResultCode(String resultCode) {
        this.resultCode = resultCode;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getMoneyProduct() {
        return moneyProduct;
    }

    public void setMoneyProduct(String moneyProduct) {
        this.moneyProduct = moneyProduct;
    }

    public String getMoneyIsdn() {
        return moneyIsdn;
    }

    public void setMoneyIsdn(String moneyIsdn) {
        this.moneyIsdn = moneyIsdn;
    }

    public Date getProcessTime() {
        return processTime;
    }

    public void setProcessTime(Date processTime) {
        this.processTime = processTime;
    }

    public long getCustId() {
        return custId;
    }

    public void setCustId(long custId) {
        this.custId = custId;
    }
}
