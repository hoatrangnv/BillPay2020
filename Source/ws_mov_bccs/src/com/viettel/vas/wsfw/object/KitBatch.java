/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.wsfw.object;

import com.viettel.im.database.BO.SaleServices;
import com.viettel.im.database.BO.SaleServicesPrice;

/**
 *
 * @author dev_linh
 */
public class KitBatch {

    private String serial;
    private String isdn;
    private String productCode;
    private String message;
    private Long result;
//    private String stateOfRecord;
    private Long kitBatchId;
    private String handsetSerial;
//    private int saleHandsetType;
//
//    public int getSaleHandsetType() {
//        return saleHandsetType;
//    }
//
//    public void setSaleHandsetType(int saleHandsetType) {
//        this.saleHandsetType = saleHandsetType;
//    }
    private SaleServices saleServices;
    private SaleServicesPrice saleServicesPrice;
    private Long reasonId;
    private String saleServiceCode;
    private double moneyProduct;
    private String currentProductCode;

    public String getCurrentProductCode() {
        return currentProductCode;
    }

    public void setCurrentProductCode(String currentProductCode) {
        this.currentProductCode = currentProductCode;
    }

    public double getMoneyProduct() {
        return moneyProduct;
    }

    public void setMoneyProduct(double moneyProduct) {
        this.moneyProduct = moneyProduct;
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

    public Long getReasonId() {
        return reasonId;
    }

    public void setReasonId(Long reasonId) {
        this.reasonId = reasonId;
    }

    public String getSaleServiceCode() {
        return saleServiceCode;
    }

    public void setSaleServiceCode(String saleServiceCode) {
        this.saleServiceCode = saleServiceCode;
    }

    public String getHandsetSerial() {
        return handsetSerial;
    }

    public void setHandsetSerial(String handsetSerial) {
        this.handsetSerial = handsetSerial;
    }

    public Long getKitBatchId() {
        return kitBatchId;
    }

    public void setKitBatchId(Long kitBatchId) {
        this.kitBatchId = kitBatchId;
    }

//    public String getStateOfRecord() {
//        return stateOfRecord;
//    }
//
//    public void setStateOfRecord(String stateOfRecord) {
//        this.stateOfRecord = stateOfRecord;
//    }
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getResult() {
        return result;
    }

    public void setResult(Long result) {
        this.result = result;
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
}
