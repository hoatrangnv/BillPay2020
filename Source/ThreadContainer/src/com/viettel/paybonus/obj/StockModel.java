/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import java.util.ArrayList;

/**
 *
 * @author itbl_linh
 */
public class StockModel implements java.io.Serializable {

    private Long stockModelId;
    private String stockModelCode;
    private Long stockTypeId;
    private String name;
    private Long checkSerial;
    private Long checkDeposit;
    private Long checkDial;
    private String unit;
    private Long status;
    private String notes;
    private Long discountGroupId;
    private Long profileId;
    private Long telecomServiceId;
    private Long stockModelType;
    private String telecomServiceName;
    private String unitName;
    private Long discountModelMapId; //tamdt1, them de phuc vu cho viec khai bao nhom chiet khau
    private Long sourcePrice;
    private String accountingModelCode;
    private String accountingModelName;
    private long quantitySaling;
    private String type;
    private String stockModelName;
    private String tableName;
    private ArrayList<Serial> listSerial;

    public ArrayList<Serial> getListSerial() {
        return listSerial;
    }

    public void setListSerial(ArrayList<Serial> listSerial) {
        this.listSerial = listSerial;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStockModelName() {
        return stockModelName;
    }

    public void setStockModelName(String stockModelName) {
        this.stockModelName = stockModelName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getQuantitySaling() {
        return quantitySaling;
    }

    public void setQuantitySaling(long quantitySaling) {
        this.quantitySaling = quantitySaling;
    }

    public String getAccountingModelCode() {
        return accountingModelCode;
    }

    public void setAccountingModelCode(String accountingModelCode) {
        this.accountingModelCode = accountingModelCode;
    }

    public String getAccountingModelName() {
        return accountingModelName;
    }

    public void setAccountingModelName(String accountingModelName) {
        this.accountingModelName = accountingModelName;
    }

    // Constructors
    /**
     * default constructor
     */
    public StockModel() {
    }

    public StockModel(Long stockModelId, String stockModelCode, String name, Long stockModelType, Long checkSerial) {
        this.stockModelId = stockModelId;
        this.stockModelCode = stockModelCode;
        this.name = name;
        this.checkSerial = checkSerial;
        this.stockModelType = stockModelType;
    }

    /**
     * minimal constructor
     */
    public StockModel(Long stockModelId, String stockModelCode, String name,
            Long status) {
        this.stockModelId = stockModelId;
        this.stockModelCode = stockModelCode;
        this.name = name;
        this.status = status;
    }

    public StockModel(Long stockModelId, String stockModelCode, Long stockTypeId, String name) {
        this.stockModelId = stockModelId;
        this.stockModelCode = stockModelCode;
        this.stockTypeId = stockTypeId;
        this.name = name;
    }

    // Property accessors
    public Long getProfileId() {
        return profileId;
    }

    public void setProfileId(Long profileId) {
        this.profileId = profileId;
    }

    public Long getStockModelId() {
        return this.stockModelId;
    }

    public void setStockModelId(Long stockModelId) {
        this.stockModelId = stockModelId;
    }

    public String getStockModelCode() {
        return this.stockModelCode;
    }

    public void setStockModelCode(String stockModelCode) {
        this.stockModelCode = stockModelCode;
    }

    public Long getStockTypeId() {
        return this.stockTypeId;
    }

    public void setStockTypeId(Long stockTypeId) {
        this.stockTypeId = stockTypeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTelecomServiceId() {
        return telecomServiceId;
    }

    public void setTelecomServiceId(Long telecomServiceId) {
        this.telecomServiceId = telecomServiceId;
    }

    public Long getCheckSerial() {
        return this.checkSerial;
    }

    public void setCheckSerial(Long checkSerial) {
        this.checkSerial = checkSerial;
    }

    public Long getCheckDeposit() {
        return this.checkDeposit;
    }

    public void setCheckDeposit(Long checkDeposit) {
        this.checkDeposit = checkDeposit;
    }

    public Long getCheckDial() {
        return this.checkDial;
    }

    public void setCheckDial(Long checkDial) {
        this.checkDial = checkDial;
    }

    public String getUnit() {
        return this.unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public Long getStatus() {
        return this.status;
    }

    public void setStatus(Long status) {
        this.status = status;
    }

    public String getNotes() {
        return this.notes;
    }

    public void setNotes(String notes) {
        this.notes = notes;
    }

    public Long getDiscountGroupId() {
        return this.discountGroupId;
    }

    public void setDiscountGroupId(Long discountGroupId) {
        this.discountGroupId = discountGroupId;
    }

    public String getTelecomServiceName() {
        return telecomServiceName;
    }

    public void setTelecomServiceName(String telecomServiceName) {
        this.telecomServiceName = telecomServiceName;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public Long getDiscountModelMapId() {
        return discountModelMapId;
    }

    public void setDiscountModelMapId(Long discountModelMapId) {
        this.discountModelMapId = discountModelMapId;
    }

    public Long getStockModelType() {
        return stockModelType;
    }

    public void setStockModelType(Long stockModelType) {
        this.stockModelType = stockModelType;
    }

    public Long getSourcePrice() {
        return sourcePrice;
    }

    public void setSourcePrice(Long sourcePrice) {
        this.sourcePrice = sourcePrice;
    }
}
