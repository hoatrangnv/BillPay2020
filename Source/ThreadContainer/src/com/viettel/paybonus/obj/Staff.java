/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import com.viettel.cluster.agent.integration.Record;
import java.util.Date;

/**
 *
 * @author dev_linh
 */
public class Staff implements Record {

    private String staffId;
    private String shopId;
    private String staffCode;
    private String tradeName;
    private String name;
    private String contactName;
    private String status;
    private Date birthday;
    private String idNo;
    private String idIssuePlace;
    private Date idIssueDate;
    private String tel;
    private String type;
    private String typeName;
    private String serial;
    private String isdn;
    private String pin;
    private String staffOwnType;
    private String staffOwnerId;
    private String channelTypeId;
    private String pointOfSale;
    private String lockStatus;
    private Date lastLockTime;
    private String province;
    private String district;
    private String precinct;
    private String address;
    private String profileState;
    private Date registryDate;
    private String usedfulWidth;
    private String surfaceArea;
    private String boardState;
    private String lastUpdateUser;
    private String lastUpdateIpAddress;
    private Date lastUpdateTime;
    private String lastUpdateKey;
    private String isWarningLogin;
    private String syncStatus;
    private String checkVat;
    private String agentType;
    private String note;
    private String streetBlockName;
    private String streetName;
    private String home;
    private String imei;
    private String anotherPhone;
    private String registrationPoint;
    private String createMethod;
    private String channelWallet;
    private String parentIdWallet;
    private String isdnWallet;
    private String btsCode;
    private String areaManageId;
    private String pricePolicy;
    private String discountPolicy;
    private String limitMoney;
    private String limitDay;
    private Date limitEndTime;
    private String oldLimitMoney;
    private String oldLimitDay;
    private String idBatch;
    private String threadName;
    private String nodeName;
    private String clusterName;
    private String resultCode;
    private String description;

    public String getLimitMoney() {
        return limitMoney;
    }

    public void setLimitMoney(String limitMoney) {
        this.limitMoney = limitMoney;
    }

    public String getLimitDay() {
        return limitDay;
    }

    public void setLimitDay(String limitDay) {
        this.limitDay = limitDay;
    }

    public Date getLimitEndTime() {
        return limitEndTime;
    }

    public void setLimitEndTime(Date limitEndTime) {
        this.limitEndTime = limitEndTime;
    }

    public String getOldLimitMoney() {
        return oldLimitMoney;
    }

    public void setOldLimitMoney(String oldLimitMoney) {
        this.oldLimitMoney = oldLimitMoney;
    }

    public String getOldLimitDay() {
        return oldLimitDay;
    }

    public void setOldLimitDay(String oldLimitDay) {
        this.oldLimitDay = oldLimitDay;
    }

    public String getPricePolicy() {
        return (pricePolicy == null) ? "" : pricePolicy;
    }

    public void setPricePolicy(String pricePolicy) {
        this.pricePolicy = pricePolicy;
    }

    public String getDiscountPolicy() {
        return (discountPolicy == null) ? "" : discountPolicy;
    }

    public void setDiscountPolicy(String discountPolicy) {
        this.discountPolicy = discountPolicy;
    }

    public String getStaffId() {
        return (staffId == null) ? "" : staffId;
    }

    public void setStaffId(String staffId) {
        this.staffId = staffId;
    }

    public String getShopId() {
        return (shopId == null) ? "" : shopId;
    }

    public void setShopId(String shopId) {
        this.shopId = shopId;
    }

    public String getStaffCode() {
        return (staffCode == null) ? "" : staffCode;
    }

    public void setStaffCode(String staffCode) {
        this.staffCode = staffCode;
    }

    public String getTradeName() {
        return (tradeName == null) ? "" : tradeName;
    }

    public void setTradeName(String tradeName) {
        this.tradeName = tradeName;
    }

    public String getName() {
        return (name == null) ? "" : name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getContactName() {
        return (contactName == null) ? "" : contactName;
    }

    public void setContactName(String contactName) {
        this.contactName = contactName;
    }

    public String getStatus() {
        return (status == null) ? "" : status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getBirthday() {
        return (birthday == null) ? new Date() : birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    public String getIdNo() {
        return (idNo == null) ? "" : idNo;
    }

    public void setIdNo(String idNo) {
        this.idNo = idNo;
    }

    public String getIdIssuePlace() {
        return (idIssuePlace == null) ? "" : idIssuePlace;
    }

    public void setIdIssuePlace(String idIssuePlace) {
        this.idIssuePlace = idIssuePlace;
    }

    public Date getIdIssueDate() {
        return (idIssueDate == null) ? new Date() : idIssueDate;
    }

    public void setIdIssueDate(Date idIssueDate) {
        this.idIssueDate = idIssueDate;
    }

    public String getTel() {
        return (tel == null) ? "" : tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getType() {
        return (type == null) ? "" : type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTypeName() {
        return (typeName == null) ? "" : typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getSerial() {
        return (serial == null) ? "" : serial;
    }

    public void setSerial(String serial) {
        this.serial = serial;
    }

    public String getIsdn() {
        return (isdn == null) ? "" : isdn;
    }

    public void setIsdn(String isdn) {
        this.isdn = isdn;
    }

    public String getPin() {
        return (pin == null) ? "" : pin;
    }

    public void setPin(String pin) {
        this.pin = pin;
    }

    public String getStaffOwnType() {
        return (staffOwnType == null) ? "" : staffOwnType;
    }

    public void setStaffOwnType(String staffOwnType) {
        this.staffOwnType = staffOwnType;
    }

    public String getStaffOwnerId() {
        return (staffOwnerId == null) ? "" : staffOwnerId;
    }

    public void setStaffOwnerId(String staffOwnerId) {
        this.staffOwnerId = staffOwnerId;
    }

    public String getChannelTypeId() {
        return (channelTypeId == null) ? "" : channelTypeId;
    }

    public void setChannelTypeId(String channelTypeId) {
        this.channelTypeId = channelTypeId;
    }

    public String getPointOfSale() {
        return (pointOfSale == null) ? "" : pointOfSale;
    }

    public void setPointOfSale(String pointOfSale) {
        this.pointOfSale = pointOfSale;
    }

    public String getLockStatus() {
        return (lockStatus == null) ? "" : lockStatus;
    }

    public void setLockStatus(String lockStatus) {
        this.lockStatus = lockStatus;
    }

    public Date getLastLockTime() {
        return (lastLockTime == null) ? new Date() : lastLockTime;
    }

    public void setLastLockTime(Date lastLockTime) {
        this.lastLockTime = lastLockTime;
    }

    public String getProvince() {
        return (province == null) ? "" : province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getDistrict() {
        return (district == null) ? "" : district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getPrecinct() {
        return (precinct == null) ? "" : precinct;
    }

    public void setPrecinct(String precinct) {
        this.precinct = precinct;
    }

    public String getAddress() {
        return (address == null) ? "" : address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getProfileState() {
        return (profileState == null) ? "" : profileState;
    }

    public void setProfileState(String profileState) {
        this.profileState = profileState;
    }

    public Date getRegistryDate() {
        return (registryDate == null) ? new Date() : registryDate;
    }

    public void setRegistryDate(Date registryDate) {
        this.registryDate = registryDate;
    }

    public String getUsedfulWidth() {
        return (usedfulWidth == null) ? "" : usedfulWidth;
    }

    public void setUsedfulWidth(String usedfulWidth) {
        this.usedfulWidth = usedfulWidth;
    }

    public String getSurfaceArea() {
        return (surfaceArea == null) ? "" : surfaceArea;
    }

    public void setSurfaceArea(String surfaceArea) {
        this.surfaceArea = surfaceArea;
    }

    public String getBoardState() {
        return (boardState == null) ? "" : boardState;
    }

    public void setBoardState(String boardState) {
        this.boardState = boardState;
    }

    public String getLastUpdateUser() {
        return (lastUpdateUser == null) ? "" : lastUpdateUser;
    }

    public void setLastUpdateUser(String lastUpdateUser) {
        this.lastUpdateUser = lastUpdateUser;
    }

    public String getLastUpdateIpAddress() {
        return (lastUpdateIpAddress == null) ? "" : lastUpdateIpAddress;
    }

    public void setLastUpdateIpAddress(String lastUpdateIpAddress) {
        this.lastUpdateIpAddress = lastUpdateIpAddress;
    }

    public Date getLastUpdateTime() {
        return (lastUpdateTime == null) ? new Date() : lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getLastUpdateKey() {
        return (lastUpdateKey == null) ? "" : lastUpdateKey;
    }

    public void setLastUpdateKey(String lastUpdateKey) {
        this.lastUpdateKey = lastUpdateKey;
    }

    public String getIsWarningLogin() {
        return (isWarningLogin == null) ? "" : isWarningLogin;
    }

    public void setIsWarningLogin(String isWarningLogin) {
        this.isWarningLogin = isWarningLogin;
    }

    public String getSyncStatus() {
        return (syncStatus == null) ? "" : syncStatus;
    }

    public void setSyncStatus(String syncStatus) {
        this.syncStatus = syncStatus;
    }

    public String getCheckVat() {
        return (checkVat == null) ? "" : checkVat;
    }

    public void setCheckVat(String checkVat) {
        this.checkVat = checkVat;
    }

    public String getAgentType() {
        return (agentType == null) ? "" : agentType;
    }

    public void setAgentType(String agentType) {
        this.agentType = agentType;
    }

    public String getNote() {
        return (note == null) ? "" : note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public String getStreetBlockName() {
        return (streetBlockName == null) ? "" : streetBlockName;
    }

    public void setStreetBlockName(String streetBlockName) {
        this.streetBlockName = streetBlockName;
    }

    public String getStreetName() {
        return (streetName == null) ? "" : streetName;
    }

    public void setStreetName(String streetName) {
        this.streetName = streetName;
    }

    public String getHome() {
        return (home == null) ? "" : home;
    }

    public void setHome(String home) {
        this.home = home;
    }

    public String getImei() {
        return (imei == null) ? "" : imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getAnotherPhone() {
        return (anotherPhone == null) ? "" : anotherPhone;
    }

    public void setAnotherPhone(String anotherPhone) {
        this.anotherPhone = anotherPhone;
    }

    public String getRegistrationPoint() {
        return (registrationPoint == null) ? "" : registrationPoint;
    }

    public void setRegistrationPoint(String registrationPoint) {
        this.registrationPoint = registrationPoint;
    }

    public String getCreateMethod() {
        return (createMethod == null) ? "" : createMethod;
    }

    public void setCreateMethod(String createMethod) {
        this.createMethod = createMethod;
    }

    public String getChannelWallet() {
        return (channelWallet == null) ? "" : channelWallet;
    }

    public void setChannelWallet(String channelWallet) {
        this.channelWallet = channelWallet;
    }

    public String getParentIdWallet() {
        return (parentIdWallet == null) ? "" : parentIdWallet;
    }

    public void setParentIdWallet(String parentIdWallet) {
        this.parentIdWallet = parentIdWallet;
    }

    public String getIsdnWallet() {
        return (isdnWallet == null) ? "" : isdnWallet;
    }

    public void setIsdnWallet(String isdnWallet) {
        this.isdnWallet = isdnWallet;
    }

    public String getBtsCode() {
        return (btsCode == null) ? "" : btsCode;
    }

    public void setBtsCode(String btsCode) {
        this.btsCode = btsCode;
    }

    public String getAreaManageId() {
        return (areaManageId == null) ? "" : areaManageId;
    }

    public void setAreaManageId(String areaManageId) {
        this.areaManageId = areaManageId;
    }

    @Override
    public String getID() {
        return this.staffId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String getBatchId() {
        return this.idBatch;
    }

    @Override
    public void setBatchId(String batchId) {
        this.idBatch = batchId;
    }

    @Override
    public String getClusterName() {
        return this.clusterName;
    }

    @Override
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public String getNodeName() {
        return this.nodeName;
    }

    @Override
    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public String getTheadName() {
        return this.threadName;
    }

    @Override
    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    @Override
    public String getResultCode() {
        return this.resultCode;
    }

    @Override
    public void setResultCode(String resultCode) {
        this.resultCode = resultCode;
    }
}
