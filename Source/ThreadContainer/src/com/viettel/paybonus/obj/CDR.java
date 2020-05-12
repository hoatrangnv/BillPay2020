/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author kdvt_thuannh2
 */
public class CDR {

    private String vasCode;
    private String user;
    private String contractId;
    private String subId;
    private String msisdn;
    private String actionId;
    private String money;
    private String description;
    private String channel;
    private String newofferid;
    private String newproductcode;
    private String homenumber;
    private String modifiedTime;
    private String backup1;
    private String backup2;
    private String backup3;
    private String ip = "";
    private int fileTypeID;
    //
    public static int PRE_3G = 1;
    public static int POS_3G = 2;
    public static int CHANGE_LAPTOP_PRE = 3;
    public static int CHANGE_LAPTOP_POS = 4;

    public CDR() {
    }

    public CDR(String vasCode, String user, String msisdn, String subId, String contractId, String actionId,
            String modified_time, String money, String description, String channel) {

        this.actionId = actionId;
        this.modifiedTime = modified_time;
        this.channel = channel;
        this.contractId = contractId;
        this.description = description;
        this.money = money;
        this.subId = subId;
        this.vasCode = vasCode;
        this.msisdn = msisdn;
        this.user = user;
    }    

    public int getFileTypeID() {
        return this.fileTypeID;
    }

    public void setFileTypeID(int fileTypeID) {
        this.fileTypeID = fileTypeID;
    }

    public String getIp() {
        return this.ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getModifiedTime() {
        return this.modifiedTime;
    }

    public void setModifiedTime(String modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    public String getActionId() {
        return this.actionId;
    }

    public void setActionId(String actionId) {
        this.actionId = actionId;
    }

    public String getBackup1() {
        return this.backup1;
    }

    public void setBackup1(String backup1) {
        this.backup1 = backup1;
    }

    public String getBackup2() {
        return this.backup2;
    }

    public void setBackup2(String backup2) {
        this.backup2 = backup2;
    }

    public String getBackup3() {
        return this.backup3;
    }

    public void setBackup3(String backup3) {
        this.backup3 = backup3;
    }

    public String getChannel() {
        return this.channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getContractId() {
        return this.contractId;
    }

    public void setContractId(String contractId) {
        this.contractId = contractId;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getHomenumber() {
        return this.homenumber;
    }

    public void setHomenumber(String homenumber) {
        this.homenumber = homenumber;
    }

    public String getMoney() {
        return this.money;
    }

    public void setMoney(String money) {
        this.money = money;
    }

    public String getMsisdn() {
        return this.msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public String getNewofferid() {
        return this.newofferid;
    }

    public void setNewofferid(String newofferid) {
        this.newofferid = newofferid;
    }

    public String getNewproductcode() {
        return this.newproductcode;
    }

    public void setNewproductcode(String newproductcode) {
        this.newproductcode = newproductcode;
    }

    public String getSubId() {
        return this.subId;
    }

    public void setSubId(String subId) {
        this.subId = subId;
    }

    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getVasCode() {
        return this.vasCode;
    }

    public void setVasCode(String vasCode) {
        this.vasCode = vasCode;
    }
}
