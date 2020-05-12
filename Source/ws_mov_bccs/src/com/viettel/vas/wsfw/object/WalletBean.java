/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.wsfw.object;

import java.io.Serializable;

/**
 *
 * @author mov_itbl_dinhdc
 */
public class WalletBean implements Serializable {

    private String mobile;
    private String amount;
    private String user;
    private String transID;
    private String userName;
    private String functionName;
    private String functionParams;

    // Constructors
    /**
     * default constructor
     */
    public WalletBean() {
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getTransID() {
        return transID;
    }

    public void setTransID(String transID) {
        this.transID = transID;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public String getFunctionParams() {
        return functionParams;
    }

    public void setFunctionParams(String functionParams) {
        this.functionParams = functionParams;
    }
}
