/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.wsfw.object;

import java.util.Date;

/**
 *
 * @author tungnt64
 */
public class Balance {
    private String balanceId;
    private String balanceValue;
    private String expDate;

    public String getExpDate() {
        return expDate;
    }

    public void setExpDate(String expDate) {
        this.expDate = expDate;
    }

    public String getBalanceId() {
        return balanceId;
    }

    public void setBalanceId(String balanceId) {
        this.balanceId = balanceId;
    }

    public String getBalanceValue() {
        return balanceValue;
    }

    public void setBalanceValue(String balanceValue) {
        this.balanceValue = balanceValue;
    }

    
}
