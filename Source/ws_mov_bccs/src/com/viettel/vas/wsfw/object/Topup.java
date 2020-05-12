/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.wsfw.object;

/**
 *
 * @author tungnt64
 */
public class Topup {
    private String balance;
    private String faceVakue;
    private String err;
    private String desc;

    public String getErr() {
        return err;
    }

    public void setErr(String err) {
        this.err = err;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getBalance() {
        return balance;
    }

    public void setBalance(String balance) {
        this.balance = balance;
    }

    public String getFaceVakue() {
        return faceVakue;
    }

    public void setFaceVakue(String faceVakue) {
        this.faceVakue = faceVakue;
    }
}
