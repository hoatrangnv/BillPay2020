/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

import java.util.HashMap;

/**
 *
 * @author tungtt8
 */
public class AccountInfo {

    private HashMap<String, String> accounts = new HashMap<String, String>();
    private HashMap<String, String> accountExpires = new HashMap<String, String>(); // Ngay expire theo dinh dang Pro
    private HashMap<String, String> products = new HashMap<String, String>();
    private HashMap<String, String> productExpires = new HashMap<String, String>();
    private HashMap<String, String> productEffectives = new HashMap<String, String>();

    public HashMap<String, String> getProductEffectives() {
        return productEffectives;
    }

    public void setProductEffectives(HashMap<String, String> productEffectives) {
        this.productEffectives = productEffectives;
    }

    public void putProductEffective(String productId, String effectiveTime) {
        this.productEffectives.put(productId, effectiveTime);
    }
    
    private String err;
    private String desc;

    public HashMap<String, String> getProductExpires() {
        return productExpires;
    }

    public void setProductExpires(HashMap<String, String> productExpires) {
        this.productExpires = productExpires;
    }

    public void putProductExpire(String productId, String expireTime) {
        this.productExpires.put(productId, expireTime);
    }

    public HashMap<String, String> getProducts() {
        return products;
    }

    public void setProducts(HashMap<String, String> products) {
        this.products = products;
    }

    public void putProduct(String productId, String pricePlan) {
        this.products.put(productId, pricePlan);
    }

    public HashMap<String, String> getAccounts() {
        return accounts;
    }

    public void setAccounts(HashMap<String, String> accounts) {
        this.accounts = accounts;
    }

    public void putAccount(String accountId, String balance) {
        accounts.put(accountId, balance);
    }

    public void putAccountExpire(String accountId, String expire) {
        accountExpires.put(accountId, expire);
    }

    public String getAccount(String accountId) {
        return accounts.get(accountId);
    }

    public String getAccountView(String accountId) {
        return formatView(accounts.get(accountId));
    }

    public String getAccountExpire(String accountId) {
        return accountExpires.get(accountId);
    }

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

    public void setPackage(HashMap<String, String> accounts) {
        this.accounts = accounts;
    }

    public void putPackage(String accountId, String balance) {
        accounts.put(accountId, balance);
    }

    public String formatView(String amount) {
        if (amount == null) {
            return "0";
        }
        String amountStr = String.valueOf(amount);
        String[] info = amountStr.split("\\.");
        if (info.length < 2) {
            System.out.println("Length: " + info.length);
            return amountStr;
        }
        String mod = info[1];
        mod = ff(mod);
        return info[0] + (mod.length() > 0 ? "." + mod : "");
    }

    private String ff(String s) {
        if (s.equals("")) {
            return s;
        }
        System.out.println("Char" + s.charAt(s.length() - 1));

        if (s.charAt(s.length() - 1) == '0') {
            return ff(s.substring(0, s.length() - 1));
        }

        return s;
    }

    public boolean checkContain(String balance) {
        if (accounts.containsKey(balance)) {
            return true;
        }
        return false;
    }
}
