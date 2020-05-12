/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author Huynq13
 */
public class ItemFee {

    private long itemFeeId;
    private long amount;
    private long checkProfile;

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public long getItemFeeId() {
        return itemFeeId;
    }

    public void setItemFeeId(long itemFeeId) {
        this.itemFeeId = itemFeeId;
    }

    public long getCheckProfile() {
        return checkProfile;
    }

    public void setCheckProfile(long checkProfile) {
        this.checkProfile = checkProfile;
    }
}
