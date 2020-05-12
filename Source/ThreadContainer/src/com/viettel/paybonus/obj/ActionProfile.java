/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.paybonus.obj;

/**
 *
 * @author Huynq13
 */
public class ActionProfile {

    private long actionProfileId;
    private boolean enoughProfile;

    public long getActionProfileId() {
        return actionProfileId;
    }

    public void setActionProfileId(long actionProfileId) {
        this.actionProfileId = actionProfileId;
    }

    public boolean isEnoughProfile() {
        return enoughProfile;
    }

    public void setEnoughProfile(boolean enoughProfile) {
        this.enoughProfile = enoughProfile;
    }
}
