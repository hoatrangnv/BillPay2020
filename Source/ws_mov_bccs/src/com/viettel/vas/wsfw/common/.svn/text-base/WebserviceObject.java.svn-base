/*
 * Copyright (C) 2010 Viettel Telecom. All rights reserved.
 * VIETTEL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.viettel.vas.wsfw.common;

/**
 *
 * @author minhnh@viettel.com.vn
 * @since Jun 3, 2013
 * @version 1.0
 */
public class WebserviceObject {

    private String name;
    private String ip;
    private String port;
    private String path;
    private String implementClass;
    private String url;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getImplementClass() {
        return implementClass;
    }

    public void setImplementClass(String implementClass) {
        this.implementClass = implementClass;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void makeUrl() {
        this.url = "http://" + this.ip + ":" + this.port + "/" + this.path;
    }
}
