package com.raycom.communicate.service;

/**
 * Created by dingjinlin on 15/11/4.
 */
public class DestinationInfo {
    String serverName;
    String address;

    public DestinationInfo(String serverName, String address) {
        this.serverName = serverName;
        this.address = address;
    }

    public String getServerName() {
        return serverName;
    }

    public String getAddress() {
        return address;
    }
}
