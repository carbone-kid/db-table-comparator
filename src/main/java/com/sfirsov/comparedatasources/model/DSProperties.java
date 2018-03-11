package com.sfirsov.comparedatasources.model;

import lombok.Data;

@Data
public class DSProperties {
    private String url;
    private String username;
    private String password;

    public DSProperties() {
    }

    public DSProperties(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }
}
