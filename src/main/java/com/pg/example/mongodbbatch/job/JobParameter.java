package com.pg.example.mongodbbatch.job;

/** Created by Pawel Gawedzki on 19-Mar-18.*/
public enum JobParameter {

    RUN_ID ("runId");

    private String key;
    JobParameter(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }
}
