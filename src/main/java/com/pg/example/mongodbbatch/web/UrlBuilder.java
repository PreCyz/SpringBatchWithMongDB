package com.pg.example.mongodbbatch.web;

import com.pg.example.mongodbbatch.util.StringUtils;

/**Created by Pawel Gawedzki on 9/21/2017.*/
public class UrlBuilder {
    private String basicUrl;
    private String branchName;
    private String dataSetName;
    private String dataSetRid;
    private ResultType resultType;

    public UrlBuilder basicUrl(String basicUrl) {
        this.basicUrl = basicUrl;
        return this;
    }

    public UrlBuilder branchName(String branchName) {
        this.branchName = branchName;
        return this;
    }

    public UrlBuilder dataSetName(String dataSetName) {
        this.dataSetName = dataSetName;
        return this;
    }

    public UrlBuilder dataSetRid(String dataSetRid) {
        this.dataSetRid = dataSetRid;
        return this;
    }

    public UrlBuilder resultType(ResultType resultType) {
        this.resultType = resultType;
        return this;
    }

    public String build() {
        if (StringUtils.nullOrEmpty(basicUrl)) {
            throw new RuntimeException("Palantir URL is not specified. Please update application.properties with proper value for the key 'gcss.endpoint'.");
        }
        if (StringUtils.nullOrEmpty(branchName)) {
            throw new RuntimeException("Data set branch name is not specified. Please update application.properties with proper value for the key 'gcss.branch'.");
        }
        if (StringUtils.nullOrEmpty(dataSetRid)) {
            throw new RuntimeException("Data set rid is not specified. Without RID I do now know what to download.");
        }
        if (resultType == null) {
            resultType = ResultType.csv;
        }
        return String.format("%s/%s/branches/%s/%s", basicUrl, dataSetRid, branchName, resultType.name());
    }

    @Override
    public String toString() {
        return "UrlBuilder{" +
                "basicUrl='" + basicUrl + '\'' +
                ", branchName='" + branchName + '\'' +
                ", dataSetName='" + dataSetName + '\'' +
                ", dataSetRid='" + dataSetRid + '\'' +
                ", resultType=" + resultType +
                '}';
    }
}
