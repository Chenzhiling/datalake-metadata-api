package com.czl.datalake.metadata.api.entity;

import java.sql.Timestamp;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/28
 * Description:
 */
public class TableHistory {

    public TableHistory(Long version,
                        String operation,
                        Timestamp timestamp) {
        this.version = version;
        this.operation = operation;
        this.timestamp = timestamp;
    }

    private Long version;

    private String operation;

    private Timestamp timestamp;

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "TableHistory{" +
                "version=" + version +
                ", operation='" + operation + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
