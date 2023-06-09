package com.czl.datalake.metadata.api.exception;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/29
 * Description:
 */
public class MetaDataException extends RuntimeException {

    public MetaDataException(Throwable cause) {
        super(cause);
    }

    public MetaDataException(String errMsg) {
        super(errMsg);
    }

    public MetaDataException(String errMsg, Throwable cause) {
        super(errMsg, cause);
    }
}
