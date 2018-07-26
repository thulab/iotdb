package cn.edu.tsinghua.iotdb.exception.code;

import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;

public class ConnectionHostException extends IoTDBException {
    public ConnectionHostException() {
        super(ExceptionBuilder.CONN_HOST_ERROR);
    }
    public ConnectionHostException(String additionalInfo) {
        super(ExceptionBuilder.CONN_HOST_ERROR, additionalInfo);
    }
}
