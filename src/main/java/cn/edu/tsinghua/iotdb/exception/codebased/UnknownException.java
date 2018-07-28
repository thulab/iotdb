package cn.edu.tsinghua.iotdb.exception.code;

import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;

public class UnknownException extends  IoTDBException {
    public UnknownException() {
        super(ExceptionBuilder.UNKNOWN_ERROR);
    }
    public UnknownException(String additionalInfo) {
        super(ExceptionBuilder.UNKNOWN_ERROR, additionalInfo);
    }
}
