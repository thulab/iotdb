package cn.edu.tsinghua.iotdb.exception.code;

import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;

public class InsecureAPIException extends IoTDBException{
    public InsecureAPIException() {
        super(ExceptionBuilder.INSECURE_API_ERR);
    }
    public InsecureAPIException(String additionalInfo) {
        super(ExceptionBuilder.INSECURE_API_ERR, additionalInfo);
    }
}
