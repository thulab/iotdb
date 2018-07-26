package cn.edu.tsinghua.iotdb.exception.code;

import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;

public class AuthPluginException extends IoTDBException {
    public AuthPluginException() {
        super(ExceptionBuilder.AUTH_PLUGIN_ERR);
    }
    public AuthPluginException(String additionalInfo) {
        super(ExceptionBuilder.AUTH_PLUGIN_ERR, additionalInfo);
    }
}

