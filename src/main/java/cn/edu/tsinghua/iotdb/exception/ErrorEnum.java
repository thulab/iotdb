package cn.edu.tsinghua.iotdb.exception;

public enum ErrorEnum {
    CR_UNKNOWN_ERROR(20000),
    CR_NO_PARAMETERS_EXISTS(20001),
    CR_INVALID﻿_PARAMETER_NO(20002),
    CR_CONN_HOST_ERROR(20003),
    CR_AUTH_PLUGIN_ERR(20061),
    CR_INSECURE_API_ERR(20062),
    CR_OUT_OF_MEMORY(20064),
    CR_NO_PREPARE_STMT(20130),
    CR_CON_FAIL_ERR(20220);
    private int errorCode;

    ErrorEnum(int code){ this.errorCode=code; }

    public int getErrorCode(){ return errorCode; }
}
