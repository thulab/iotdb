package cn.edu.tsinghua.iotdb.exception;

import cn.edu.tsinghua.iotdb.exception.handler.ExceptionHandler;

public class IoTDBException extends Exception{
    private static final long serialVersionUID = -8998294067060075273L;
    public int errorCode;
    public String errorEnum;
    public String description;

    public IoTDBException(int errCode, String errEnum, String desc){
        super("[Error: "+errCode+"] "+errEnum+" "+ desc);
        this.errorCode=errCode;
        this.errorEnum=errEnum;
        this.description=desc;
    }
    public IoTDBException(ErrorEnum errEnum){
        this.errorEnum=errEnum.name();
        this.errorCode= errEnum.getErrorCode();
        this.description= ExceptionHandler.getInstance().searchInfo(this.errorCode);
    }
}
