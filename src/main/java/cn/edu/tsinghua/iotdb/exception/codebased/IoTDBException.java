package cn.edu.tsinghua.iotdb.exception.code;

import cn.edu.tsinghua.iotdb.exception.builder.ExceptionBuilder;
import com.sun.istack.internal.NotNull;

public abstract class IoTDBException extends Exception{
    private static final long serialVersionUID = -8998294067060075273L;
    protected int errorCode;
    protected String defaultInfo;
    protected String additionalInfo;

public IoTDBException(int errorCode){
    this.defaultInfo=ExceptionBuilder.getInstance().searchInfo(errorCode);
    this.errorCode=errorCode;

}
    public IoTDBException(int errCode, @NotNull String additionalInfo){
        this.errorCode=errCode;
        this.additionalInfo=additionalInfo;
    }
    @Override
    public String getMessage(){
        if(additionalInfo==null){
            return defaultInfo;
        }else {
            return defaultInfo + ". " + additionalInfo;
        }
    }
}
