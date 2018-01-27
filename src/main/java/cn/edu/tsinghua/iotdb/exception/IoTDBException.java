package cn.edu.tsinghua.iotdb.exception;

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

}
