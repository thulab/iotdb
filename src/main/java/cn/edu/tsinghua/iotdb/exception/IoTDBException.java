package cn.edu.tsinghua.iotdb.exception;

public class IoTDBException extends Exception{
    private static final long serialVersionUID = -8998294067060075273L;
    public int errorCode;
    public String errorEnum;
    public String description;
    
<<<<<<< HEAD

    public IoTDBException(int errCode, String errEnum, String desc){
        super("[Error: "+errCode+"] "+errEnum+" "+ desc);
        this.errorCode=errCode;
        this.errorEnum=errEnum;
        this.description=desc;
=======
    public IoTDBException(int errCode, String errEnum, String desc){
        super("[Error: "+errCode+"] "+errEnum+" "+ desc);
        this.errorCode = errCode;
        this.errorEnum = errEnum;
        this.description = desc;
>>>>>>> 76b995c855873411fb1bdeb0efb976a9218b4bf0
    }

}
