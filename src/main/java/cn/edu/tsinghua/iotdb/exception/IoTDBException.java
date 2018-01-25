package cn.edu.tsinghua.iotdb.exception;

public class IoTDBException extends Exception{
    private static final long serialVersionUID = -8998294067060075273L;
    private int ErrorCode;
    private String ErrorEnum;
    private String Description;

    IoTDBException(int ErrCode, String ErrEnum, String Desc){
        super("[Error: "+ErrCode+"] "+ErrEnum+" "+ Desc);
    }

}
