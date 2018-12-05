package cn.edu.tsinghua.tsfile.timeseries.read.datatype;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * <p> Field is the components of one {@code RowRecordV2} which store a value in
 * specific data type. The value type of Field is primitive.
 */
public class Field {
    private TSDataType dataType;
    private String deviceId;
    private String measurementId;
    private boolean boolV;
    private int intV;
    private long longV;
    private float floatV;
    private double doubleV;
    private Binary binaryV;
    private boolean isNull;

    public Field(TSDataType dataType, String measurementId) {
        this.dataType = dataType;
        this.measurementId = measurementId;
    }

    public Field(TSDataType dataType, String deviceId, String measurementId) {
        this.dataType = dataType;
        this.deviceId = deviceId;
        this.measurementId = measurementId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getMeasurementId() {
        return measurementId;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public boolean getBoolV() {
        return boolV;
    }

    public void setBoolV(boolean boolV) {
        this.boolV = boolV;
    }

    public int getIntV() {
        return intV;
    }

    public void setIntV(int intV) {
        this.intV = intV;
    }

    public long getLongV() {
        return longV;
    }

    public void setLongV(long longV) {
        this.longV = longV;
    }

    public float getFloatV() {
        return floatV;
    }

    public void setFloatV(float floatV) {
        this.floatV = floatV;
    }

    public double getDoubleV() {
        return doubleV;
    }

    public void setDoubleV(double doubleV) {
        this.doubleV = doubleV;
    }

    public Binary getBinaryV() {
        return binaryV;
    }

    public void setBinaryV(Binary binaryV) {
        this.binaryV = binaryV;
    }

    private String getStringValue() {
        if (isNull) {
            return "null";
        }
        switch (dataType) {
            case BOOLEAN:
                return String.valueOf(boolV);
            case INT32:
                return String.valueOf(intV);
            case INT64:
                return String.valueOf(longV);
            case FLOAT:
                return String.valueOf(floatV);
            case DOUBLE:
                return String.valueOf(doubleV);
            case TEXT:
                return binaryV.toString();
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public String toString() {
        return getStringValue();
    }

    public void setNull() {
        this.isNull = true;
    }
}







