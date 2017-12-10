package cn.edu.tsinghua.iotdb.query.fill;

import cn.edu.tsinghua.iotdb.exception.UnSupportedFillTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.Map;


public class FillFactory {

//    private Set<String> previousFillSupportType;
//    private Set<String> linearFillSupportType;
    private Map<TSDataType, IFill> fillMaps;

    private FillFactory () {
//        previousFillSupportType.add("boolean");
//        previousFillSupportType.add("int32");
//        previousFillSupportType.add("int64");
//        previousFillSupportType.add("float");
//        previousFillSupportType.add("double");
//        previousFillSupportType.add("text");
//
//        linearFillSupportType.add("int32");
//        linearFillSupportType.add("int64");
//        linearFillSupportType.add("float");
//        linearFillSupportType.add("double");
    }

    private static class SingletonHolder {
        private static final FillFactory INSTANCE = new FillFactory();
    }

    public static final FillFactory getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public IFill getNewFill(Path path, TSDataType dataType) {
        if (!fillMaps.containsKey(dataType)) {
            throw new UnSupportedFillTypeException("current datatype " + dataType + "is not defined in fill");
        }

        return fillMaps.get(dataType).copy(path);
    }

    public void addFillSupport(String dataType, long queryTime, long beforeRange) {

        switch (dataType) {
            case "int32":
            case "INT32":
                fillMaps.put(TSDataType.INT32, new PreviousFill(TSDataType.INT32, queryTime, beforeRange));
                break;
            case "int64":
            case "INT64":
                fillMaps.put(TSDataType.INT64, new PreviousFill(TSDataType.INT64, queryTime, beforeRange));
                break;
            case "float":
            case "FLOAT":
                fillMaps.put(TSDataType.INT64, new PreviousFill(TSDataType.INT64, queryTime, beforeRange));
                break;
            case "double":
            case "DOUBLE":
                fillMaps.put(TSDataType.INT64, new PreviousFill(TSDataType.INT64, queryTime, beforeRange));
                break;
            case "boolean":
            case "BOOLEAN":
                fillMaps.put(TSDataType.INT64, new PreviousFill(TSDataType.INT64, queryTime, beforeRange));
                break;
            case "text":
            case "TEXT":
                fillMaps.put(TSDataType.INT64, new PreviousFill(TSDataType.INT64, queryTime, beforeRange));
                break;
            default:
                throw new UnSupportedFillTypeException("previous fill does not support " + dataType);
        }

    }

    public void addFillSupport(String dataType, long queryTime, long beforeRange, long afterRange) {

        switch (dataType) {
            case "int32":
            case "INT32":
                fillMaps.put(TSDataType.INT32, new LinearFill(TSDataType.INT32, queryTime, beforeRange, afterRange));
                break;
            case "int64":
            case "INT64":
                fillMaps.put(TSDataType.INT64, new LinearFill(TSDataType.INT64, queryTime, beforeRange, afterRange));
                break;
            case "float":
            case "FLOAT":
                fillMaps.put(TSDataType.INT64, new LinearFill(TSDataType.INT64, queryTime, beforeRange, afterRange));
                break;
            case "double":
            case "DOUBLE":
                fillMaps.put(TSDataType.INT64, new LinearFill(TSDataType.INT64, queryTime, beforeRange, afterRange));
                break;
            default:
                throw new UnSupportedFillTypeException("linear fill does not support " + dataType);
        }
    }

}
