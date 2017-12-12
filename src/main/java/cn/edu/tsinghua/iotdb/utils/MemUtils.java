package cn.edu.tsinghua.iotdb.utils;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Notice : methods in this class may not be accurate.
public class MemUtils {
    private static Logger logger  = LoggerFactory.getLogger(MemUtils.class);

    // TODO : move this down to TsFile
    public static long getTsRecordMem(TSRecord record) {
        long memUsed = 8;   // time
        memUsed += 8;       // deltaObjectId reference
        memUsed += getStringMem(record.deltaObjectId);
        for(DataPoint dataPoint : record.dataPointList) {
            memUsed += 8;  // dataPoint reference
            memUsed += getDataPointMem(dataPoint);
        }
        return memUsed;
    }

    public static long getStringMem(String str) {
        // wide char (2 bytes each) and 64B String overhead
        return str.length() * 2 + 64;
    }

    // TODO : move this down to TsFile
    public static long getDataPointMem(DataPoint dataPoint) {
        // type reference
        long memUsed = 8;
        // measurementId and its reference
        memUsed += getStringMem(dataPoint.getMeasurementId());
        memUsed += 8;

        if(dataPoint instanceof FloatDataPoint) {
            memUsed += 4;
        } else if(dataPoint instanceof IntDataPoint) {
            memUsed += 4;
        } else if(dataPoint instanceof BooleanDataPoint) {
            memUsed += 1;
        } else if(dataPoint instanceof DoubleDataPoint) {
            memUsed += 8;
        } else if(dataPoint instanceof EnumDataPoint) {
            memUsed += 4;
        } else if(dataPoint instanceof StringDataPoint) {
            StringDataPoint stringDataPoint = (StringDataPoint) dataPoint;
            memUsed += 8 + 20;  // array reference and array overhead
            memUsed += ((Binary) stringDataPoint.getValue()).values.length;
            // encoding string reference and its memory
            memUsed += 8;
            memUsed += getStringMem(((Binary) stringDataPoint.getValue()).getTextEncodingType());
        } else {
            logger.error("Unsupported data point type");
        }

        return memUsed;
    }
}
