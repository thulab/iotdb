package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeSet;

public class TreeSetMemSeries implements IMemSeries{
    private static final Logger logger = LoggerFactory.getLogger(TreeSetMemSeries.class);

    private final TreeSet<UpdateTimeValuePair> treeSet;
    private final TSDataType dataType;

    public TreeSetMemSeries(TSDataType dataType) {
        this.dataType = dataType;
        switch (dataType){
            case BOOLEAN:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case TEXT:
                treeSet = new TreeSet<>();
                break;
            case FIXED_LEN_BYTE_ARRAY:
            case ENUMS:
            case INT96:
            case BIGDECIMAL:
            default:
                logger.error("not support data type:", dataType);
                treeSet = null;
        }
    }

    private void checkDataType(TSDataType dataType){
        assert dataType != this.dataType;
    }

    @Override
    public void putBoolean(long t, boolean v) {
        checkDataType(TSDataType.BOOLEAN);
        treeSet.add(new UpdateTimeValuePair(t,new TsPrimitiveType.TsBoolean(v)));
    }

    @Override
    public void putLong(long t, long v) {
        checkDataType(TSDataType.INT64);
        treeSet.add(new UpdateTimeValuePair(t,new TsPrimitiveType.TsLong(v)));
    }

    @Override
    public void putInt(long t, int v) {
        checkDataType(TSDataType.INT32);
        treeSet.add(new UpdateTimeValuePair(t,new TsPrimitiveType.TsInt(v)));
    }

    @Override
    public void putFloat(long t, float v) {
        checkDataType(TSDataType.FLOAT);
        treeSet.add(new UpdateTimeValuePair(t,new TsPrimitiveType.TsFloat(v)));
    }

    @Override
    public void putDouble(long t, double v) {
        checkDataType(TSDataType.DOUBLE);
        treeSet.add(new UpdateTimeValuePair(t,new TsPrimitiveType.TsDouble(v)));;
    }

    @Override
    public void putBinary(long t, Binary v) {
        checkDataType(TSDataType.TEXT);
        treeSet.add(new UpdateTimeValuePair(t,new TsPrimitiveType.TsBinary(v)));
    }

    @Override
    public void write(TSDataType dataType, long insertTime, String insertValue) {
        switch (dataType){
            case BOOLEAN:
                putBoolean(insertTime, Boolean.valueOf(insertValue));
                break;
            case INT32:
                putInt(insertTime, Integer.valueOf(insertValue));
                break;
            case INT64:
                putLong(insertTime, Long.valueOf(insertValue));
                break;
            case FLOAT:
                putFloat(insertTime, Float.valueOf(insertValue));
                break;
            case DOUBLE:
                putDouble(insertTime, Double.valueOf(insertValue));
                break;
            case TEXT:
                putBinary(insertTime, Binary.valueOf(insertValue));
                break;
            case FIXED_LEN_BYTE_ARRAY:
            case ENUMS:
            case INT96:
            case BIGDECIMAL:
            default:
                logger.error("writing data points not support data type:", dataType);
        }
    }

    @Override
    public void sortAndDeduplicate() {
        //Do Nothing
    }

    @Override
    public Iterable<UpdateTimeValuePair> query() {
        return (Iterable<UpdateTimeValuePair>) treeSet.clone();
    }

    @Override
    public void reset() {
        treeSet.clear();
    }

    @Override
    public int size() {
        //TODO: this implement just returns the number of data points in the tree set.
        return treeSet.size();
    }

    class UpdateTimeValuePair extends TimeValuePair implements Comparable{

        public UpdateTimeValuePair(long timestamp, TsPrimitiveType value) {
            super(timestamp, value);
        }

        @Override
        public int compareTo(Object object) {
            UpdateTimeValuePair o = (UpdateTimeValuePair) object;
            if(this.getTimestamp() == o.getTimestamp())
                return 0;
            return this.getTimestamp() < o.getTimestamp()? -1 : 1;
        }

        @Override
        public boolean equals(Object object) {
            if(object == null || !(object instanceof UpdateTimeValuePair))
                return false;
            UpdateTimeValuePair o = (UpdateTimeValuePair) object;
            return o.getTimestamp() == this.getTimestamp();
        }
    }
}
