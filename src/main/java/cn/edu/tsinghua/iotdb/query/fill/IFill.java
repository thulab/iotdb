package cn.edu.tsinghua.iotdb.query.fill;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.io.IOException;

public abstract class IFill {

    long queryTime;

    public IFill(TSDataType dataType, long queryTime) {
    }

    public IFill() {

    }

    public TSDataType getDataType() {
        return null;
    }

    public long getQueryTime() {
        return 0;
    }

    public abstract IFill copy(Path path);

    public abstract boolean hasNext();

    public abstract DynamicOneColumnData getFillResult() throws ProcessorException, IOException, PathErrorException;

}
