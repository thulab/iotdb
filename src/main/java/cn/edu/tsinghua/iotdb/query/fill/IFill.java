package cn.edu.tsinghua.iotdb.query.fill;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.io.IOException;

public abstract class IFill {

    protected long queryTime;

    protected TSDataType dataType;

    public IFill(TSDataType dataType, long queryTime) {
        this.queryTime = queryTime;
        this.dataType = dataType;
    }

    public TSDataType getDataType() {
        return this.dataType;
    }

    public long getQueryTime() {
        return this.queryTime;
    }

    public abstract IFill copy(Path path);

    public abstract boolean hasNext();

    public abstract DynamicOneColumnData getFillResult() throws ProcessorException, IOException, PathErrorException;

}
