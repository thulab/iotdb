package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;


public class PrimitiveMemTable extends AbstractMemTable {
    @Override
    protected IMemSeries genMemSeries(TSDataType dataType) {
        return new PrimitiveMemSeries(dataType);
    }
}
