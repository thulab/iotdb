package cn.edu.tsinghua.tsfile.timeseries.read.query.dataset;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.*;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.BatchData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;

import java.io.IOException;
import java.util.*;


public class DataSetWithoutTimeGenerator extends QueryDataSet {

    private List<Reader> readers;

    private List<BatchData> batchDataList;

    private List<Boolean> hasDataRemaining;

    // heap only need to store time
    private PriorityQueue<Long> timeHeap;

    private Set<Long> timeSet;

    public DataSetWithoutTimeGenerator(List<Path> paths, List<TSDataType> dataTypes, List<Reader> readers) throws IOException {
        super(paths, dataTypes);
        this.readers = readers;
        initHeapV2();
    }

    private void initHeapV2() throws IOException {
        hasDataRemaining = new ArrayList<>();
        batchDataList = new ArrayList<>();
        timeHeap = new PriorityQueue<>();
        timeSet = new HashSet<>();

        for(int i = 0; i < paths.size(); i++) {
            Reader reader = readers.get(i);
            reader.hasNextBatch();
            batchDataList.add(reader.nextBatch());
            hasDataRemaining.add(true);
        }

        for (BatchData data : batchDataList) {
            if (data.hasNext()) {
                heapPut(data.getTime());
            }
        }
    }

    @Override
    public boolean hasNextV2() {
        return timeHeap.size() > 0;
    }

    @Override
    public RowRecordV2 nextV2() throws IOException {
        long minTime = heapGet();

        RowRecordV2 record = new RowRecordV2(minTime);

        for(int i = 0; i < paths.size(); i++) {
            BatchData data = batchDataList.get(i);
            Field field = new Field(data.getDataType());

            if (!hasDataRemaining.get(i)) {
                field.setNull();
                record.addField(field);
                continue;
            }

            if (data.hasNext()) {
                if (data.getTime() == minTime) {
                    putValueToField(data, field);

                    data.next();
                    if (!data.hasNext()) {
                        Reader reader = readers.get(i);
                        reader.hasNextBatch();
                        data = reader.nextBatch();
                        batchDataList.set(i, data);
                        if (!data.hasNext()) {
                            hasDataRemaining.set(i, false);
                        } else {
                            heapPut(data.getTime());
                        }
                    } else {
                        heapPut(data.getTime());
                    }
                } else {
                    field.setNull();
                }

            } else {
                field.setNull();
                hasDataRemaining.set(i, false);
            }

            record.addField(field);
        }

        return record;
    }

    // for the reason that heap stores duplicate elements
    private void heapPut(long time) {
        if (!timeSet.contains(time)) {
            timeSet.add(time);
            timeHeap.add(time);
        }
    }

    private Long heapGet() {
        Long t = timeHeap.poll();
        timeSet.remove(t);
        return t;
    }

    private void putValueToField(BatchData col, Field field) {
        switch (col.getDataType()) {
            case BOOLEAN:
                field.setBoolV(col.getBoolean());
                break;
            case INT32:
                field.setIntV(col.getInt());
                break;
            case INT64:
                field.setLongV(col.getLong());
                break;
            case FLOAT:
                field.setFloatV(col.getFloat());
                break;
            case DOUBLE:
                field.setDoubleV(col.getDouble());
                break;
            case TEXT:
                field.setBinaryV(col.getBinary());
                break;
            default:
                throw new UnSupportedDataTypeException("UnSupported" + String.valueOf(col.getDataType()));
        }
    }
}
