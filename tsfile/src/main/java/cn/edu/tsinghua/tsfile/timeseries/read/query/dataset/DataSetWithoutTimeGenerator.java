package cn.edu.tsinghua.tsfile.timeseries.read.query.dataset;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.*;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.BatchData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;

import java.io.IOException;
import java.util.*;


/**
 * multi-way merging data set ,no need to use TimeGenerator
 */
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
        initHeap();
    }

    private void initHeap() throws IOException {
        hasDataRemaining = new ArrayList<>();
        batchDataList = new ArrayList<>();
        timeHeap = new PriorityQueue<>();
        timeSet = new HashSet<>();

        for (int i = 0; i < paths.size(); i++) {
            Reader reader = readers.get(i);
            if (!reader.hasNextBatch()) {
                batchDataList.add(new BatchData());
                hasDataRemaining.add(false);
            } else {
                batchDataList.add(reader.nextBatch());
                hasDataRemaining.add(true);
            }
        }

        for (BatchData data : batchDataList) {
            if (data.hasNext()) {
                heapPut(data.currentTime());
            }
        }
    }

    @Override
    public boolean hasNext() {
        return timeHeap.size() > 0;
    }

    @Override
    public RowRecord next() throws IOException {
        long minTime = heapGet();

        RowRecord record = new RowRecord(minTime);

        for (int i = 0; i < paths.size(); i++) {

            Field field = new Field(dataTypes.get(i));

            if (!hasDataRemaining.get(i)) {
                field.setNull();
                record.addField(field);
                continue;
            }

            BatchData data = batchDataList.get(i);

            if (data.currentTime() == minTime) {
                putValueToField(data, field);
                data.next();

                if (!data.hasNext()) {
                    Reader reader = readers.get(i);
                    if (reader.hasNextBatch()) {
                        data = reader.nextBatch();
                        if(data.hasNext()) {
                            batchDataList.set(i, data);
                            heapPut(data.currentTime());
                        } else {
                            hasDataRemaining.set(i, false);
                        }
                    } else {
                        hasDataRemaining.set(i, false);
                    }
                } else {
                    heapPut(data.currentTime());
                }

            } else {
                field.setNull();
            }

            record.addField(field);
        }

        return record;
    }

    // keep heap from storing duplicate time
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
