package cn.edu.tsinghua.iotdb.queryV2.dataset;

import cn.edu.tsinghua.iotdb.read.IReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.exception.write.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.common.Field;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.*;

public class EngineDataSetWithoutTimeGenerator extends QueryDataSet {

    private List<IReader> readers;

    private List<BatchData> batchDataList;

    private List<Boolean> hasDataRemaining;

    // TODO heap only need to store time
    private PriorityQueue<Long> timeHeap;

    private PriorityQueue<Point> heap;

    private Set<Long> timeSet;

    public EngineDataSetWithoutTimeGenerator(List<Path> paths, List<TSDataType> dataTypes, List<IReader> readers) throws IOException {
        super(paths, dataTypes);
        this.readers = readers;
        initHeap();
    }

    private void initHeap() throws IOException {
        heap = new PriorityQueue<>();
        for(int i = 0; i < readers.size(); i++) {
            IReader reader = readers.get(i);
            if (reader.hasNext()) {
                TimeValuePair timeValuePair = reader.next();
                heap.add(new Point(i, dataTypes.get(i), timeValuePair.getTimestamp(), timeValuePair.getValue()));
            }
        }
    }


    @Override
    public boolean hasNext() {
        return timeHeap.size() > 0;
    }

    @Override
    public RowRecord next() throws IOException {
        Point aimPoint = heap.peek();
        RowRecord record = new RowRecord(aimPoint.timestamp);

        while(heap.size() > 0 && heap.peek().timestamp == aimPoint.timestamp) {
            Point point = heap.poll();
            record.addField(point.getField());
            int index = point.readerIndex;
            if(readers.get(index).hasNext()) {
                TimeValuePair next = readers.get(index).next();
                heap.add(new Point(index, dataTypes.get(index), next.getTimestamp(), next.getValue()));
            }
        }
        return record;
    }


    private static class Point implements Comparable<Point> {
        private int readerIndex;
        private TSDataType dataType;
        private long timestamp;
        private TsPrimitiveType tsPrimitiveType;

        private Point(int readerIndex, TSDataType dataType, long timestamp, TsPrimitiveType tsPrimitiveType) {
            this.readerIndex = readerIndex;
            this.dataType = dataType;
            this.timestamp = timestamp;
            this.tsPrimitiveType = tsPrimitiveType;
        }

        @Override
        public int compareTo(Point o) {
            return Long.compare(timestamp, o.timestamp);
        }

        public Field getField() {
            Field field = new Field(dataType);
            switch (dataType) {
                case INT32:
                    field.setIntV(tsPrimitiveType.getInt());
                    break;
                case INT64:
                    field.setLongV(tsPrimitiveType.getLong());
                    break;
                case FLOAT:
                    field.setFloatV(tsPrimitiveType.getFloat());
                    break;
                case DOUBLE:
                    field.setDoubleV(tsPrimitiveType.getDouble());
                    break;
                case BOOLEAN:
                    field.setBoolV(tsPrimitiveType.getBoolean());
                    break;
                case TEXT:
                    field.setBinaryV(tsPrimitiveType.getBinary());
                    break;
                default:
                        throw new UnSupportedDataTypeException("UnSupported" + String.valueOf(dataType));
            }
            return field;
        }
    }
}
