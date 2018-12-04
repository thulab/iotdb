package cn.edu.tsinghua.tsfile.timeseries.read.query.dataset;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.*;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;

import java.io.IOException;
import java.util.*;


public class DataSetWithoutTimeGenerator implements QueryDataSet {

    private LinkedHashMap<Path, Reader> readersOfSelectedSeries;
    private PriorityQueue<Point> heap;

    public DataSetWithoutTimeGenerator(LinkedHashMap<Path, Reader> readersOfSelectedSeries) throws IOException {
        this.readersOfSelectedSeries = readersOfSelectedSeries;
        initHeap();
    }

    private void initHeap() throws IOException {
        heap = new PriorityQueue<>();
        for (Path path : readersOfSelectedSeries.keySet()) {
            Reader seriesReader = readersOfSelectedSeries.get(path);
            if (seriesReader.hasNext()) {
                TimeValuePair timeValuePair = seriesReader.next();
                heap.add(new Point(path, timeValuePair.getTimestamp(), timeValuePair.getValue()));
            }
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return heap.size() > 0;
    }

    @Override
    public RowRecord next() throws IOException {
        Point aimPoint = heap.peek();
        RowRecord rowRecord = new RowRecord(aimPoint.timestamp);
        for (Path path : readersOfSelectedSeries.keySet()) {
            rowRecord.putField(path, null);
        }
        while (heap.size() > 0 && heap.peek().timestamp == aimPoint.timestamp) {
            Point point = heap.poll();
            rowRecord.putField(point.path, point.tsPrimitiveType);
            if (readersOfSelectedSeries.get(point.path).hasNext()) {
                TimeValuePair nextTimeValuePair = readersOfSelectedSeries.get(point.path).next();
                heap.add(new Point(point.path, nextTimeValuePair.getTimestamp(), nextTimeValuePair.getValue()));
            }
        }
        return rowRecord;
    }

    private static class Point implements Comparable<Point> {
        private Path path;
        private long timestamp;
        private TsPrimitiveType tsPrimitiveType;

        private Point(Path path, long timestamp, TsPrimitiveType tsPrimitiveType) {
            this.path = path;
            this.timestamp = timestamp;
            this.tsPrimitiveType = tsPrimitiveType;
        }

        @Override
        public int compareTo(Point o) {
            return Long.compare(timestamp, o.timestamp);
        }
    }

    public LinkedHashMap<Path, DynamicOneColumnData> seriesDataMap;
    protected DynamicOneColumnData[] seriesDataArray;

    // usingIdxsOfSeriesData[i] stores the using index of seriesDataArray[i]
    protected int[] usingIdxsOfSeriesData;

    private Map<Path, Boolean> hasDataRemaining;

    // heap only need to store
    private PriorityQueue<Long> timeHeap;

    private Set<Long> timeSet;

    public DataSetWithoutTimeGenerator(LinkedHashMap<Path, Reader> readersOfSelectedSeries, boolean flag) throws IOException {
        this.readersOfSelectedSeries = readersOfSelectedSeries;
        initHeapV2();
    }

    private void initHeapV2() throws IOException {
        hasDataRemaining = new HashMap<>();
        seriesDataMap = new LinkedHashMap<>();
        timeHeap = new PriorityQueue<>();
        timeSet = new HashSet<>();

        for (Path path : readersOfSelectedSeries.keySet()) {
            Reader reader = readersOfSelectedSeries.get(path);
            reader.hasNextBatch();
            seriesDataMap.put(path, reader.nextBatch());
            hasDataRemaining.put(path, true);
        }

        for (DynamicOneColumnData data : seriesDataMap.values()) {
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

        RowRecordV2 record = new RowRecordV2(minTime, null, null);

        record.setTimestamp(minTime);

        for (Map.Entry<Path, DynamicOneColumnData> entry : seriesDataMap.entrySet()) {

            Path path = entry.getKey();
            DynamicOneColumnData data = entry.getValue();

            Field field = new Field(data.getDataType(), path.getDeviceToString(), path.getMeasurementToString());

            if (!hasDataRemaining.get(path)) {
                field.setNull(true);
                record.addField(field);
                continue;
            }


            if (data.hasNext()) {
                if (data.getTime() == minTime) {
                    putValueToField(data, field);

                    data.next();
                    if (!data.hasNext()) {
                        readersOfSelectedSeries.get(path).hasNextBatch();
                        data = readersOfSelectedSeries.get(path).nextBatch();
                        seriesDataMap.put(path, data);
                        if (!data.hasNext()) {
                            hasDataRemaining.put(path, false);
                        } else {
                            heapPut(data.getTime());
                        }
                    } else {
                        heapPut(data.getTime());
                    }
                } else {
                    field.setNull(true);
                }

            } else {
                field.setNull(true);
                hasDataRemaining.put(path, false);
            }

            record.addField(field);
        }

        return record;
    }

    // for the reason that heap stores duplicate elements
    protected void heapPut(long time) {
        if (!timeSet.contains(time)) {
            timeSet.add(time);
            timeHeap.add(time);
        }
    }

    protected Long heapGet() {
        Long t = timeHeap.poll();
        timeSet.remove(t);
        return t;
    }

    public void putValueToField(DynamicOneColumnData col, Field f) {
        switch (col.getDataType()) {
            case BOOLEAN:
                f.setBoolV(col.getBoolean());
                break;
            case INT32:
                f.setIntV(col.getInt());
                break;
            case INT64:
                f.setLongV(col.getLong());
                break;
            case FLOAT:
                f.setFloatV(col.getFloat());
                break;
            case DOUBLE:
                f.setDoubleV(col.getDouble());
                break;
            case TEXT:
                f.setBinaryV(col.getBinary());
                break;
            default:
                throw new UnSupportedDataTypeException("UnSupported" + String.valueOf(col.getDataType()));
        }
    }
}
