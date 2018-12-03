package cn.edu.tsinghua.tsfile.timeseries.read.query.dataset;

import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;


public class DataSetWithoutFilter implements QueryDataSet {

    private LinkedHashMap<Path, Reader> readersOfSelectedSeries;
    private PriorityQueue<Point> heap;

    public DataSetWithoutFilter(LinkedHashMap<Path, Reader> readersOfSelectedSeries) throws IOException {
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
}
