package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PrioritySeriesReader.Priority;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by zhangjinrui on 2018/1/11.
 */
public class SeriesMergeSortReader implements SeriesReader {

    private List<PrioritySeriesReader> seriesReaderList;
    private PriorityQueue<Element> heap;

    public SeriesMergeSortReader(PrioritySeriesReader... seriesReaders) throws IOException {
        seriesReaderList = new ArrayList<>();
        for (int i = 0; i < seriesReaders.length; i++) {
            seriesReaderList.add(seriesReaders[i]);
        }
        init();
    }

    public SeriesMergeSortReader(List<PrioritySeriesReader> seriesReaderList) throws IOException {
        this.seriesReaderList = seriesReaderList;
        init();
    }

    private void init() throws IOException {
        heap = new PriorityQueue<>();
        for (int i = 0; i < seriesReaderList.size(); i++) {
            if (seriesReaderList.get(i).hasNext()) {
                heap.add(new Element(i, seriesReaderList.get(i).next(), seriesReaderList.get(i).getPriority()));
            }
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return heap.size() > 0;
    }

    @Override
    public TimeValuePair next() throws IOException {
        Element top = heap.peek();
        updateHeap(top);
        return top.timeValuePair;
    }

    private void updateHeap(Element top) throws IOException {
        while (heap.size() > 0 && heap.peek().timeValuePair.getTimestamp() == top.timeValuePair.getTimestamp()) {
            Element e = heap.poll();
            PrioritySeriesReader prioritySeriesReader = seriesReaderList.get(e.index);
            if (prioritySeriesReader.hasNext()) {
                heap.add(new Element(e.index, prioritySeriesReader.next(), prioritySeriesReader.getPriority()));
            }
        }
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        if (hasNext()) {
            next();
        }
    }

    private class Element implements Comparable<Element> {
        int index;
        TimeValuePair timeValuePair;
        Priority priority;

        public Element(int index, TimeValuePair timeValuePair, Priority priority) {
            this.index = index;
            this.timeValuePair = timeValuePair;
            this.priority = priority;
        }

        @Override
        public int compareTo(Element o) {
            return this.timeValuePair.getTimestamp() > o.timeValuePair.getTimestamp() ? 1 :
                    this.timeValuePair.getTimestamp() < o.timeValuePair.getTimestamp() ? -1 :
                            o.priority.compareTo(this.priority);
        }
    }
}
