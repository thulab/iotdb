package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader.Priority;
import cn.edu.tsinghua.iotdb.read.ISeriesReader;
import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import cn.edu.tsinghua.tsfile.read.common.BatchData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;


public class PriorityMergeReader implements ISeriesReader {

    protected List<PriorityTimeValuePairReader> readerList;
    protected PriorityQueue<Element> heap;

    public PriorityMergeReader(PriorityTimeValuePairReader... readers) throws IOException {
        readerList = new ArrayList<>();

        for (int i = 0; i < readers.length; i++) {
            readerList.add(readers[i]);
        }
        init();
    }

    public PriorityMergeReader(List<PriorityTimeValuePairReader> readerList) throws IOException {
        this.readerList = readerList;
        init();
    }

    private void init() throws IOException {
        heap = new PriorityQueue<>();
        for (int i = 0; i < readerList.size(); i++) {
            if (readerList.get(i).hasNext()) {
                heap.add(new Element(i, readerList.get(i).next(), readerList.get(i).getPriority()));
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
            PriorityTimeValuePairReader priorityTimeValuePairReader = readerList.get(e.index);
            if (priorityTimeValuePairReader.hasNext()) {
                heap.add(new Element(e.index, priorityTimeValuePairReader.next(), priorityTimeValuePairReader.getPriority()));
            }
        }
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        if (hasNext()) {
            next();
        }
    }

    @Override
    public void close() throws IOException {
        for (PriorityTimeValuePairReader timeValuePairReader : readerList) {
            timeValuePairReader.close();
        }
    }

    protected class Element implements Comparable<Element> {
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

    @Override
    public boolean hasNextBatch() {
        return false;
    }

    @Override
    public BatchData nextBatch() {
        return null;
    }

    @Override
    public BatchData currentBatch() {
        return null;
    }
}
