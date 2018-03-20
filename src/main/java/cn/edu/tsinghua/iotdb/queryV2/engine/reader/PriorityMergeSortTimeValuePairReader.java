package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessor;
import cn.edu.tsinghua.iotdb.monitor.IFixStatistics;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader.Priority;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by zhangjinrui on 2018/1/11.
 */
public class PriorityMergeSortTimeValuePairReader implements TimeValuePairReader, SeriesReader, IFixStatistics {

    private List<PriorityTimeValuePairReader> readerList;
    private PriorityQueue<Element> heap;
    private long point_cover_num;

    private List<FileNodeProcessor> fileNodeProcessorList;

    public PriorityMergeSortTimeValuePairReader(PriorityTimeValuePairReader... readers) throws IOException {
        readerList = new ArrayList<>();
        for (int i = 0; i < readers.length; i++) {
            readerList.add(readers[i]);
        }
        init();
    }

    public PriorityMergeSortTimeValuePairReader(List<PriorityTimeValuePairReader> readerList) throws IOException {
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

        fileNodeProcessorList = new ArrayList<>();
        point_cover_num = 0;
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
        boolean flag = false;
        while (heap.size() > 0 && heap.peek().timeValuePair.getTimestamp() == top.timeValuePair.getTimestamp()) {
            Element e = heap.poll();
            PriorityTimeValuePairReader priorityTimeValuePairReader = readerList.get(e.index);
            if (priorityTimeValuePairReader.hasNext()) {
                heap.add(new Element(e.index, priorityTimeValuePairReader.next(), priorityTimeValuePairReader.getPriority()));
            }

            if(flag){
//                fixStatistics();
                point_cover_num++;
            }
            else flag = true;
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
        for (TimeValuePairReader timeValuePairReader : readerList) {
            timeValuePairReader.close();
        }

        point_cover_num = 0;
    }

    @Override
    public void registStatStorageGroup(FileNodeProcessor fileNodeProcessor) {
        fileNodeProcessorList.add(fileNodeProcessor);
    }

    @Override
    public void fixStatistics() {
        for(FileNodeProcessor fileNodeProcessor : fileNodeProcessorList){
            fileNodeProcessor.fixStatistics();
        }

        FileNodeManager.getInstance().fixStatistics();
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

    public long getPointCoverNum(){
        return point_cover_num;
    }
}
